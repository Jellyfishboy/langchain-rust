use std::{collections::HashMap, error::Error, sync::Arc};

use async_trait::async_trait;
use pgvector::Vector;
use serde_json::{json, Value};
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

use crate::{
    embedding::embedder_trait::Embedder,
    schemas::Document,
    vectorstore::{VecStoreOptions, VectorStore},
};

pub struct Store {
    pub(crate) embedder: Arc<dyn Embedder>,
    pub(crate) pool: Pool<Postgres>,
    pub(crate) collection_name: String,
    pub(crate) collection_table_name: String,
    pub(crate) collection_uuid: String,
    pub(crate) collection_metadata: HashMap<String, Value>,
    pub(crate) embedder_table_name: String,
    pub(crate) pre_delete_collection: bool,
    pub(crate) vector_dimensions: i32,
    pub(crate) hns_index: Option<HNSWIndex>,
    pub(crate) vstore_options: VecStoreOptions,
}

pub struct HNSWIndex {
    pub(crate) m: i32,
    pub(crate) ef_construction: i32,
    pub(crate) distance_function: String,
}

impl HNSWIndex {
    pub fn new(m: i32, ef_construction: i32, distance_function: &str) -> Self {
        HNSWIndex {
            m,
            ef_construction,
            distance_function: distance_function.into(),
        }
    }
}

impl Store {
    // getFilters return metadata filters, now only support map[key]value pattern
    // TODO: should support more types like {"key1": {"key2":"values2"}} or {"key": ["value1", "values2"]}.
    fn get_filters(&self, opt: &VecStoreOptions) -> Result<HashMap<String, Value>, Box<dyn Error>> {
        match &opt.filters {
            Some(Value::Object(map)) => {
                // Convert serde_json Map to HashMap<String, Value>
                let filters = map.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                Ok(filters)
            }
            None => Ok(HashMap::new()), // No filters provided
            _ => Err("Invalid filters format".into()), // Filters provided but not in the expected format
        }
    }

    fn get_name_space(&self, opt: &VecStoreOptions) -> String {
        match &opt.name_space {
            Some(name_space) => name_space.clone(),
            None => self.collection_name.clone(),
        }
    }

    fn get_score_threshold(&self, opt: &VecStoreOptions) -> Result<f32, Box<dyn Error>> {
        match &opt.score_threshold {
            Some(score_threshold) => {
                if *score_threshold < 0.0 || *score_threshold > 1.0 {
                    return Err("Invalid score threshold".into());
                }
                Ok(*score_threshold)
            }
            None => Ok(0.0),
        }
    }

    async fn drop_tables(&self) -> Result<(), Box<dyn Error>> {
        sqlx::query(&format!(
            r#"DROP TABLE IF EXISTS {}"#,
            self.embedder_table_name
        ))
        .execute(&self.pool)
        .await?;

        sqlx::query(&format!(
            r#"DROP TABLE IF EXISTS {}"#,
            self.collection_table_name
        ))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn remove_collection(&self) -> Result<(), Box<dyn Error>> {
        sqlx::query(r#"DELETE FROM collection WHERE uuid = $1"#)
            .bind(&self.collection_uuid)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
#[async_trait]
impl VectorStore for Store {
    async fn add_documents(
        &self,
        docs: &[Document],
        opt: &VecStoreOptions,
    ) -> Result<Vec<String>, Box<dyn Error>> {
        if opt.score_threshold.is_some() || opt.filters.is_some() || opt.name_space.is_some() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "score_threshold, filters, and name_space are not supported in pgvector",
            )));
        }
        let texts: Vec<String> = docs.iter().map(|d| d.page_content.clone()).collect();

        let embedder = opt.embedder.as_ref().unwrap_or(&self.embedder);

        let vectors = embedder.embed_documents(&texts).await?;

        if vectors.len() != docs.len() {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Number of vectors and documents do not match",
            )));
        }

        let mut tx = self.pool.begin().await?;

        let mut ids = Vec::with_capacity(docs.len());

        for (doc, vector) in docs.iter().zip(vectors.iter()) {
            let id = Uuid::new_v4().to_string();
            ids.push(id.clone());

            let vector_value =
                Vector::from(vector.into_iter().map(|x| *x as f32).collect::<Vec<f32>>());

            sqlx::query(&format!(
                r#"INSERT INTO {} 
(uuid, document, embedding, cmetadata, collection_id) VALUES ($1, $2, $3, $4, $5)"#,
                self.embedder_table_name
            ))
            .bind(&id)
            .bind(&doc.page_content)
            .bind(&vector_value)
            .bind(json!(&doc.metadata))
            .bind(&self.collection_uuid)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(ids)
    }

    async fn similarity_search(
        &self,
        query: &str,
        limit: usize,
        opt: &VecStoreOptions,
    ) -> Result<Vec<Document>, Box<dyn Error>> {
        let namespace = opt.name_space.as_deref().unwrap_or("default");
        
        let sql = format!(
            r#"SELECT 
                content,
                namespace,
                (vectors <=> $1) as distance
            FROM 
                vector_docs
            WHERE 
                namespace = $2
            ORDER BY 
                distance ASC
            LIMIT $3"#
        );
    
        let query_vector = self.embedder.embed_query(query).await?;
    
        let rows = sqlx::query(&sql)
            .bind(&Vector::from(
                query_vector
                    .into_iter()
                    .map(|x| x as f32)
                    .collect::<Vec<f32>>(),
            ))
            .bind(namespace)
            .bind(limit as i32)
            .fetch_all(&self.pool)
            .await?;
    
        let docs = rows
            .into_iter()
            .map(|row| {
                let page_content: String = row.try_get(0)?;
                let namespace: String = row.try_get(1)?;
                let distance: f64 = row.try_get(2)?;
    
                let mut metadata = HashMap::new();
                metadata.insert("namespace".to_string(), Value::String(namespace));
    
                Ok(Document {
                    page_content,
                    metadata,
                    score: distance,  // Lower distance means more similar
                })
            })
            .collect::<Result<Vec<Document>, sqlx::Error>>()?;
    
        Ok(docs)
    }
}

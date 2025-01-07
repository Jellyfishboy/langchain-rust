use std::error::Error;
use async_trait::async_trait;
use serde_json::{json, Value};
use crate::tools::Tool;

pub struct DataForSeo {
    access_token: String,
    location: Option<String>,
    language_code: Option<String>,
    depth: Option<u32>,
}

impl DataForSeo {
    pub fn new(access_token: String) -> Self {
        Self {
            access_token,
            location: Some("United States".to_string()),
            language_code: Some("en".to_string()),
            depth: Some(100),
        }
    }

    pub fn with_location<S: Into<String>>(mut self, location: S) -> Self {
        self.location = Some(location.into());
        self
    }

    pub fn with_language_code<S: Into<String>>(mut self, language_code: S) -> Self {
        self.language_code = Some(language_code.into());
        self
    }

    pub fn with_depth(mut self, depth: u32) -> Self {
        self.depth = Some(depth);
        self
    }

    pub async fn simple_search(&self, query: &str) -> Result<String, Box<dyn Error>> {
        let client = reqwest::Client::new();
        
        let body = json!([{
            "language_code": self.language_code.as_deref().unwrap_or("en"),
            "location_name": self.location.as_deref().unwrap_or("United States"),
            "group_organic_results": true,
            "se_domain": "google.com",
            "keyword": query,
            "depth": self.depth.unwrap_or(100)
        }]);

        let response = client
            .post("https://api.dataforseo.com/v3/serp/google/organic/live/regular")
            .basic_auth(&self.access_token, Some(""))
            .json(&body)
            .send()
            .await?;

        let results: Value = response.json().await?;
        
        process_dataforseo_response(&results)
    }
}

fn process_dataforseo_response(res: &Value) -> Result<String, Box<dyn Error>> {
    if let Some(tasks) = res["tasks"].as_array() {
        if let Some(first_task) = tasks.first() {
            if let Some(results) = first_task["result"].as_array() {
                if let Some(first_result) = results.first() {
                    if let Some(items) = first_result["items"].as_array() {
                        if let Some(first_item) = items.first() {
                            if let Some(description) = first_item["description"].as_str() {
                                return Ok(description.to_string());
                            }
                        }
                    }
                }
            }
        }
    }
    
    Err("No results found".into())
}

#[async_trait]
impl Tool for DataForSeo {
    fn name(&self) -> String {
        String::from("GoogleSearch")
    }

    fn description(&self) -> String {
        String::from(
            r#"A wrapper around Google Search. 
            Useful for when you need to answer questions about current events. 
            Always one of the first options when you need to find information on internet.
            Input should be a search query."#,
        )
    }

    async fn run(&self, input: Value) -> Result<String, Box<dyn Error>> {
        let input = input.as_str().ok_or("Input should be a string")?;
        self.simple_search(input).await
    }
}

impl Default for DataForSeo {
    fn default() -> Self {
        Self {
            access_token: std::env::var("DATAFORSEO_ACCESS_TOKEN").unwrap_or_default(),
            location: Some("United States".to_string()),
            language_code: Some("en".to_string()),
            depth: Some(100),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DataForSeo;

    #[tokio::test]
    #[ignore]
    async fn dataforseo_tool() {
        let dataforseo = DataForSeo::default();
        let s = dataforseo
            .simple_search("Who is the President of Peru")
            .await
            .unwrap();
        println!("{}", s);
    }
}
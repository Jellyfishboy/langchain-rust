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
    
        println!("🔍 Request body: {}", serde_json::to_string_pretty(&body)?);
    
        let response = client
            .post("https://api.dataforseo.com/v3/serp/google/organic/live/regular")
            .header("Authorization", format!("Basic {}", self.access_token))
            .json(&body)
            .send()
            .await?;
    
        println!("📡 Response status: {}", response.status());
        
        let results: Value = response.json().await?;
        println!("📊 Raw API response: {}", serde_json::to_string_pretty(&results)?);
        
        process_dataforseo_response(&results)
    }
}

fn process_dataforseo_response(res: &Value) -> Result<String, Box<dyn Error>> {
    println!("Processing response...");
    
    // Check for API status
    if let Some(status_code) = res["status_code"].as_u64() {
        println!("API status code: {}", status_code);
        if status_code != 20000 {
            return Err(format!("API error: {}", res["status_message"].as_str().unwrap_or("Unknown error")).into());
        }
    }

    if let Some(tasks) = res["tasks"].as_array() {
        println!("Found {} tasks", tasks.len());
        
        if let Some(first_task) = tasks.first() {
            println!("Task status: {}", first_task["status_code"].as_u64().unwrap_or(0));
            
            if let Some(results) = first_task["result"].as_array() {
                println!("Found {} results", results.len());
                
                if let Some(first_result) = results.first() {
                    if let Some(items) = first_result["items"].as_array() {
                        println!("Found {} items", items.len());
                        
                        // Collect all organic results
                        let mut organic_results = Vec::new();
                        for item in items {
                            // Only require title field, make snippet optional
                            if let Some(title) = item["title"].as_str() {
                                let snippet = item["snippet"].as_str().unwrap_or("");
                                organic_results.push(format!("Title: {}\nSnippet: {}\n", title, snippet));

                                if organic_results.len() >= 30 {
                                    break;
                                }
                            }
                        }
                        
                        if !organic_results.is_empty() {
                            return Ok(organic_results.join("\n"));
                        }
                    }
                }
            }
        }
    }
    
    Err("No valid results found in the response structure".into())
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
        let dataforseo = DataForSeo::new("your_base64_encoded_api_key".to_string());
        let s = dataforseo
            .simple_search("Who is the President of Peru")
            .await
            .unwrap();
        println!("{}", s);
    }
}
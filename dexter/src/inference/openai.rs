use serde::{Deserialize, Serialize};
use serde_json::json;

use super::{Key, Error, get_key};



#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Usage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

type Embedding = Vec<f64>;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EmbeddingResponseData {
    pub object: String,
    pub embedding: Embedding,
    pub index: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingResponseData>,
    pub model: String,
    pub usage: Usage,
}

pub async fn run_openai_raw_inference(key: &Key, model: &str, url: &str, payload: String) -> Result<String, Error>
{
    let api_token = get_key(key).await
        .map_err(Error::KeyLoadError)?;

    let client = reqwest::Client::new();
    let res = client.post(url)
        .bearer_auth(api_token)
        .header("Content-Type", "application/json")
        .body(payload)
        .send()
        .await
        .map_err(Error::RemoteCallError)?;

    match res.status() {
        reqwest::StatusCode::OK => {
            let text = res.text().await
                .map_err(Error::RemoteCallError)?;

            Ok(text)
        },
        _ => {
            Err(Error::RemoteCallFailure(res.status()))
        }
    }
}

pub async fn run_openai_embedding(
    key: &Key, 
    model: &str, 
    inputs: Vec<&str>) -> Result<EmbeddingResponse, Error>
{
    let payload = json!({
        "model": model,
        "input": inputs
    });

    let serialized_payload = serde_json::to_string(&payload)
        .map_err(Error::SerializeError)?;

    let url = "https://api.openai.com/v1/embeddings";

    let res = run_openai_raw_inference(key, model, url, serialized_payload).await?;

    let output: EmbeddingResponse = serde_json::from_str(&res)
        .map_err(Error::DeserializeError)?;

    Ok(output)
}

#[tokio::test]
async fn test_openai_embedding() {
    let key = Key::File("/home/sublogical/.api/openai".to_string());
    let model = "text-embedding-ada-002";
    let input = vec!["Get a vector representation of a given input that can be easily consumed by machine learning models and algorithms"];

    let output = run_openai_embedding(&key, model, input).await.unwrap();
    assert!(output.data.len() > 0);
    assert!(output.data[0].embedding.len() > 0);
    assert!(output.usage.total_tokens == 19);
    assert!(output.usage.prompt_tokens == 19);
}


#[test]
fn test_openai_embedding_deserialization() {
    let path = "resources/test/inference/openai_embedding_response.json";
    let raw = std::fs::read(path).unwrap();
    let text = String::from_utf8(raw).unwrap();
    let output: EmbeddingResponse = serde_json::from_str(&text).unwrap();

    assert!(output.data.len() > 0);
    assert!(output.data[0].embedding.len() > 0);
    assert!(output.usage.total_tokens == 19);
    assert!(output.usage.prompt_tokens == 19);
}
use async_trait::async_trait;
use reqwest::header::AUTHORIZATION;
use serde_json::{json, Value};
use tokio::{fs::File, io::AsyncReadExt};


#[derive(Debug)]
enum Error {
    KeyLoadError(std::io::Error),
    RemoteCallError(reqwest::Error),
    RemoteCallFailure(reqwest::StatusCode),
}

#[derive(Debug)]
enum Key {
    Local(String),
    File(String)
}

async fn get_key(key: Key) -> Result<String, std::io::Error> {
    match key {
        Key::Local(key) => Ok(key),
        Key::File(path) => {
            let mut file = File::open(path).await?;
            let mut contents = String::new();
            file.read_to_string(&mut contents).await?;
            let contents = contents.trim().to_string();
            Ok(contents)
        }
    }
}

async fn run_text_inference_huggingface(key: Key, model: String, payload: String) -> Result<String, Error> {    
    let api_token = get_key(key).await
        .map_err(Error::KeyLoadError)?;

    println!("API Token: [{}]", api_token);
    let client = reqwest::Client::new();
    let url = format!("https://api-inference.huggingface.co/models/{}", model);

    let res = client.post(url)
        .bearer_auth(api_token)
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

#[tokio::test]
async fn test_get_key() {
    let key = get_key(Key::Local("test".to_string())).await.unwrap();
    assert_eq!(key, "test");
}

#[tokio::test]
async fn test_huggingface() {
    let key = Key::File("/home/sublogical/.api/huggingface".to_string());
    let model = "distilbert-base-uncased".to_string();
    let obj = json!({ "inputs": "The answer to the universe is [MASK]." });        
    let payload = serde_json::to_string(&obj).unwrap();

    let res = run_text_inference_huggingface(key, model, payload).await.unwrap();
    let output: Value = serde_json::from_str(&res).unwrap();

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
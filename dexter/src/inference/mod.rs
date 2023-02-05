use tokio::{fs::File, io::AsyncReadExt};

pub mod huggingface;
pub mod openai;


#[derive(Debug)]
pub enum Error {
    KeyLoadError(std::io::Error),
    RemoteCallError(reqwest::Error),
    RemoteCallFailure(reqwest::StatusCode),
    SerializeError(serde_json::Error),
    DeserializeError(serde_json::Error),
}

#[derive(Debug)]
pub enum Key {
    Local(String),
    File(String)
}

async fn get_key(key: &Key) -> Result<String, std::io::Error> {
    match key {
        Key::Local(key) => Ok(key.clone()),
        Key::File(path) => {
            let mut file = File::open(path).await?;
            let mut contents = String::new();
            file.read_to_string(&mut contents).await?;
            let contents = contents.trim().to_string();
            Ok(contents)
        }
    }
}


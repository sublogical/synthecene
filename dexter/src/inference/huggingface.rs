use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{fs::File, io::AsyncReadExt};

use super::{Key, Error, get_key};

async fn run_raw_inference_huggingface(key: &Key, model: &str, payload: String) -> Result<String, Error> {    
    let api_token = get_key(key).await
        .map_err(Error::KeyLoadError)?;

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

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct InferenceOptions {
    pub use_cache: bool,
    pub wait_for_model: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SummarizationTaskParameters {
    // Optional minimum length of the summary
    pub min_length: Option<usize>,

    // Optional maximum length of the summary
    pub max_length: Option<usize>,

    // optional top tokens considered in the sample operation for each step
    pub top_k: Option<usize>,

    // optional top sum of probabilities for tokens considered in the sample operation
    pub top_p: Option<f64>,

    // optional temperature for the sample operation
    pub temperature: Option<f64>,

    // optional repetition penalty to the sample operation
    pub repetition_penalty: Option<f64>,

    // optional max time in seconds to wait for the model to generate a summary
    pub max_time: Option<f64>,
}


#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct SummarizationResponse {
    pub summary_text: String,
}

/**
 * Run a summarization task on a Hugging Face model
 * 
 * Recommended Models:
 * - facebook/bart-large-cnn
 * - google/bigbird-peganet-large-
 */
pub async fn run_huggingface_summarization(
    key: &Key, 
    model: &str, 
    inputs: &str, 
    parameters: Option<SummarizationTaskParameters>, 
    options: Option<InferenceOptions>) -> Result<Vec<SummarizationResponse>, Error> 
{
    #[derive(Debug, Clone, Serialize, Deserialize, Default)]
    struct SummarizationPayload {
        inputs: String,

        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        parameters: Option<SummarizationTaskParameters>,

        #[serde(skip_serializing_if = "Option::is_none")]
        #[serde(default)]
        options: Option<InferenceOptions>,
    }

    let payload = SummarizationPayload { inputs: inputs.to_string(), parameters, options };

    let serialized_payload = serde_json::to_string(&payload)
        .map_err(Error::SerializeError)?;

    let res = run_raw_inference_huggingface(key, model, serialized_payload).await?;

    let output: Vec<SummarizationResponse> = serde_json::from_str(&res)
        .map_err(Error::DeserializeError)?;

    Ok(output)
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct QuestionAnsweringTask {
    pub question: String,
    pub context: String
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct QuestionAnsweringResponse {
    pub answer: String,
    pub score: f64,
    pub start: usize,
    pub end: usize,
}

/**
 * Run a question answering task on a huggingface model
 * 
 * Recommended Models: 
 * - distilbert-base-uncased-distilled-squad
 * - deepset/bert-base-cased-squad2
 * - deepset/bert-base-uncased-squad2
 * - deepset/bert-large-uncased-whole-word-masking-squad2
 * - deepset/roberta-base-squad2
 * - deepset/xlm-roberta-base-squad2
 * - deepset/xlm-roberta-large-squad2
 * 
 * See https://huggingface.co/models?pipeline_tag=question-answering for more models
 * 
 * @param key The key to use for authentication
 * @param model The model to use
 * @param task The task to run
 * 
 * @returns The response from the model
 */
pub async fn run_huggingface_qa(key: &Key, model: &str, task: &QuestionAnsweringTask) -> Result<QuestionAnsweringResponse, Error> {    
    let payload = serde_json::to_string(task)
        .map_err(Error::SerializeError)?;

    let res = run_raw_inference_huggingface(key, model, payload).await?;

    let output: QuestionAnsweringResponse = serde_json::from_str(&res)
        .map_err(Error::DeserializeError)?;

    Ok(output)
}



#[tokio::test]
async fn test_get_key() {
    let key = get_key(&Key::Local("test".to_string())).await.unwrap();
    assert_eq!(key, "test");
}

#[tokio::test]
async fn test_raw_huggingface() {
    let key = Key::File("/home/sublogical/.api/huggingface".to_string());
    let model = "distilbert-base-uncased";
    let obj = json!({ "inputs": "The answer to the universe is [MASK]." });        
    let payload = serde_json::to_string(&obj).unwrap();

    let res = run_raw_inference_huggingface(&key, model, payload).await.unwrap();
    let output: Value = serde_json::from_str(&res).unwrap();

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

#[tokio::test]
async fn test_huggingface_summarization() {
    let key = Key::File("/home/sublogical/.api/huggingface".to_string());
    let model = "facebook/bart-large-cnn";
    let text = "The tower is 324 metres (1,063 ft) tall, about the same height as an 81-storey building, and the tallest structure in Paris. Its base is square, measuring 125 metres (410 ft) on each side. During its construction, the Eiffel Tower surpassed the Washington Monument to become the tallest man-made structure in the world, a title it held for 41 years until the Chrysler Building in New York City was finished in 1930. It was the first structure to reach a height of 300 metres. Due to the addition of a broadcasting aerial at the top of the tower in 1957, it is now taller than the Chrysler Building by 5.2 metres (17 ft). Excluding transmitters, the Eiffel Tower is the second tallest free-standing structure in France after the Millau Viaduct.".to_string();

    let output = run_huggingface_summarization(&key, model, &text, None, None).await.unwrap();
    assert!(output.len() > 0);
    assert!(output[0].summary_text.len() > 0);
    assert!(output[0].summary_text.len() < text.len());
}

#[tokio::test]
async fn test_huggingface_qa() {
    let key = Key::File("/home/sublogical/.api/huggingface".to_string());
    let model = "deepset/roberta-base-squad2";
    let task = QuestionAnsweringTask {
        question: "What's my name?".to_string(),
        context: "My name is Clara and I live in Berkeley.".to_string()
    };

    let output = run_huggingface_qa(&key, model, &task).await.unwrap();
    assert_eq!(output.answer, "Clara");
    assert_eq!(output.start, 11);
    assert_eq!(output.end, 16);

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}
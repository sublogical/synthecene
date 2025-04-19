use std::{fmt::Debug, collections::{HashMap, hash_map::Entry}, sync::Arc};
use serde::{Deserialize, Serialize };
use tokio::sync::{mpsc, RwLock};
use warp::{Filter};

use crate::inference::{huggingface::run_huggingface_summarization, Key};

#[derive(Debug)]
enum Error {
    InferenceError(crate::inference::Error),
    UnsatisfactoryResponse,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct GenerativeTextResponse {
    pub(crate) text: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct GoogleDocsTaskRequest {
    pub(crate) document_id: String,
    pub(crate) document_name: String,
    pub(crate) document_lang: String,
    pub(crate) selected_text: Vec<String>,
}

impl warp::reject::Reject for Error {}

/**
 * POST   /task/text/google-narrative - create a narrative summary
 */
async fn google_summarize(request: GoogleDocsTaskRequest) -> Result<impl warp::Reply, warp::Rejection>  {
    println!("IN SUMMARIZE");
    let key = Key::File("/home/sublogical/.api/huggingface".to_string());
    let model = "facebook/bart-large-cnn";
    let selected_text = request.selected_text.join("\n");

    let summary = run_huggingface_summarization(&key, model, &selected_text, None, None).await
        .map_err(|e| warp::reject::custom(Error::InferenceError(e)))?;

    if summary.len() == 0 {
        return Err(warp::reject::custom(Error::UnsatisfactoryResponse));
    }
    let result = GenerativeTextResponse { text: summary[0].summary_text.clone() };

    Ok(warp::reply::json(&result))
}

fn create_narrative_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("task" / "text" / "google-summarize")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(google_summarize)
}


pub(crate) fn routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    create_narrative_route()
}


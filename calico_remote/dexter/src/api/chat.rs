use std::{fmt::Debug, collections::{HashMap, hash_map::Entry}, sync::Arc};
use serde::{Deserialize, Serialize };
use tokio::sync::{mpsc, RwLock};
use warp::{Filter};


/**
 * SEQUENCE DIAGRAM:
 * 
 * Create a new chat
 *  
 */




struct ChatData {
    id: String,
    name: String,
    messages: Vec<ChatMessage>,
}

struct ChatClient {
    chat_id: String,
    sender: mpsc::UnboundedSender<Result<ChatMessage, warp::Error>>
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct CreateChatRequest {
    pub(crate) name: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub(crate) struct Chat {
    pub(crate) id: String,
    pub(crate) name: String,
}

/**
 * POST   /chat - create a chat
 */
async fn create_chat(chat_request: CreateChatRequest) -> Result<impl warp::Reply, warp::Rejection> {
    let result = Chat { id: "1".to_string(), name: chat_request.name };

    Ok(warp::reply::json(&result))
}

fn create_chat_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat")
        .and(warp::post())
        .and(warp::body::json())
        .and_then(create_chat)
}

async fn list_chats() -> Result<impl warp::Reply, warp::Rejection>  {
    let result = vec![
        Chat { id: "1".to_string(), name: "Chat 1".to_string() },
        Chat { id: "2".to_string(), name: "Chat 2".to_string() },
    ];

    Ok(warp::reply::json(&result)) 
}

fn list_chats_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat")
        .and(warp::get())
        .and_then(list_chats)
}

async fn get_chat(session_id: String) -> Result<impl warp::Reply, warp::Rejection> {
    let result = Chat { id: "1".to_string(), name: "Chat 1".to_string() };

    Ok(warp::reply::json(&result))
}

fn get_chat_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String)
        .and(warp::get())
        .and_then(get_chat)
}

async fn delete_chat(session_id: String) -> Result<impl warp::Reply, warp::Rejection> {
    let result = Chat { id: "1".to_string(), name: "Chat 1".to_string() };

    Ok(warp::reply::json(&result))
}

fn delete_chat_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String)
        .and(warp::delete())
        .and_then(delete_chat)
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChatMessage {
    pub chat_id: String,
    pub message_id: String,
    pub agent: bool,
    pub message: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct ChatMessageRequest {
    pub agent: bool,
    pub message: String,
}
/**
 * POST   /chat/{session}/message - create a message
 */
async fn create_message(session_id: String, 
                        request: ChatMessageRequest,
                        chat_store: ChatStore) -> Result<impl warp::Reply, warp::Rejection> {

    let new_message = ChatMessage { 
        chat_id: session_id.clone(), 
        message_id: "1".to_string(),
        agent: request.agent, 
        message: request.message
    };

    {
        let mut chat_store = chat_store.write().await;

        let mut chat = match chat_store.entry(session_id.clone()) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(vec![]),
        };

        chat.push(new_message.clone());
    }

    Ok(warp::reply::json(&new_message))
}

fn create_message_route(chat_store: ChatStore) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String / "message")
        .and(warp::post())
        .and(warp::body::json())
        .and(warp::any().map(move || chat_store.clone()))
        .and_then(create_message)
}

/**
 * GET    /chat/{session}/message - list messages
 */
async fn list_messages(session_id: String, chat_store: ChatStore) -> Result<impl warp::Reply, warp::Rejection> {
    let result = chat_store.read().await.get(&session_id).unwrap_or(&vec![]).clone();
    Ok(warp::reply::json(&result))
}

fn list_messages_route(chat_store: ChatStore) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String / "message")
        .and(warp::get())
        .and(warp::any().map(move || chat_store.clone()))
        .and_then(list_messages)
}

/**
 * GET    /chat/{session}/message/{message} - get a message
 */
async fn get_message(session_id: String, message_id: String) -> Result<impl warp::Reply, warp::Rejection> {
    let result = ChatMessage { chat_id: "1".to_string(), message_id: "1".to_string(), agent: true, message: "Hello, world!".to_string() };
    Ok(warp::reply::json(&result))
}

fn get_message_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String / "message" / String)
        .and(warp::get())
        .and_then(get_message)
}

/**
 * DELETE /chat/{session}/message/{message} - delete a message
 */
async fn delete_message(session_id: String, message_id: String) -> Result<impl warp::Reply, warp::Rejection> {
    let result = ChatMessage { chat_id: "1".to_string(), message_id: "1".to_string(), agent: true, message: "Hello, world!".to_string() };
    Ok(warp::reply::json(&result))
}

fn delete_message_route() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("chat" / String / "message" / String)
        .and(warp::delete())
        .and_then(delete_message)
}

type ChatStore = Arc<RwLock<HashMap<String, Vec<ChatMessage>>>>;

/**
 * POST   /chat - create new session
 * GET    /chat - list sessions   
 * GET    /chat/{session} - get a session
 * DELETE /chat/{session} - end a session
 * POST   /chat/{session}/message - create a message
 * GET    /chat/{session}/message - get messages for a session
 * GET    /chat/{session}/message/{message} - get a message
 * DELETE /chat/{session}/message/{message} - delete a message
 */
pub(crate) fn routes() -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    let chats: ChatStore = Arc::new(RwLock::new(HashMap::new()));

    list_chats_route().or(create_chat_route()).or(get_chat_route()).or(delete_chat_route())
        .or(create_message_route(chats.clone()))
        .or(list_messages_route(chats.clone()))
        .or(get_message_route())
        .or(delete_message_route())
}

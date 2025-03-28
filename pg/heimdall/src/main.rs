use axum::{
    routing::{get, post, put, delete},
    Router,
    response::sse::{Event, Sse},
    response::IntoResponse,
    body::Body,
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use futures::stream::{self, Stream};
use std::convert::Infallible;
use std::time::Duration;
use tokio_stream::StreamExt;
use tokio::time::interval;
use serde::{Deserialize, Serialize};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use glimmer::GlimmerConnection;
use tokio::sync::Mutex;

/// Command line arguments
#[derive(Parser)]
#[command(version, about)]
struct Args {
    /// HTTP address to bind to (e.g., "127.0.0.1:50053" or "0.0.0.0:8080")
    #[arg(short, long, default_value = "127.0.0.1:50053")]
    address: String,

    /// gRPC server address (e.g., "[::1]:50052" or "127.0.0.1:50052")
    #[arg(short = 'g', long, default_value = "[::1]:50052")]
    grpc_address: String,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: &'static str,
    timestamp: u64,
}

// Data structures
#[derive(Debug, Serialize, Deserialize)]
struct Agent {
    id: String,
    status: AgentStatus,
    properties: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
enum AgentStatus {
    Running,
    Paused,
    Stopped,
}

#[derive(Debug, Deserialize)]
struct CreateAgentRequest {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ChannelConfig {
    // Add necessary fields for channel configuration
}

// Wrap GlimmerConnection in Arc<Mutex> for shared state
type SharedState = Arc<Mutex<GlimmerConnection>>;

// Wrapper types for serialization
#[derive(Serialize)]
struct AgentResponseWrapper {
    success: bool,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    agent: Option<AgentWrapper>,
}

#[derive(Serialize)]
struct AgentWrapper {
    id: String,
    status: String,
    properties: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct PropertyResponseWrapper {
    success: bool,
    message: String,
    properties: std::collections::HashMap<String, String>,
}

#[derive(Serialize)]
struct ChannelResponseWrapper {
    success: bool,
    message: String,
    channel_id: String,
}

#[derive(Serialize)]
struct ChannelMessageWrapper {
    agent_id: String,
    channel_id: String,
    content: String,
    timestamp: i64,
}

impl From<glimmer::glimmer_api::ChannelMessage> for ChannelMessageWrapper {
    fn from(msg: glimmer::glimmer_api::ChannelMessage) -> Self {
        Self {
            agent_id: msg.agent_id,
            channel_id: msg.channel_id,
            content: msg.content,
            timestamp: msg.timestamp,
        }
    }
}

// Convert from gRPC types to our wrapper types
impl From<glimmer::glimmer_api::AgentResponse> for AgentResponseWrapper {
    fn from(response: glimmer::glimmer_api::AgentResponse) -> Self {
        Self {
            success: response.success,
            message: response.message,
            agent: response.agent.map(|a| AgentWrapper {
                id: a.id,
                status: format!("{:?}", glimmer::AgentStatus::try_from(a.status).unwrap_or_default()),
                properties: a.properties,
            }),
        }
    }
}

impl From<glimmer::glimmer_api::PropertyResponse> for PropertyResponseWrapper {
    fn from(response: glimmer::glimmer_api::PropertyResponse) -> Self {
        Self {
            success: response.success,
            message: response.message,
            properties: response.properties,
        }
    }
}

impl From<glimmer::glimmer_api::ChannelResponse> for ChannelResponseWrapper {
    fn from(response: glimmer::glimmer_api::ChannelResponse) -> Self {
        Self {
            success: response.success,
            message: response.message,
            channel_id: response.channel_id,
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    println!("Connecting to GlimmerService on {:?}", args.grpc_address);

    // Initialize Glimmer client
    let glimmer = GlimmerConnection::connect(&args.grpc_address)
        .await
        .expect("Failed to connect to Glimmer service");
    let shared_state = Arc::new(Mutex::new(glimmer));

    // Initialize router with shared state
    let app = Router::new()
        .route("/health", get(health_check))
        // Agent CRUD operations
        .route("/agent", post(create_agent))
        .route("/agent/:id", get(get_agent))
        .route("/agent/:id", delete(delete_agent))
        // Agent properties
        .route("/agent/:id/property", get(get_properties))
        .route("/agent/:id/property/:key", get(get_property))
        .route("/agent/:id/property/:key", put(set_property))
        .route("/agent/:id/property/:key", delete(delete_property))
        // Agent lifecycle
        .route("/agent/:id/start", post(start_agent))
        .route("/agent/:id/pause", post(pause_agent))
        .route("/agent/:id/stop", post(stop_agent))
        // Agent channels
        .route("/agent/:id/channel", post(create_channel))
        .route("/agent/:id/channel/:channel", get(get_channel))
        .route("/agent/:id/channel/:channel/stream", get(stream_channel))
        .route("/agent/:id/channel/:channel", delete(delete_channel))
        .with_state(shared_state);

    // Run server
    let listener = tokio::net::TcpListener::bind(&args.address).await.unwrap();
    println!("Server running on http://{}", args.address);
    axum::serve(listener, app).await.unwrap();
}

// Basic health check endpoint
async fn health_check() -> Json<HealthResponse> {
    let response = HealthResponse {
        status: "OK".to_string(),
        version: env!("CARGO_PKG_VERSION"),
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };
    
    Json(response)
}

// Handler functions
async fn create_agent(
    State(state): State<SharedState>,
    Json(payload): Json<CreateAgentRequest>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    match client.create_agent(payload.id).await {
        Ok(response) => {
            Json(AgentResponseWrapper::from(response)).into_response()
        },
        Err(status) => {
            (StatusCode::INTERNAL_SERVER_ERROR, status.message().to_string()).into_response()
        }
    }
}

async fn get_agent(
    State(state): State<SharedState>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    match client.get_agent(id).await {
        Ok(response) => {
            Json(AgentResponseWrapper::from(response)).into_response()
        },
        Err(status) => {
            (StatusCode::INTERNAL_SERVER_ERROR, status.message().to_string()).into_response()
        }
    }
}

async fn delete_agent(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
) -> Response {
    // Delete agent logic
    todo!()
}

async fn get_properties(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
) -> Response {
    // Get all properties
    todo!()
}

async fn get_property(
    State(_state): State<SharedState>,
    Path((_id, _key)): Path<(String, String)>,
) -> Response {
    // Get specific property
    todo!()
}

async fn set_property(
    State(state): State<SharedState>,
    Path((id, key)): Path<(String, String)>,
    Json(value): Json<String>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    match client.set_property(id, key, value).await {
        Ok(response) => {
            Json(PropertyResponseWrapper::from(response)).into_response()
        },
        Err(status) => {
            (StatusCode::INTERNAL_SERVER_ERROR, status.message().to_string()).into_response()
        }
    }
}

async fn delete_property(
    State(_state): State<SharedState>,
    Path((_id, _key)): Path<(String, String)>,
) -> Response {
    // Delete property
    todo!()
}

async fn start_agent(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
) -> Response {
    // Start agent logic
    todo!()
}

async fn pause_agent(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
) -> Response {
    // Pause agent logic
    todo!()
}

async fn stop_agent(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
) -> Response {
    // Stop agent logic
    todo!()
}

async fn create_channel(
    State(_state): State<SharedState>,
    Path(_id): Path<String>,
    Json(_channel_config): Json<ChannelConfig>,
) -> Response {
    // Create channel logic
    todo!()
}

async fn get_channel(
    State(_state): State<SharedState>,
    Path((_id, _channel)): Path<(String, String)>,
) -> Response {
    // Get channel details
    todo!()
}

async fn stream_channel(
    State(state): State<SharedState>,
    Path((id, channel)): Path<(String, String)>,
) -> impl IntoResponse {
    let mut client = state.lock().await;
    
    match client.stream_channel(id, channel).await {
        Ok(mut stream) => {
            // Convert gRPC stream to SSE
            let stream = async_stream::stream! {
                while let Some(result) = stream.message().await.transpose() {
                    match result {
                        Ok(msg) => {
                            yield Ok::<_, Infallible>(Event::default()
                                .data(serde_json::to_string(&ChannelMessageWrapper::from(msg)).unwrap())
                                .event("message"))
                        }
                        Err(e) => {
                            yield Ok::<_, Infallible>(Event::default()
                                .data(format!("Error: {}", e))
                                .event("error"))
                        }
                    }
                }
            };
            
            Sse::new(stream).into_response()
        },
        Err(status) => {
            (StatusCode::INTERNAL_SERVER_ERROR, status.message().to_string()).into_response()
        }
    }
}

async fn delete_channel(
    State(_state): State<SharedState>,
    Path((_id, _channel)): Path<(String, String)>,
) -> Response {
    // Delete channel logic
    todo!()
}

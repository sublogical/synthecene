use axum::{
    routing::{get, post, put, delete},
    Router,
    response::sse::{Event, Sse},
    response::IntoResponse,
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::Response,
};
use futures::stream::{self, Stream};
use std::convert::Infallible;
use serde::{Deserialize, Serialize};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use glimmer::{ ChannelMessage,GlimmerConnection };
use tokio::sync::Mutex;
use deadpool::managed::{Manager, Pool, Metrics, Object};
use std::future::Future;
use tonic::{Status, Streaming, transport::Error as TonicError, Code};
use async_trait::async_trait;

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
    grpc_connection: bool,
    message: Option<String>,
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
    name: String,
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

#[derive(Clone)]
struct GlimmerManager {
    grpc_address: String,
}

#[async_trait]
impl Manager for GlimmerManager {
    type Type = GlimmerConnection;
    type Error = tonic::transport::Error;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let grpc_address = self.grpc_address.clone();
        let conn = GlimmerConnection::connect(&grpc_address).await?;
        Ok(conn)
    }

    async fn recycle(
        &self,
        conn: &mut Self::Type,
        _metrics: &Metrics,
    ) -> deadpool::managed::RecycleResult<Self::Error> {
        match conn.health_check().await {
            Ok(_) => Ok(()),
            Err(_) => Err(deadpool::managed::RecycleError::Message("Connection unhealthy".into()))
        }
    }
}

type GlimmerPool = Pool<GlimmerManager>;

#[derive(Clone)]
struct AppState {
    pool: GlimmerPool,
}

impl AppState {
    async fn new(grpc_address: String) -> Self {
        let manager = GlimmerManager { grpc_address };
        let pool = Pool::builder(manager)
            .max_size(32)
            .build()
            .expect("Failed to create connection pool");

        Self { pool }
    }
}

#[derive(Debug)]
enum GrpcError {
    Transport(TonicError),
    Status(Status),
}

impl From<TonicError> for GrpcError {
    fn from(err: TonicError) -> Self {
        GrpcError::Transport(err)
    }
}

impl From<Status> for GrpcError {
    fn from(status: Status) -> Self {
        GrpcError::Status(status)
    }
}

// Helper trait for handling gRPC connections
trait GrpcHandler {
    async fn with_grpc_conn<F, Fut, T>(pool: &GlimmerPool, f: F) -> Response
    where
        F: FnOnce(Object<GlimmerManager>) -> Fut,
        Fut: Future<Output = Result<T, GrpcError>>,
        T: IntoResponse;
}

// Implement for Response to allow usage in all handlers
impl GrpcHandler for Response {
    async fn with_grpc_conn<F, Fut, T>(pool: &GlimmerPool, f: F) -> Response
    where
        F: FnOnce(Object<GlimmerManager>) -> Fut,
        Fut: Future<Output = Result<T, GrpcError>>,
        T: IntoResponse,
    {
        match pool.get().await {
            Ok(conn) => {
                match f(conn).await {
                    Ok(response) => response.into_response(),
                    Err(e) => match e {
                        GrpcError::Transport(e) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            format!("Transport error: {}", e),
                        ).into_response(),
                        GrpcError::Status(s) => (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            s.message().to_string(),
                        ).into_response(),
                    }
                }
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Failed to get connection: {}", e),
            ).into_response(),
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // Initialize connection pool
    let state = AppState::new(args.grpc_address).await;

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
        .with_state(state);

    // Run server
    let listener = tokio::net::TcpListener::bind(&args.address).await.unwrap();
    println!("Server running on http://{}", args.address);
    axum::serve(listener, app).await.unwrap();
}

// Basic health check endpoint
async fn health_check(
    State(state): State<AppState>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        match conn.health_check().await {
            Ok(response) => {
                Ok(Json(HealthResponse {
                    status: if response.status { "healthy" } else { "degraded" }.to_string(),
                    grpc_connection: true,
                    message: if response.message.is_empty() { None } else { Some(response.message) },
                }))
            },
            Err(e) => {
                Ok(Json(HealthResponse {
                    status: "degraded".to_string(),
                    grpc_connection: false,
                    message: Some(format!("gRPC error: {}", e)),
                }))
            }
        }
    })
    .await
}

// Handler functions
async fn create_agent(
    State(state): State<AppState>,
    Json(payload): Json<CreateAgentRequest>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let response = conn.create_agent(payload.id).await.map_err(GrpcError::from)?;
        Ok(Json(AgentResponseWrapper::from(response)))
    }).await
}

async fn get_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let response = conn.get_agent(id).await.map_err(GrpcError::from)?;
        Ok(Json(AgentResponseWrapper::from(response)))
    }).await
}

async fn delete_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.delete_agent(id).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn get_properties(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let properties = conn.get_properties(id).await.map_err(GrpcError::from)?;
        let response = PropertyResponseWrapper {
            success: true,
            message: "Properties retrieved successfully".to_string(),
            properties: properties.properties,
        };
        Ok(Json(response))
    }).await
}

async fn get_property(
    State(state): State<AppState>,
    Path((id, key)): Path<(String, String)>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let property = conn.get_property(id, key.clone()).await.map_err(GrpcError::from)?;
        let mut properties = HashMap::new();
        properties.insert(key, property.properties.get("value").unwrap_or(&String::new()).clone());
        let response = PropertyResponseWrapper {
            success: true,
            message: "Property retrieved successfully".to_string(),
            properties,
        };
        Ok(Json(response))
    }).await
}

async fn set_property(
    State(state): State<AppState>,
    Path((id, key)): Path<(String, String)>,
    Json(value): Json<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let response = conn.set_property(id, key, value).await.map_err(GrpcError::from)?;
        Ok(Json(PropertyResponseWrapper::from(response)))
    }).await
}

async fn delete_property(
    State(state): State<AppState>,
    Path((id, key)): Path<(String, String)>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.delete_property(id, key).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn start_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.start_agent(id).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn pause_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.pause_agent(id).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn stop_agent(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.stop_agent(id).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

async fn create_channel(
    State(state): State<AppState>,
    Path(id): Path<String>,
    Json(channel_config): Json<ChannelConfig>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let response = conn.create_channel(id, channel_config.name).await.map_err(GrpcError::from)?;
        Ok(Json(ChannelResponseWrapper::from(response)))
    }).await
}

async fn get_channel(
    State(state): State<AppState>,
    Path((id, channel)): Path<(String, String)>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let response = conn.get_channel(id, channel).await.map_err(GrpcError::from)?;
        Ok(Json(ChannelResponseWrapper::from(response)))
    }).await
}

async fn stream_channel(
    State(state): State<AppState>,
    Path((id, channel)): Path<(String, String)>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        let stream = conn.stream_channel(id.clone(), channel.clone()).await.map_err(GrpcError::from)?;
        Ok(handle_channel_stream(stream, id, channel))
    }).await
}

// Helper function for stream handling
fn handle_channel_stream(
    mut stream: Streaming<ChannelMessage>,
    _id: String,
    _channel: String,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        while let Some(result) = stream.message().await.transpose() {
            match result {
                Ok(msg) => {
                    yield Ok(Event::default()
                        .data(serde_json::to_string(&ChannelMessageWrapper::from(msg)).unwrap())
                        .event("message"))
                }
                Err(e) => {
                    yield Ok(Event::default()
                        .data(format!("Error: {}", e))
                        .event("error"));

                    // TODO: Try to reconnect
                    break;
                }
            }
        }
    };
    
    Sse::new(stream)
}

async fn delete_channel(
    State(state): State<AppState>,
    Path((id, channel)): Path<(String, String)>,
) -> Response {
    Response::with_grpc_conn(&state.pool, |mut conn| async move {
        conn.delete_channel(id, channel).await.map_err(GrpcError::from)?;
        Ok(StatusCode::NO_CONTENT)
    }).await
}

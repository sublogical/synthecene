pub mod glimmer_api {
    tonic::include_proto!("glimmer_api"); // The string specified here must match the proto package name
}

use glimmer_api::glimmer_server::{ Glimmer, GlimmerServer };
use glimmer_api::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status, transport::Server};
use std::pin::Pin;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use futures::Stream;
use futures::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{info, warn, error, instrument};

// In-memory state for our service
#[derive(Debug, Default)]
struct AgentState {
    properties: HashMap<String, String>,
    status: i32, // Maps to AgentStatus enum
    channels: HashMap<String, Vec<ChannelMessage>>,
}

#[derive(Debug, Default)]
pub struct GlimmerService {
    agents: Arc<RwLock<HashMap<String, AgentState>>>,
}

#[tonic::async_trait]
impl Glimmer for GlimmerService {
    #[instrument(skip(self))]
    async fn create_agent(
        &self,
        request: Request<CreateAgentRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        info!(agent_id = %req.id, "Creating new agent");
        
        let mut agents = self.agents.write().await;
        if agents.contains_key(&req.id) {
            warn!(agent_id = %req.id, "Agent already exists");
            return Err(Status::already_exists("Agent already exists"));
        }

        agents.insert(req.id.clone(), AgentState::default());
        info!(agent_id = %req.id, "Agent created successfully");

        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent created successfully".to_string(),
            agent: Some(Agent {
                id: req.id,
                status: AgentStatus::Stopped as i32,
                properties: HashMap::new(),
            }),
        }))
    }

    #[instrument(skip(self))]
    async fn get_agent(
        &self,
        request: Request<GetAgentRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        info!(agent_id = %req.id, "Getting agent");
        
        let agents = self.agents.read().await;
        let state = agents.get(&req.id)
            .ok_or_else(|| {
                warn!(agent_id = %req.id, "Agent not found");
                Status::not_found("Agent not found")
            })?;

        info!(agent_id = %req.id, "Agent found");
        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent found".to_string(),
            agent: Some(Agent {
                id: req.id,
                status: state.status,
                properties: state.properties.clone(),
            }),
        }))
    }

    #[instrument(skip(self))]
    async fn delete_agent(
        &self,
        request: Request<DeleteAgentRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        info!(agent_id = %req.id, "Deleting agent");
        
        let mut agents = self.agents.write().await;
        if agents.remove(&req.id).is_none() {
            warn!(agent_id = %req.id, "Agent not found for deletion");
            return Err(Status::not_found("Agent not found"));
        }

        info!(agent_id = %req.id, "Agent deleted successfully");
        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent deleted successfully".to_string(),
            agent: None,
        }))
    }

    #[instrument(skip(self))]
    async fn get_properties(
        &self,
        request: Request<GetPropertiesRequest>,
    ) -> Result<Response<PropertyResponse>, Status> {
        let req = request.into_inner();
        info!(agent_id = %req.agent_id, "Getting all properties");
        
        let agents = self.agents.read().await;
        let state = agents.get(&req.agent_id)
            .ok_or_else(|| {
                warn!(agent_id = %req.agent_id, "Agent not found");
                Status::not_found("Agent not found")
            })?;

        info!(agent_id = %req.agent_id, property_count = %state.properties.len(), "Properties retrieved");
        Ok(Response::new(PropertyResponse {
            success: true,
            message: "Properties retrieved".to_string(),
            properties: state.properties.clone(),
        }))
    }

    async fn get_property(
        &self,
        request: Request<GetPropertyRequest>,
    ) -> Result<Response<PropertyResponse>, Status> {
        let req = request.into_inner();
        let agents = self.agents.read().await;

        let state = agents.get(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        let mut properties = HashMap::new();
        if let Some(value) = state.properties.get(&req.key) {
            properties.insert(req.key, value.clone());
        }

        Ok(Response::new(PropertyResponse {
            success: true,
            message: "Property retrieved".to_string(),
            properties,
        }))
    }

    #[instrument(skip(self))]
    async fn set_property(
        &self,
        request: Request<SetPropertyRequest>,
    ) -> Result<Response<PropertyResponse>, Status> {
        let req = request.into_inner();
        info!(
            agent_id = %req.agent_id,
            key = %req.key,
            value = %req.value,
            "Setting property"
        );
        
        let mut agents = self.agents.write().await;
        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| {
                warn!(agent_id = %req.agent_id, "Agent not found");
                Status::not_found("Agent not found")
            })?;

        state.properties.insert(req.key.clone(), req.value.clone());
        info!(
            agent_id = %req.agent_id,
            key = %req.key,
            "Property set successfully"
        );

        Ok(Response::new(PropertyResponse {
            success: true,
            message: "Property set successfully".to_string(),
            properties: state.properties.clone(),
        }))
    }

    async fn delete_property(
        &self,
        request: Request<DeletePropertyRequest>,
    ) -> Result<Response<PropertyResponse>, Status> {
        let req = request.into_inner();
        let mut agents = self.agents.write().await;

        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        state.properties.remove(&req.key);

        Ok(Response::new(PropertyResponse {
            success: true,
            message: "Property deleted successfully".to_string(),
            properties: state.properties.clone(),
        }))
    }

    #[instrument(skip(self))]
    async fn start_agent(
        &self,
        request: Request<AgentLifecycleRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        info!(agent_id = %req.agent_id, "Starting agent");
        
        let mut agents = self.agents.write().await;
        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| {
                warn!(agent_id = %req.agent_id, "Agent not found");
                Status::not_found("Agent not found")
            })?;

        state.status = AgentStatus::Running as i32;
        info!(agent_id = %req.agent_id, "Agent started successfully");

        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent started successfully".to_string(),
            agent: Some(Agent {
                id: req.agent_id,
                status: state.status,
                properties: state.properties.clone(),
            }),
        }))
    }

    async fn pause_agent(
        &self,
        request: Request<AgentLifecycleRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        let mut agents = self.agents.write().await;

        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        state.status = AgentStatus::Paused as i32;

        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent paused successfully".to_string(),
            agent: Some(Agent {
                id: req.agent_id,
                status: state.status,
                properties: state.properties.clone(),
            }),
        }))
    }

    async fn stop_agent(
        &self,
        request: Request<AgentLifecycleRequest>,
    ) -> Result<Response<AgentResponse>, Status> {
        let req = request.into_inner();
        let mut agents = self.agents.write().await;

        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        state.status = AgentStatus::Stopped as i32;

        Ok(Response::new(AgentResponse {
            success: true,
            message: "Agent stopped successfully".to_string(),
            agent: Some(Agent {
                id: req.agent_id,
                status: state.status,
                properties: state.properties.clone(),
            }),
        }))
    }

    async fn create_channel(
        &self,
        request: Request<CreateChannelRequest>,
    ) -> Result<Response<ChannelResponse>, Status> {
        let req = request.into_inner();
        let mut agents = self.agents.write().await;

        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        if state.channels.contains_key(&req.channel_id) {
            return Err(Status::already_exists("Channel already exists"));
        }

        state.channels.insert(req.channel_id.clone(), Vec::new());

        Ok(Response::new(ChannelResponse {
            success: true,
            message: "Channel created successfully".to_string(),
            channel_id: req.channel_id,
        }))
    }

    async fn get_channel(
        &self,
        request: Request<GetChannelRequest>,
    ) -> Result<Response<ChannelResponse>, Status> {
        let req = request.into_inner();
        let agents = self.agents.read().await;

        let state = agents.get(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        if !state.channels.contains_key(&req.channel_id) {
            return Err(Status::not_found("Channel not found"));
        }

        Ok(Response::new(ChannelResponse {
            success: true,
            message: "Channel found".to_string(),
            channel_id: req.channel_id,
        }))
    }

    async fn delete_channel(
        &self,
        request: Request<DeleteChannelRequest>,
    ) -> Result<Response<ChannelResponse>, Status> {
        let req = request.into_inner();
        let mut agents = self.agents.write().await;

        let state = agents.get_mut(&req.agent_id)
            .ok_or_else(|| Status::not_found("Agent not found"))?;

        if state.channels.remove(&req.channel_id).is_none() {
            return Err(Status::not_found("Channel not found"));
        }

        Ok(Response::new(ChannelResponse {
            success: true,
            message: "Channel deleted successfully".to_string(),
            channel_id: req.channel_id,
        }))
    }

    type StreamChannelStream = Pin<Box<dyn Stream<Item = Result<ChannelMessage, Status>> + Send>>;

    #[instrument(skip(self))]
    async fn stream_channel(
        &self,
        request: Request<GetChannelRequest>,
    ) -> Result<Response<Self::StreamChannelStream>, Status> {
        let req = request.into_inner();
        info!(
            agent_id = %req.agent_id,
            channel_id = %req.channel_id,
            "Starting channel stream"
        );
        
        let agents = self.agents.read().await;
        let state = agents.get(&req.agent_id)
            .ok_or_else(|| {
                warn!(agent_id = %req.agent_id, "Agent not found");
                Status::not_found("Agent not found")
            })?;

        if !state.channels.contains_key(&req.channel_id) {
            warn!(
                agent_id = %req.agent_id,
                channel_id = %req.channel_id,
                "Channel not found"
            );
            return Err(Status::not_found("Channel not found"));
        }

        // Create a channel for streaming messages
        let (tx, rx) = mpsc::channel(128);
        let agent_id = req.agent_id.clone();
        let channel_id = req.channel_id.clone();

        info!(
            agent_id = %agent_id,
            channel_id = %channel_id,
            "Channel stream established"
        );

        // Create the output stream
        let output_stream = ReceiverStream::new(rx).map(Ok::<_, Status>);

        // Example: Send a test message every 5 seconds
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let msg = ChannelMessage {
                    agent_id: agent_id.clone(),
                    channel_id: channel_id.clone(),
                    content: "Test message".to_string(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i64,
                };
                if tx.send(msg).await.is_err() {
                    error!(
                        agent_id = %agent_id,
                        channel_id = %channel_id,
                        "Failed to send message to channel"
                    );
                    break;
                }
            }
        });

        Ok(Response::new(Box::pin(output_stream)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("glimmer=debug,tower_http=debug")
        .init();

    let addr = "[::1]:50052".parse()?;
    let glimmer = GlimmerService::default();
    info!(%addr, "Starting GlimmerService");
    
    Server::builder()
        .add_service(GlimmerServer::new(glimmer))
        .serve(addr)
        .await?;

    Ok(())
}
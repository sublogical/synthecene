pub mod glimmer_api {
    tonic::include_proto!("glimmer_api");
}

use glimmer_api::glimmer_client::GlimmerClient;
use tonic::transport::Channel;
use tonic::{Request, Streaming, Status};
use std::collections::HashMap;
use glimmer_api::{
    AgentResponse,
    CreateAgentRequest, 
    create_agent_request::OptionalTemplate::TemplateUri,
    HealthCheckRequest, 
    HealthCheckResponse
};

// Re-export common types for convenience
pub use glimmer_api::{
    Agent, 
    AgentStatus, 
    ChannelMessage
}; 

pub struct GlimmerConnection {
    client: GlimmerClient<Channel>,
}

impl GlimmerConnection {
    pub async fn connect(addr: &str) -> Result<Self, tonic::transport::Error> {
        let client = GlimmerClient::connect(format!("http://{}", addr)).await?;
        Ok(Self { client })
    }

    // Agent CRUD operations
    pub async fn create_agent(
        &mut self,
        id: String,
        template_uri: Option<String>,
        override_parameters: HashMap<String, String>,
    ) -> Result<AgentResponse, Status> {
        let request = Request::new(CreateAgentRequest {
            id,
            optional_template: template_uri.map(|uri| {
                TemplateUri(uri)
            }),
            override_parameters,
        });
        let response = self.client.create_agent(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_agent(&mut self, id: String) -> Result<glimmer_api::AgentResponse, Status> {
        let request = Request::new(glimmer_api::GetAgentRequest { id });
        let response = self.client.get_agent(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_agent(&mut self, id: String) -> Result<glimmer_api::AgentResponse, Status> {
        let request = Request::new(glimmer_api::DeleteAgentRequest { id });
        let response = self.client.delete_agent(request).await?;
        Ok(response.into_inner())
    }

    // Property management
    pub async fn get_properties(&mut self, agent_id: String) -> Result<glimmer_api::PropertyResponse, Status> {
        let request = Request::new(glimmer_api::GetPropertiesRequest { agent_id });
        let response = self.client.get_properties(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_property(&mut self, agent_id: String, key: String) -> Result<glimmer_api::PropertyResponse, Status> {
        let request = Request::new(glimmer_api::GetPropertyRequest { agent_id, key });
        let response = self.client.get_property(request).await?;
        Ok(response.into_inner())
    }

    pub async fn set_property(
        &mut self,
        agent_id: String,
        key: String,
        value: String,
    ) -> Result<glimmer_api::PropertyResponse, Status> {
        let request = Request::new(glimmer_api::SetPropertyRequest {
            agent_id,
            key,
            value,
        });
        let response = self.client.set_property(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_property(
        &mut self,
        agent_id: String,
        key: String,
    ) -> Result<glimmer_api::PropertyResponse, Status> {
        let request = Request::new(glimmer_api::DeletePropertyRequest { agent_id, key });
        let response = self.client.delete_property(request).await?;
        Ok(response.into_inner())
    }

    // Lifecycle management
    pub async fn start_agent(&mut self, agent_id: String) -> Result<glimmer_api::AgentResponse, Status> {
        let request = Request::new(glimmer_api::AgentLifecycleRequest { agent_id });
        let response = self.client.start_agent(request).await?;
        Ok(response.into_inner())
    }

    pub async fn pause_agent(&mut self, agent_id: String) -> Result<glimmer_api::AgentResponse, Status> {
        let request = Request::new(glimmer_api::AgentLifecycleRequest { agent_id });
        let response = self.client.pause_agent(request).await?;
        Ok(response.into_inner())
    }

    pub async fn stop_agent(&mut self, agent_id: String) -> Result<glimmer_api::AgentResponse, Status> {
        let request = Request::new(glimmer_api::AgentLifecycleRequest { agent_id });
        let response = self.client.stop_agent(request).await?;
        Ok(response.into_inner())
    }

    // Channel management
    pub async fn create_channel(
        &mut self,
        agent_id: String,
        channel_id: String,
    ) -> Result<glimmer_api::ChannelResponse, Status> {
        let request = Request::new(glimmer_api::CreateChannelRequest {
            agent_id,
            channel_id,
        });
        let response = self.client.create_channel(request).await?;
        Ok(response.into_inner())
    }

    pub async fn get_channel(
        &mut self,
        agent_id: String,
        channel_id: String,
    ) -> Result<glimmer_api::ChannelResponse, Status> {
        let request = Request::new(glimmer_api::GetChannelRequest {
            agent_id,
            channel_id,
        });
        let response = self.client.get_channel(request).await?;
        Ok(response.into_inner())
    }

    pub async fn delete_channel(
        &mut self,
        agent_id: String,
        channel_id: String,
    ) -> Result<glimmer_api::ChannelResponse, Status> {
        let request = Request::new(glimmer_api::DeleteChannelRequest {
            agent_id,
            channel_id,
        });
        let response = self.client.delete_channel(request).await?;
        Ok(response.into_inner())
    }

    pub async fn stream_channel(
        &mut self,
        agent_id: String,
        channel_id: String,
    ) -> Result<Streaming<glimmer_api::ChannelMessage>, tonic::Status> {
        let request = tonic::Request::new(glimmer_api::GetChannelRequest {
            agent_id,
            channel_id,
        });
        let response = self.client.stream_channel(request).await?;
        Ok(response.into_inner())
    }

    pub async fn health_check(&mut self) -> Result<HealthCheckResponse, Status> {
        let request = Request::new(HealthCheckRequest {});
        let response = self.client.health_check(request).await?;
        Ok(response.into_inner())
    }

    // ... rest of the client methods ...
}

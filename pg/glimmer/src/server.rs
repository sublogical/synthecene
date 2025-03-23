pub mod glimmer_api {
    tonic::include_proto!("glimmer_api"); // The string specified here must match the proto package name
}

use glimmer_api::glimmer_server::{ Glimmer, GlimmerServer };
use glimmer_api::{ ChannelMessage, ChannelResponse, ContextRequest, ContextResponse, NotificationRequest, NotificationResponse };

use std::{error::Error, io::ErrorKind, net::ToSocketAddrs, pin::Pin, time::Duration};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status, Streaming};

#[derive(Debug, Default)]
pub struct GlimmerService {}

#[tonic::async_trait]
impl Glimmer for GlimmerService {
    type GetNotificationsStream = Pin<Box<dyn Stream<Item = Result<NotificationResponse, Status>> + Send>>;

    /// Stream of notifications from the server to the client
    async fn get_notifications(
        &self, 
        request: Request<NotificationRequest>
    ) -> Result<Response<Self::GetNotificationsStream>,Status>
    {
        println!("GlimmerServer::get_notifications");
        println!("\tclient connected from: {:?}", request.remote_addr());

        // creating infinite stream with requested message
        let repeat = std::iter::repeat(NotificationResponse {            
            message: "NOTIFICATION MESSAGE".into(),
            notification_type: "NOTIFICATION_TYPE".into(),
            timestamp: 0
        });
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_secs(10)));

        // spawn and channel are required if you want handle "disconnect" functionality
        // the `out_stream` will not be polled after client disconnect
        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Result::<_, Status>::Ok(item)).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::GetNotificationsStream
        ))
    }

    /// Report the current context to the server
    async fn report_context(
        &self,
        request: Request<ContextRequest>,
    ) -> Result<Response<ContextResponse>, Status>
    {
        Err(Status::unimplemented("report_context not yet implemented"))        
    }

    type SyncChannelStream = Pin<Box<dyn Stream<Item = Result<ChannelResponse, Status>> + Send>>;

    /// Bidirectional streaming for channel synchronization
    async fn sync_channel(
        &self,
        request: Request<Streaming<ChannelMessage>>,
    ) -> std::result::Result<Response<Self::SyncChannelStream>, Status>
    {
        Err(Status::unimplemented("sync_channel not yet implemented"))        
    }
    
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50052".parse()?;
    let glimmer = GlimmerService::default();
    println!("{} Starting GlimmerService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(GlimmerServer::new(glimmer))
        .serve(addr)
        .await?;

    Ok(())
}
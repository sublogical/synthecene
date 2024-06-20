pub mod porta_api {
    tonic::include_proto!("porta_api"); // The string specified here must match the proto package name
}

use porta_api::porta_client::PortaClient;
use porta_api::{MetricRequest, MetricResponse};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PortaClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(MetricRequest {
        metric_suri: "Tonic".into(),
    });

    let response = client.get_metrics(request).await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
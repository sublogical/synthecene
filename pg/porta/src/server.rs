pub mod porta_api {
    tonic::include_proto!("porta_api"); // The string specified here must match the proto package name
}
pub(crate) mod metrics;

use metrics::auto::sine_wave_metric;
use metrics::format_metric;
use porta_api::porta_server::{ Porta, PortaServer };
use porta_api::{ Gauge, MetricRequest, MetricResponse, Metric };

use tonic::{transport::Server, Request, Response, Status};
use yansi::{Paint, Style, Color::*};

#[derive(Debug, Default)]
pub struct PortaService {}

/**
// Create a `Router<Body, Infallible>` for response body type `hyper::Body`
// and for handler error type `Infallible`.
fn router() -> Router<Metric, Infallible> {
    // Create a router and specify the logger middleware and the handlers.
    // Here, "Middleware::pre" means we're adding a pre middleware which will be executed
    // before any route handlers.
    Router::builder()
        // Specify the state data which will be available to every route handlers,
        // error handler and middlewares.
        .data(State(100))
        .middleware(Middleware::pre(logger))
        .get("/", home_handler)
        .get("/users/:userId", user_handler)
        .err_handler_with_info(error_handler)
        .build()
        .unwrap()
}
 */

#[tonic::async_trait]
impl Porta for PortaService {
    async fn get_metrics(
        &self,
        request: Request<MetricRequest>, // Accept request of type MetricRequest
    ) -> Result<Response<MetricResponse>, Status> { // Return an instance of type MetricResponse

        let metric_suri = request.into_inner().metric_suri;

        println!("REQUEST /GetMetrics [count:{}, suri:{:?}]", 
            metric_suri.len(), metric_suri.first().unwrap());

        let reply = MetricResponse {
            metric: vec![Metric {
                data: Some(porta_api::metric::Data::Gauge(
                    Gauge {
                        data_points: sine_wave_metric(0, 100_000_000, 10, 1.0, 0.0, 1.0)
                    }
                )),
                ..Default::default()
            }]
        };

        Ok(Response::new(reply)) // Send back our formatted greeting
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let porta = PortaService::default();
    println!("{} Starting PortaService on {:?}", "🦑", addr);
    Server::builder()
        .add_service(PortaServer::new(porta))
        .serve(addr)
        .await?;

    Ok(())
}
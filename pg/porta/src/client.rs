use clap::{arg, Command};

pub mod porta_api {
    tonic::include_proto!("porta_api"); // The string specified here must match the proto package name
}

pub(crate) mod metrics;

use metrics::format_metric;

use porta_api::porta_client::PortaClient;
use porta_api::{MetricRequest, MetricResponse};
use yansi::{Paint, Style, Color::*};

fn cli() -> Command {
    Command::new("portac")
        .about("porta cli utility")
        .version("0.0.1")
        .subcommand_required(true)
        .arg_required_else_help(true)

        .subcommand(
            Command::new("view")
                .about("View a metric.")
                .arg(arg!(<SURI> "SURI describing the path to the metric (e.g. 'm.auto.sine.p2s+0')"))
                .arg(arg!(--stream "continuously stream metrics updates from service"))
        )
}


async fn handle_view(metric_suris:&Vec<&String>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PortaClient::connect("http://[::1]:50051").await?;

    println!(
        "Viewing {:?}",
        metric_suris
    );

    let request = tonic::Request::new(MetricRequest {
        metric_suri: vec!["Tonic".to_string()],
        ..Default::default()
    });

    let response = client.get_metrics(request).await?;

    let response = response.into_inner();

    for metric in response.metric {
        match metric.data {
            Some(porta_api::metric::Data::Gauge(guage)) => {
                println!("{}", format_metric(&guage.data_points));
            }
            _ => {}
        }        
    }

    Ok(())
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = cli().get_matches();

    match matches.subcommand() {
        Some(("view", sub_matches)) => {
            let suris = sub_matches
                .get_many::<String>("SURI")
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            handle_view(&suris).await;
        },
        Some((unexpected, _sub_matches)) => {
            println!(
                "Unexpected subcommand {}",
                unexpected
            );
        }
        None => {
            println!("{} Expected command!", "ERROR".red());
        }
    }

    Ok(())
}
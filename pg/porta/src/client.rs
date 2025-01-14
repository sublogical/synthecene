use clap::{arg, Command};

pub mod porta_api {
    tonic::include_proto!("porta_api"); // The string specified here must match the proto package name
}

pub(crate) mod metrics;

use metrics::format_metric;

use porta_api::porta_client::PortaClient;
use porta_api::MetricRequest;
use porta_api::number_data_point::Value;
use textplots::{Chart, ColorPlot, Shape};
use yansi::Paint;

const RED: rgb::RGB8 = rgb::RGB8::new(0xFF, 0x00, 0x00);

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
    let width = termsize::get().map(|size| size.cols).unwrap_or(80) as u32;

    println!(
        "Viewing {:?} @ {}",
        metric_suris, width
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
                let xy_pairs = guage.data_points.iter().map(|data_point| {
                    let x = data_point.time_unix_nano as f32 / 1_000_000_000.0;
                    let y = match data_point.value {
                        Some(Value::AsDouble(fval)) => (fval as f32).into(),
                        Some(Value::AsInt(ival)) => (ival as f32).into(),
                        None => {
                            panic!("unexpected datapoint with null value");
                        }
                    };
                    (x,y)
                }).collect::<Vec<(f32, f32)>>();

                let start = xy_pairs.first().unwrap().0;
                let end = xy_pairs.last().unwrap().0;
                
                let lines = Shape::Lines(&xy_pairs);
                Chart::new(width, 60, start, end)
                    .linecolorplot(&lines, RED)
                    .display();
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

            // todo: make this pass through the result
            let _ = handle_view(&suris).await;
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
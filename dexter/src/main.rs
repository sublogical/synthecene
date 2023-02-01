use tokio::time::{Duration, sleep};
use warp::Filter;

fn world() -> &'static str {
    "Hello, world!\n"
}

async fn delay(seconds: u64) -> Result<String, warp::Rejection> {
    sleep(Duration::from_secs(seconds)).await;
    Ok(format!("Waited for {} seconds\n", seconds))
}

#[tokio::main]
async fn main() {
    let hello = warp::path!("hello")
        .map(world);

    let delay = warp::path!("delay" / u64)
        .and_then(delay);

    let routes = hello.or(delay);

    warp::serve(routes)
        .run(([127, 0, 0, 1], 3030))
        .await;
}
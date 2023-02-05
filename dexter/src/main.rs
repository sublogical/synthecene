use warp::Filter;
use yansi::Paint;

mod chat;
mod inference;

pub trait PaintExt {
    fn emoji(item: &str) -> Paint<&str>;
}

impl PaintExt for Paint<&str> {
    /// Paint::masked(), but hidden on Windows due to broken output. See #1122.
    fn emoji(_item: &str) -> Paint<&str> {
        #[cfg(windows)] { Paint::masked("") }
        #[cfg(not(windows))] { Paint::masked(_item) }
    }
}
#[tokio::main]
async fn main() {
    env_logger::init();

    let port = 3030;

    let cors = warp::cors()
        .allow_origins(vec!["http://127.0.0.1:3000", "https://localhost:3000", "http://localhost:3000/"])
        .allow_headers(vec!["User-Agent", "Sec-Fetch-Mode", "Referer", "Origin", "Access-Control-Request-Method", "Access-Control-Request-Headers", "content-type"])
        .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"]);

    let chat_routes = chat::routes();

    let routes = chat_routes.with(cors).with(warp::log("cors test"));

    println!("{} Starting Dexter", Paint::emoji("🦞"));
    println!("  port: {}", Paint::blue(port).bold());

    warp::serve(routes)
        .run(([127, 0, 0, 1], port))
        .await;
}
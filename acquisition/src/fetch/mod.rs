use std::time::{SystemTime, Instant, UNIX_EPOCH};

use calico_shared::result::CalicoResult;
use acquisition::protocol;
use reqwest;
use robotstxt::DefaultMatcher;
use scraper::{Html, Selector};

pub mod controller;
pub mod smarts;
pub mod state;
pub mod task;

fn full_url(host: &protocol::Host, relative_url: &str) -> String{

    let scheme = match &host.scheme {
        Some(scheme) => scheme.clone(),
        None => "https".to_string()
    };

    match host.port {
        Some(port) => format!("{}://{}:{}{}", scheme, host.hostname, port, relative_url),
        None => format!("{}//{}{}", scheme, host.hostname, relative_url),
    }
}

#[derive(Clone, Debug, Default)]
pub struct Capture {
    // raw HTML body of page
    body: String,

    /// Time the fetch was started, in ms since epoch
    fetched_at: u64,

    /// Time the fetch took to return, in ms
    fetch_time: u64,

    /// Content-length in bytes
    content_length: u64,

    status_code: u32,

    /// Unique list of links on the page, unscored, sorted by first appearance
    outlinks: Vec<String>
}


const INDIGO_USER_AGENT: &str = r"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/600.2.5 (KHTML\, like Gecko) Version/8.0.2 Safari/600.2.5 (Panubot/0.1; +https://developer.panulirus.com/support/panubot)";

fn robots_filter(robots: &Option<String>, url: &str) -> bool {
    // Step 1. Determine whether we're allowed to crawl this site
    match &robots {
        Some(robots_txt) => {
            let mut matcher = DefaultMatcher::default();
            if !matcher.one_agent_allowed_by_robots(robots_txt, INDIGO_USER_AGENT, &url) {
                false
            } else {
                true
            }
        },
        None => { true }
    }
}

async fn retrieve(url: String) -> CalicoResult<Capture> {
    let fetched_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Should always be able to get time since EPOCH")
        .as_millis()
        .try_into()
        .expect("It's ok if this code stops working in 584M years, really");

    let start = Instant::now();
    let resp = reqwest::get(url).await?;
    let content_length = resp.content_length();
    let status_code:u32 = resp.status().as_u16().into();
    
    let body = resp.text().await?;
    let content_length = content_length.unwrap_or(body.len().try_into()
        .expect("Should never take more than 584M years to request a web page"));

    let fetch_time = start.elapsed().as_millis().try_into().unwrap();
    let outlinks = extract_links(&body);

    Ok(Capture {
        body,
        fetch_time,
        content_length,
        status_code,
        outlinks,
        fetched_at
    })
}

fn extract_links(text: &String) -> Vec<String> {
    let document = Html::parse_document(&text);
    let selector = Selector::parse(r#"a"#).unwrap();

    let links:Vec<String> = document.select(&selector).into_iter()
        .filter_map(|n| n.value().attr("href"))
        .map(|s| s.to_string())
        .collect();

    links
}


#[cfg(test)]
mod tests {
    use std::{path::{PathBuf, Path}, time::{Instant, Duration}};

    use mockito;
    use mockito::{mock, Mock, Matcher};
    use rand::{thread_rng, Rng};
    use rand::distributions::Alphanumeric;
    use tempfile::tempdir;

    use crate::fetch::{*, state::DomainState};

    pub fn mockito_host() -> protocol::Host{
        let address = mockito::server_address();

        protocol::Host {
            scheme: Some("http".to_string()),
            hostname: address.ip().to_string(),
            port: Some(address.port() as u32),
            ..Default::default()
        }
    }

    pub fn with_friendly_robots() -> Mock {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/yes_robots.txt");
        mock("GET", "/robots.txt")
            .with_status(200)
            .with_body_from_file(d)
            .create()
    }

    pub fn with_unfriendly_robots() -> Mock {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/no_robots.txt");
        mock("GET", "/robots.txt")
            .with_status(200)
            .with_body_from_file(d)
            .create()
    }

    pub fn with_text(path: &str, body: &str) -> Mock {
        mock("GET", path)
            .with_status(200)
            .with_body(body)
            .create()
    }

    pub fn with_spider_trap() -> Mock {
        mock("GET", Matcher::Regex(r"^/req/.*$".to_string()))
            .with_status(200)
            .with_body("FARM")
            .with_body_from_fn(|w| {
                let rand_string: String = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .map(char::from)
                    .collect();
                let body = format!("<html><body><a href=\"/req/{}\"</a></body></html>", rand_string);
                w.write_all(body.as_bytes())
            })
            .create()
    }

    pub fn url(path: &str) -> String{
        let host = &mockito::server_url();
        let url = format!("{}{}", host, path);
        url
    }

    #[tokio::test]
    async fn test_inner_retriever() {
        let host = &mockito::server_url();

        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/yes_robots.txt");
            
        let _m = mock("GET", "/hello")
            .with_status(200)
            .with_body("world")
            .create();
        let url = format!("{}/hello", host);

        let capture = retrieve(url).await.unwrap();

        assert_eq!(capture.body, "world");
    }
    #[tokio::test]
    async fn test_yes_robots() {
        let temp = tempdir().unwrap();
        let _m = [
            with_friendly_robots(),
            with_text("/hello", "world")
        ];
        let remote_path = Path::new("crawler_state");

        let state = DomainState::init(&mockito_host(), temp.path(), remote_path).await.unwrap();
        assert!(robots_filter(&state.robots, &url("/hello")));
    }
    #[tokio::test]
    async fn test_no_robots() {
        let temp = tempdir().unwrap();
        let _m = [
            with_unfriendly_robots(),
            with_text("/hello", "world")
        ];
        let remote_path = Path::new("crawler_state");

        let state = DomainState::init(&mockito_host(), temp.path(), remote_path).await.unwrap();
        assert!(!robots_filter(&state.robots, &url("/hello")));
    }
    #[tokio::test]
    async fn test_link_farm() {
        let _m = [
            with_spider_trap()
        ];

        let now = Instant::now();

        let mut path = "/req/abc".to_string();
        for _ in 1..101 {
            let capture = retrieve(url(&path)).await.unwrap();
        
            path = capture.outlinks[0].clone();
        }
        let elapsed = now.elapsed();
        assert!(elapsed < Duration::from_secs(5))
        //todo: extract links & loop around a bit
    }
  }
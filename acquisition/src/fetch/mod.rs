use acquisition::result::{IndigoResult, IndigoError};
use reqwest;
use robotstxt::DefaultMatcher;
use rand::{thread_rng, Rng};
use rand::distributions::Alphanumeric;
use scraper::{Html, Selector};


#[derive(Clone, Debug, Default)]
pub struct Host {
    scheme: String,
    hostname: String,
    port: u16
}

#[derive(Clone, Debug, Default)]
pub struct Capture {
    body: String
}
#[derive(Clone, Debug, Default)]

struct DomainState {
    host: Host,
    robots: Option<String>,
    ms_since_last_fetch: u64,
    fetch_rate_ms: u64
}

impl DomainState {
    async fn for_domain(host: &Host) -> IndigoResult<DomainState> {
        let url = format!("{}://{}:{}/robots.txt", host.scheme, host.hostname, host.port);
        let robots_capture = inner_retrieve(url).await?;

        Ok(DomainState {
            host: host.clone(),
            robots: Some(robots_capture.body),
            ms_since_last_fetch: 0,
            fetch_rate_ms: 0
        })
    }
}

const INDIGO_USER_AGENT: &str = r"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/600.2.5 (KHTML\, like Gecko) Version/8.0.2 Safari/600.2.5 (Panubot/0.1; +https://developer.panulirus.com/support/panubot)";

async fn retrieve(state: DomainState, url: String) -> IndigoResult<(Capture, DomainState)> {
    // Step 1. Determine whether we're allowed to crawl this site
    match &state.robots {
        Some(robots_txt) => {
            let mut matcher = DefaultMatcher::default();
            if !matcher.one_agent_allowed_by_robots(robots_txt, INDIGO_USER_AGENT, &url) {
                return Err(IndigoError::RobotForbidden)
            }
        },
        None => {}
    }
    Ok((inner_retrieve(url).await?, state))
}

async fn inner_retrieve(url: String) -> IndigoResult<Capture> {
    let resp = reqwest::get(url).await?;
    let body = resp.text().await?;
    
    let capture = Capture {
        body
    };

    Ok(capture)
}

fn extract_links(text: String) -> Vec<String> {
    let document = Html::parse_document(&text);
    let selector = Selector::parse(r#"a"#).unwrap();

    let links:Vec<String> = document.select(&selector).into_iter()
        .filter_map(|n| n.value().attr("href"))
        .map(|s| s.to_string())
        .collect();

    links
}


#[cfg(test)]
use mockito;

mod tests {
    use std::{path::PathBuf, time::{Instant, Duration}};

    use mockito::{mock, Mock, Matcher};

    use crate::fetch::*;

    fn mockito_host() -> Host{
        let address = mockito::server_address();

        Host {
            scheme: "http".to_string(),
            hostname: address.ip().to_string(),
            port: address.port()
        }
    }

    fn with_friendly_robots() -> Mock {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/yes_robots.txt");
        mock("GET", "/robots.txt")
            .with_status(200)
            .with_body_from_file(d)
            .create()
    }

    fn with_unfriendly_robots() -> Mock {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test/no_robots.txt");
        mock("GET", "/robots.txt")
            .with_status(200)
            .with_body_from_file(d)
            .create()
    }

    fn with_text(path: &str, body: &str) -> Mock {
        mock("GET", path)
            .with_status(200)
            .with_body(body)
            .create()
    }

    fn with_spider_trap() -> Mock {
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

    fn url(path: &str) -> String{
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

        let capture = inner_retrieve(url).await.unwrap();

        assert_eq!(capture.body, "world");
    }
    #[tokio::test]
    async fn test_yes_robots() {
        let _m = [
            with_friendly_robots(),
            with_text("/hello", "world")
        ];

        let state = DomainState::for_domain(&mockito_host()).await.unwrap();
        let (capture, _state) = retrieve(state, url("/hello")).await.unwrap();
        assert_eq!(capture.body, "world");
    }
    #[tokio::test]
    async fn test_no_robots() {
        let _m = [
            with_unfriendly_robots(),
            with_text("/hello", "world")
        ];

        let state = DomainState::for_domain(&mockito_host()).await.unwrap();
        let res = retrieve(state, url("/hello")).await;
        assert!(matches!(res, Err(IndigoError::RobotForbidden)));
    }
    #[tokio::test]
    async fn test_link_farm() {
        let _m = [
            with_friendly_robots(),
            with_spider_trap()
        ];

        let mut state = DomainState::for_domain(&mockito_host()).await.unwrap();

        let now = Instant::now();

        let mut path = "/req/abc".to_string();
        for _ in 1..101 {
            let (capture, new_state) = retrieve(state, url(&path)).await.unwrap();
            let links = extract_links(capture.body);
            path = links[0].clone();
            state = new_state;
        }
        let elapsed = now.elapsed();
        assert!(elapsed < Duration::from_secs(5))
        //todo: extract links & loop around a bit
    }
  }
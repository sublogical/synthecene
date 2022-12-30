use acquisition::result::IndigoResult;
use reqwest;

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

async fn retrieve(state: DomainState, url: String) -> IndigoResult<(Capture, DomainState)> {
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


#[cfg(test)]
use mockito;

mod tests {
    use std::path::PathBuf;

    use mockito::{mock, Mock};

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

    fn with_text(path: &str, body: &str) -> Mock {
        mock("GET", path)
            .with_status(200)
            .with_body(body)
            .create()
    }

    fn url(path: &str) -> String{
        let host = &mockito::server_url();
        let url = format!("{}/hello", host);
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
  }
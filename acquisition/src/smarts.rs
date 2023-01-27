use crate::protocol;
use lazy_static::lazy_static;
use regex::Regex;

enum _PriorityBin {
    Urgent,
    Important,
    NotImportant,
    Ignore
}

struct _Priority {
    score: f32,
    bin: _PriorityBin
}

trait FetchSmarts {
    fn url_priority(host: protocol::Host, url: String) -> _Priority;
}

lazy_static! {
    static ref SCHEME_MATCHER: Regex = Regex::new(r"^([a-z][a-z0-9+\-.]*):").unwrap();
}

pub fn normalize_links<'a, T>(url: reqwest::Url, links: T) -> (Vec<String>, Vec<String>)
where T: IntoIterator<Item=&'a String> {
    let mut inlinks:Vec<String> = Vec::new();
    let mut outlinks:Vec<String> = Vec::new();

    for link in links {
        if SCHEME_MATCHER.is_match(link) {
            // This URL starts with a scheme (http://, https://, etc.)

            // todo: consider that the link might be fully qualified to the same host!
            // todo: consider that the hostname www.X.com is the same as x.com
            outlinks.push(link.to_string());
        } else if link.starts_with("//") {
            // This URL starts with a double-slash, which means it's a scheme-relative URL

            let link = format!("{}:{}", url.scheme(), link);
            outlinks.push(link);

        } else if link.starts_with("/") {
            // This URL starts with a slash, which means it's a host-relative URL
            
            inlinks.push(link.to_string());
        } else {
            let base_path = url.path();
            // find index of last slash
            match base_path.chars().rev().position(|c| c == '/') {
                Some(i) => {
                    let base_path = &base_path[..base_path.len() - i];
                    let link = format!("{}{}", base_path, link);
                    inlinks.push(link);
                },
                None => {
                    // there should always be a slash in the base path
                    let link = format!("/{}", link);
                    inlinks.push(link);
                }
            }
        }
    }

    (inlinks, outlinks)
}

pub fn estimate_compression(_num_captures: f64, _content_bytes: f64) -> f64 {
    0.3
}

#[cfg(test)]
mod tests {
    use super::SCHEME_MATCHER;

    #[test]
    fn test_matchers() {
        assert!(SCHEME_MATCHER.is_match("http://www.google.com"));
        assert!(SCHEME_MATCHER.is_match("https://www.google.com"));
        assert!(SCHEME_MATCHER.is_match("z39.50://www.google.com"));
        assert!(SCHEME_MATCHER.is_match("web+soup://www.google.com"));
        assert!(!SCHEME_MATCHER.is_match("//www.google.com"));
        assert!(!SCHEME_MATCHER.is_match("/search?q=hello"));
        assert!(!SCHEME_MATCHER.is_match("search?q=hello"));
    }

    #[test]
    fn test_normalize_url() {
        let url = reqwest::Url::parse("http://www.google.com/search?q=hello").unwrap();
        let links = vec![
            "http://www.google.com/search?q=hello".to_string(),
            "https://www.google.com/search?q=hello".to_string(),
            "z39.50://www.google.com/search?q=hello".to_string(),
            "web+soup://www.google.com/search?q=hello".to_string(),
            "//www.google.com/search?q=hello".to_string(),
            "/search?q=hello".to_string(),
            "foo".to_string(),
        ];

        let (inlinks, outlinks) = super::normalize_links(url, &links);

        assert_eq!(inlinks, vec![
            "/search?q=hello".to_string(), 
            "/foo".to_string()]);
        assert_eq!(outlinks, vec![
            "http://www.google.com/search?q=hello".to_string(),
            "https://www.google.com/search?q=hello".to_string(),
            "z39.50://www.google.com/search?q=hello".to_string(),
            "web+soup://www.google.com/search?q=hello".to_string(),
            "http://www.google.com/search?q=hello".to_string(),
        ]);
    }

    #[test]
    fn test_normalize_basic_url() {
        let url = reqwest::Url::parse("http://www.google.com").unwrap();

        let links = vec![
            "/foo".to_string(),
            "bar".to_string(),
            "//www.wikipedia.org".to_string(),
        ];

        let (inlinks, outlinks) = super::normalize_links(url, &links);

        assert_eq!(inlinks, vec![
            "/foo".to_string(),
            "/bar".to_string(),
        ]);

        assert_eq!(outlinks, vec![
            "http://www.wikipedia.org".to_string(),
        ]);
    }

    #[test]
    fn test_normalize_nested_url() {
        let url = reqwest::Url::parse("https://www.google.com/a/a/a").unwrap();

        let links = vec![
            "/foo".to_string(),
            "bar".to_string(),
            "//www.wikipedia.org".to_string(),
        ];

        let (inlinks, outlinks) = super::normalize_links(url, &links);

        assert_eq!(inlinks, vec![
            "/foo".to_string(),
            "/a/a/bar".to_string(),
        ]);

        assert_eq!(outlinks, vec![
            "https://www.wikipedia.org".to_string(),
        ]);   
    }
}
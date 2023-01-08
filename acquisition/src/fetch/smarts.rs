use acquisition::protocol;

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
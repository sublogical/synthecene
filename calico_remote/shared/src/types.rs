use std::time::{SystemTime, UNIX_EPOCH};

pub fn systemtime_to_timestamp(time:SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("timestamp prior to epoch")
        .as_millis()
        .try_into()
        .expect("timestamp greater than expected")
}

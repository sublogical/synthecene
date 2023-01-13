use std::time::UNIX_EPOCH;

use chrono::{NaiveDateTime, DateTime, Utc};


pub fn datetime_to_timestamp(datetime: &DateTime<Utc>) -> u64 {

    let ts_secs:u64 = datetime.timestamp().try_into().unwrap();
    let ts_ms:u64 = datetime.timestamp_millis().try_into().unwrap();
    ts_secs * 1000 + ts_ms
}

pub fn systemtime_to_timestamp(time: &std::time::SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH)
        .expect("always after epoch")
        .as_millis()
        .try_into()
        .expect("u64 millis should last >2M years")
}

pub fn timestamp_to_systemtime(timestamp: u64) -> std::time::SystemTime {
    UNIX_EPOCH + std::time::Duration::from_millis(timestamp)
}

pub fn timestamp_to_datetime(timestamp: u64) -> DateTime<Utc> {
    // todo: move to a utility
    let ts_secs:i64 = (timestamp / 1000).try_into().unwrap();
    let ts_ns = (timestamp % 1000) * 1_000_000;
    
    let naive = NaiveDateTime::from_timestamp_opt(ts_secs, ts_ns as u32).expect("should have a valid timestamp");
    DateTime::<Utc>::from_utc(naive, Utc)
}

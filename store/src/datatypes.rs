use chrono::{NaiveDateTime, DateTime, Utc};


pub fn datetime_to_timestamp(datetime: &DateTime<Utc>) -> u64 {

    let ts_secs:u64 = datetime.timestamp().try_into().unwrap();
    let ts_ms:u64 = datetime.timestamp_millis().try_into().unwrap();
    ts_secs * 1000 + ts_ms
}

pub fn timestamp_to_datetime(timestamp: u64) -> DateTime<Utc> {
    // todo: move to a utility
    let ts_secs:i64 = (timestamp / 1000).try_into().unwrap();
    let ts_ns = (timestamp % 1000) * 1_000_000;
    
    DateTime::<Utc>::from_utc(NaiveDateTime::from_timestamp(ts_secs, ts_ns as u32), Utc)
}

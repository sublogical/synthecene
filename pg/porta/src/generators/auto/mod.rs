
use crate::porta_api::{ NumberDataPoint };
use crate::porta_api::number_data_point::Value;

pub(crate) fn sine_wave_metric(
    start:u64,
    interval:u64, // interval in nanoseconds
    steps:u64,
    freq:f64,
    phase:f64,
    amplitude:f64) -> Vec<NumberDataPoint> {
    
    (0..steps).map(|step| {
        let current = start + step * interval;
        let x = current as f64 / 1_000_000_000.0 * 2.0 * std::f64::consts::PI * freq - phase;
        let y = x.sin() * amplitude;

        NumberDataPoint {
            start_time_unix_nano: start,
            time_unix_nano: current,
            value: Some(Value::AsDouble(y))
        }        
    }).collect()
}


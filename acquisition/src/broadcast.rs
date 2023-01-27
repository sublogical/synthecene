
#[cfg(feature = "integration_tests")]
mod integration {
    use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};

    use hdrhistogram::Histogram;
    use rdkafka::{producer::{FutureProducer, FutureRecord}, ClientConfig, consumer::{StreamConsumer, Consumer}, Message};

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .try_into()
            .unwrap()
    }
    
    #[tokio::test]
    async fn test_kafka() {
        let brokers = "127.0.0.1:9094";
        let topic = "test";

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()
            .expect("Producer creation error");

            let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("session.timeout.ms", "6000")
            .set("enable.auto.commit", "false")
            .set("group.id", "rust-rdkafka-roundtrip-example")
            .create()
            .expect("Consumer creation failed");
        consumer.subscribe(&[&topic]).unwrap();
    
        tokio::spawn(async move {
            let mut i = 0_usize;
            loop {
                producer
                    .send_result(
                        FutureRecord::to(&topic)
                            .key(&i.to_string())
                            .payload("dummy")
                            .timestamp(now()),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
                i += 1;
            }
        });
    
        let start = Instant::now();
        let mut latencies = Histogram::<u64>::new(5).unwrap();
        println!("Warming up for 10s...");
        loop {
            let message = consumer.recv().await.unwrap();
            let then = message.timestamp().to_millis().unwrap();
            if start.elapsed() < Duration::from_secs(10) {
                // Warming up.
            } else if start.elapsed() < Duration::from_secs(20) {
                if latencies.len() == 0 {
                    println!("Recording for 10s...");
                }
                latencies += (now() - then) as u64;
            } else {
                break;
            }
        }
    
        println!("measurements: {}", latencies.len());
        println!("mean latency: {}ms", latencies.mean());
        println!("p50 latency:  {}ms", latencies.value_at_quantile(0.50));
        println!("p90 latency:  {}ms", latencies.value_at_quantile(0.90));
        println!("p99 latency:  {}ms", latencies.value_at_quantile(0.99));            

    }
}

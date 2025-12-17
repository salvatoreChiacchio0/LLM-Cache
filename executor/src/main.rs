use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use serde_json::Value;

use prometheus::{Counter, Histogram, Encoder, TextEncoder};
use warp::Filter;
use lazy_static::lazy_static;

use std::convert::TryInto;
use std::error::Error;
use std::fmt::{Display, Formatter}; 
type DynError = Box<dyn Error + Send + Sync>;

 #[derive(Debug)]
struct TerminatedError(String);

impl Display for TerminatedError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TerminatedError {}


lazy_static! {
    static ref EVICT_COUNTER: Counter =
        prometheus::register_counter!("executor_evict_total", "Total evictions")
            .unwrap();

    static ref PREFETCH_COUNTER: Counter =
        prometheus::register_counter!("executor_prefetch_total", "Total prefetches")
            .unwrap();

    static ref LATENCY_HIST: Histogram =
        prometheus::register_histogram!("executor_latency_seconds", "Plan execution latency")
            .unwrap();
    
     static ref TEST_COUNTER: Counter =
        prometheus::register_counter!("executor_test_total", "Test counter")
            .unwrap();
}
#[tokio::main]
async fn main() -> Result<(), DynError> {
     TEST_COUNTER.inc();

     tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "kafka:29092")
        .set("group.id", "policy-executor-group")
        .set("auto.offset.reset", "earliest")
        .create()?;

    let mut retries = 0;
    const MAX_RETRIES: u32 = 10;
    const RETRY_DELAY_SECS: u64 = 2;
    
    loop {
        match consumer.subscribe(&["aura-plan"]) {
            Ok(_) => {
                eprintln!("Successfully subscribed to topic 'aura-plan'");
                break;
            }
            Err(e) => {
                retries += 1;
                if retries >= MAX_RETRIES {
                    eprintln!("Failed to subscribe to topic 'aura-plan' after {} retries: {}", MAX_RETRIES, e);
                    return Err(DynError::from(e));
                }
                eprintln!("Failed to subscribe to topic 'aura-plan' (attempt {}/{}): {}. Retrying in {}s...", 
                    retries, MAX_RETRIES, e, RETRY_DELAY_SECS);
                tokio::time::sleep(std::time::Duration::from_secs(RETRY_DELAY_SECS)).await;
            }
        }
    }

    let redis_client = redis::Client::open("redis://redis-aura:6379")?;
    let _ = redis_client.get_multiplexed_async_connection().await?;

    let metrics_route = warp::path!("metrics")
        .map(|| {
            println!("Metrics endpoint called!");
            let encoder = TextEncoder::new();
            let metric_families = prometheus::gather();
            let mut buffer = Vec::new();
            
            println!("Collected {} metric families", metric_families.len());

            encoder.encode(&metric_families, &mut buffer).unwrap();
            warp::reply::with_header(buffer,"Content-Type",encoder.format_type())
        });

    let metrics_server_future = async move {
        warp::serve(metrics_route)
            .run(([0, 0, 0, 0], 9097))
            .await;

        Err(DynError::from(TerminatedError("Metrics server terminated unexpectedly".to_string()))) as Result<(), DynError>
    };

    let redis_client_for_consumer = redis_client.clone();

    let kafka_consumer_future = async move {
        let mut redis_conn =
            redis_client_for_consumer
                .get_multiplexed_async_connection()
                .await?;

        loop {
            let msg_result = consumer.recv().await;

            match msg_result {
                Ok(msg) => {
                    let timer = LATENCY_HIST.start_timer();

                    let payload = msg.payload().unwrap_or(b"");
                    let payload_str = std::str::from_utf8(payload).unwrap_or("");

                    let v: Value = match serde_json::from_str(payload_str) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("JSON parse error: {}", e);
                            continue;
                        }
                    };

                    let mut pipe = redis::pipe();

                    if let Some(evict) = v["evict"].as_array() {
                        for k in evict.iter().filter_map(|x| x.as_str()) {
                            pipe.del(k);
                            EVICT_COUNTER.inc();
                        }
                    }

                    if let Some(pref) = v["prefetch"].as_array() {
                        for p in pref {
                            if let (Some(k), Some(v)) =
                                (p["k"].as_str(), p["v"].as_str())
                            {
                                let ttl: usize = p["ttl"]
                                    .as_u64()
                                    .unwrap_or(180)
                                    .try_into()
                                    .unwrap_or(180);

                                pipe.set_ex(k, v, ttl);
                                PREFETCH_COUNTER.inc();
                            }
                        }
                    }

                    pipe.query_async::<_, ()>(&mut redis_conn).await?;

                    timer.stop_and_record();

                    if let Err(e) = consumer.commit_message(&msg, rdkafka::consumer::CommitMode::Async) {
                        return Err(DynError::from(e)) as Result<(), DynError>;
                    }
                }
                Err(e) => {
                    eprintln!("Kafka consumer failed: {}", e);
                    // FIX E0282: Cast esplicito del tipo di ritorno
                    return Err(DynError::from(e)) as Result<(), DynError>;
                }
            }
        }
    };

    tokio::select! {
        res = metrics_server_future => {
            eprintln!("Metrics server exited: {:?}", res);
            if let Err(e) = res {
                return Err(e);
            }
        },
        res = kafka_consumer_future => {
            eprintln!("Kafka consumer exited: {:?}", res);
            if let Err(e) = res {
                return Err(e);
            }
        }
    }

    Ok(())
}
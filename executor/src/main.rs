use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::ClientConfig;
use rdkafka::Message;
use serde_json::Value;

use prometheus::{Counter, Histogram, Gauge, Encoder, TextEncoder};
use warp::Filter;
use lazy_static::lazy_static;

use std::convert::TryInto;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

type DynError = Box<dyn Error + Send + Sync>;

 #[derive(Debug)]
struct TerminatedError(String);

impl Display for TerminatedError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TerminatedError {}

// Constants matching Python code
const TTL_MIN_SECONDS: i64 = 60;
const TTL_MAX_SECONDS: i64 = 3600;
const PRIORITY_TTL_MULT: f64 = 3.0;
const EVICT_TTL_MULT: f64 = 0.1;
const MAX_KEYS_TO_SCAN: usize = 500;

async fn get_item_category(
    item_id: i64,
    mongo_coll: &mongodb::Collection<mongodb::bson::Document>,
    cache: &Arc<RwLock<HashMap<i64, Option<String>>>>,
) -> Option<String> {
    // Check cache first
    {
        let cache_read = cache.read().await;
        if let Some(cached) = cache_read.get(&item_id) {
            return cached.clone();
        }
    }
    
    // Query MongoDB
    let filter = mongodb::bson::doc! { "item_id": item_id };
    let mut options = mongodb::options::FindOneOptions::default();
    options.projection = Some(mongodb::bson::doc! { "category": 1 });
    
    let result = mongo_coll
        .find_one(filter, options)
        .await;
    
    let category = match result {
        Ok(Some(doc)) => {
            doc.get_str("category")
                .ok()
                .map(|s| s.to_string())
        }
        Ok(None) => None,
        Err(e) => {
            eprintln!("[WARNING] MongoDB query failed for item_id={}: {}", item_id, e);
            None
        }
    };
    
    // Update cache
    {
        let mut cache_write = cache.write().await;
        if cache_write.len() >= MAX_KEYS_TO_SCAN {
            cache_write.clear();
        }
        cache_write.insert(item_id, category.clone());
    }
    
    category
}

async fn apply_policy_retroactively(
    redis_conn: &mut redis::aio::MultiplexedConnection,
    mongo_coll: &mongodb::Collection<mongodb::bson::Document>,
    category_cache: &Arc<RwLock<HashMap<i64, Option<String>>>>,
    global_ttl_factor: f64,
    priority_categories: &[String],
    evict_categories: &[String],
) -> Result<(), DynError> {
    let mut evicted_count = 0;
    let mut extended_count = 0;
    let mut updated_count = 0;
    let mut keys_scanned = 0;
    
    // Convert slices to HashSet for faster lookup
    let priority_set: HashSet<String> = priority_categories.iter().cloned().collect();
    let evict_set: HashSet<String> = evict_categories.iter().cloned().collect();
    
    // Use SCAN to iterate through Redis keys
    let mut cursor: u64 = 0;
    let mut batch_keys = Vec::new();
    
    loop {
        // SCAN command
        let result: (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("item:*")
            .arg("COUNT")
            .arg(100)
            .query_async(redis_conn)
            .await?;
        
        cursor = result.0;
        let keys = result.1;
        
        for key in keys {
            if keys_scanned >= MAX_KEYS_TO_SCAN {
                break;
            }
            batch_keys.push(key);
            keys_scanned += 1;
        }
        
        if cursor == 0 || keys_scanned >= MAX_KEYS_TO_SCAN {
            break;
        }
    }
    
    // Process keys in batches
    for key in batch_keys {
        // Get TTL
        let ttl: i64 = redis::cmd("TTL")
            .arg(&key)
            .query_async(redis_conn)
            .await
            .unwrap_or(-1);
        
        if ttl <= 0 {
            continue; // Key doesn't exist or has no TTL
        }
        
        // Extract item_id from key (format: "item:123")
        let item_id_str = key.strip_prefix("item:").unwrap_or("");
        let item_id: i64 = match item_id_str.parse() {
            Ok(id) => id,
            Err(_) => continue,
        };
        
        // Get category
        let category = get_item_category(item_id, mongo_coll, category_cache).await;
        
        // Apply policy
        if let Some(cat) = &category {
            if evict_set.contains(cat) {
                // Evict: delete the key
                let _: () = redis::cmd("DEL")
                    .arg(&key)
                    .query_async(redis_conn)
                    .await?;
                evicted_count += 1;
                RETROACTIVE_EVICTIONS.inc();
            } else if priority_set.contains(cat) {
                // Priority: extend TTL
                let new_ttl = (ttl as f64 * PRIORITY_TTL_MULT) as i64;
                let new_ttl = new_ttl.max(TTL_MIN_SECONDS).min(TTL_MAX_SECONDS);
                
                let _: () = redis::cmd("EXPIRE")
                    .arg(&key)
                    .arg(new_ttl)
                    .query_async(redis_conn)
                    .await?;
                extended_count += 1;
                RETROACTIVE_EXTENSIONS.inc();
            } else if global_ttl_factor != 1.0 {
                // Apply global_ttl_factor
                let new_ttl = (ttl as f64 * global_ttl_factor) as i64;
                let new_ttl = new_ttl.max(TTL_MIN_SECONDS).min(TTL_MAX_SECONDS);
                
                let _: () = redis::cmd("EXPIRE")
                    .arg(&key)
                    .arg(new_ttl)
                    .query_async(redis_conn)
                    .await?;
                updated_count += 1;
                RETROACTIVE_UPDATES.inc();
            }
        } else if global_ttl_factor != 1.0 {
            // No category, but apply global_ttl_factor anyway
            let new_ttl = (ttl as f64 * global_ttl_factor) as i64;
            let new_ttl = new_ttl.max(TTL_MIN_SECONDS).min(TTL_MAX_SECONDS);
            
            let _: () = redis::cmd("EXPIRE")
                .arg(&key)
                .arg(new_ttl)
                .query_async(redis_conn)
                .await?;
            updated_count += 1;
            RETROACTIVE_UPDATES.inc();
        }
    }
    
    eprintln!("[POLICY] Retroactive application: evicted={}, extended={}, updated={}, scanned={}", 
        evicted_count, extended_count, updated_count, keys_scanned);
    
    Ok(())
}


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

    static ref POLICY_VERSION: Gauge =
        prometheus::register_gauge!("executor_policy_version", "Version of last applied policy")
            .unwrap();
    
    static ref RETROACTIVE_EVICTIONS: Counter =
        prometheus::register_counter!("executor_retroactive_evictions_total", "Items evicted retroactively by policy")
            .unwrap();
    
    static ref RETROACTIVE_EXTENSIONS: Counter =
        prometheus::register_counter!("executor_retroactive_extensions_total", "TTLs extended retroactively by policy")
            .unwrap();
    
    static ref RETROACTIVE_UPDATES: Counter =
        prometheus::register_counter!("executor_retroactive_updates_total", "TTLs updated retroactively by policy")
            .unwrap();
    
    static ref POLICY_APPLICATIONS: Counter =
        prometheus::register_counter!("executor_policy_applications_total", "Total policy applications")
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
    
    // Connect to MongoDB
    let mongo_client = mongodb::Client::with_uri_str("mongodb://mongo:27017").await?;
    let mongo_db = mongo_client.database("aura");
    let mongo_coll = mongo_db.collection::<mongodb::bson::Document>("catalog");
    
    // Category cache: item_id -> category
    let category_cache: Arc<RwLock<HashMap<i64, Option<String>>>> = Arc::new(RwLock::new(HashMap::new()));

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
    let mongo_coll_for_consumer = mongo_coll.clone();
    let category_cache_for_consumer = category_cache.clone();

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
                    // Check for new policy format (global_ttl_factor, priority_categories, evict_categories)
                    let global_ttl_factor = v.get("global_ttl_factor")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(1.0);
                    let priority_categories: Vec<String> = v.get("priority_categories")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    let evict_categories: Vec<String> = v.get("evict_categories")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                                .collect()
                        })
                        .unwrap_or_default();
                    
                    let has_new_policy = global_ttl_factor != 1.0 
                        || !priority_categories.is_empty() 
                        || !evict_categories.is_empty();
                    
                    let mut pipe = redis::pipe();

                    // Handle old format (evict/prefetch arrays) for backward compatibility
                    if let Some(evict) = v["evict"].as_array() {
                        for k in evict.iter().filter_map(|x| x.as_str()).take(20) {
                            if k.starts_with("item:") {
                                pipe.del(k);
                                EVICT_COUNTER.inc();
                            }
                        }
                    }

                    if let Some(pref) = v["prefetch"].as_array() {
                        for p in pref.iter().take(20) {
                            if let (Some(k), Some(v)) =
                                (p["k"].as_str(), p["v"].as_str())
                            {
                                let ttl_raw = p["ttl"].as_u64().unwrap_or(180);
                                let ttl_clamped = if ttl_raw < 60 { 60 } else if ttl_raw > 3600 { 3600 } else { ttl_raw };
                                let ttl: usize = ttl_clamped.try_into().unwrap_or(180);

                                pipe.set_ex(k, v, ttl);
                                PREFETCH_COUNTER.inc();
                            }
                        }
                    }

                    pipe.query_async::<_, ()>(&mut redis_conn).await?;

                    // Apply new policy format retroactively
                    if has_new_policy {
                        POLICY_APPLICATIONS.inc();
                        POLICY_VERSION.inc();
                        
                        eprintln!("[POLICY] Applying policy: factor={:.2}, priority={:?}, evict={:?}", 
                            global_ttl_factor, priority_categories, evict_categories);
                        
                        apply_policy_retroactively(
                            &mut redis_conn,
                            &mongo_coll_for_consumer,
                            &category_cache_for_consumer,
                            global_ttl_factor,
                            &priority_categories,
                            &evict_categories,
                        ).await?;
                    }

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
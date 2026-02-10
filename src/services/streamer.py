import time
import signal
import json
import threading
import requests
from pathlib import Path
from ..core.config import (
    TTL_TIME_COMPRESSION,
    KAFKA_TOPIC_STATS, KAFKA_TOPIC_PLAN,
    FLUSH_ON_STARTUP,
    TINYLFU_SKETCH_WIDTH, TINYLFU_SKETCH_DEPTH, TINYLFU_DOORKEEPER_SIZE,
    TINYLFU_RESET_INTERVAL, TINYLFU_SAMPLE_SIZE,
    CacheAdaptationState,
    OLLAMA_HOST
)
from ..core.db import get_redis_aura, get_redis_lru, get_kafka_producer, get_kafka_consumer
from ..modules.generator import TrafficGenerator
from ..modules.preprocessed_loader import PreprocessedLoader
from ..modules.scenario_loader import ScenarioLoader
from ..modules.limited_dataset_loader import LimitedDatasetLoader
import os
from ..modules.aggregator import ContextAggregator
from ..modules.metrics import (
    start_metrics_server, CACHE_HITS, CACHE_MISSES, 
    TINYLFU_HITS, TINYLFU_MISSES,
    REDIS_MEMORY_USAGE
)
from ..modules.tinylfu import TinyLFU

PLAN_EVERY_N_EVENTS = 5000
PLAN_EVERY_N_EVENTS_VOLATILE = 2000
PLAN_EVERY_N_EVENTS_ATTACK_MODE = 1500
VOLATILITY_THRESHOLD_FOR_FAST_SNAPSHOTS = 0.3
VOLATILITY_THRESHOLD_FOR_ATTACK_MODE = 0.35

tinylfu_aura_instance = None
tinylfu_baseline_instance = None
tinylfu_lock = threading.Lock()

def check_ollama_active():
    try:
        version_url = f"http://{OLLAMA_HOST}:11434/api/version"
        resp = requests.get(version_url, timeout=3)
        resp.raise_for_status()
        print(f"Ollama is active: {resp.json().get('version', 'unknown version')}")
    except Exception as e:
        print(f"WARNING: Ollama not reachable at http://{OLLAMA_HOST}:11434 ({e})")

def policy_listener():
    print("Policy Listener thread started")
    consumer = None
    retry_count = 0
    max_retries = 30
    
    while consumer is None and retry_count < max_retries and not shutdown_requested.is_set():
        try:
            consumer = get_kafka_consumer("aura_streamer_policy_group", [KAFKA_TOPIC_PLAN])
            msg = consumer.poll(0.5)
            if msg and msg.error() and "UNKNOWN_TOPIC_OR_PART" in str(msg.error()):
                print(f"Topic {KAFKA_TOPIC_PLAN} not ready yet, retrying... ({retry_count}/{max_retries})")
                consumer.close()
                consumer = None
                retry_count += 1
                time.sleep(2)
                continue
            print(f"Successfully connected to Kafka topic {KAFKA_TOPIC_PLAN}")
            break
        except Exception as e:
            print(f"Failed to connect to Kafka, retrying... ({retry_count}/{max_retries}): {e}")
            if consumer:
                try:
                    consumer.close()
                except:
                    pass
            consumer = None
            retry_count += 1
            time.sleep(2)
    
    if consumer is None:
        print(f"ERROR: Failed to connect to Kafka after {max_retries} retries. Policy updates will not work.")
        return
    
    while not shutdown_requested.is_set():
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            error_str = str(msg.error())
            if "UNKNOWN_TOPIC_OR_PART" in error_str:
                print(f"Topic {KAFKA_TOPIC_PLAN} not available yet, waiting...")
                time.sleep(5)
                continue
            print(f"Policy Consumer Error: {msg.error()}")
            continue
            
        try:
            plan = json.loads(msg.value().decode('utf-8'))
            plan_type = plan.get("type", "unknown")

            if plan_type == "tinylfu_parameter_update":
                tinylfu_control = plan.get("tinylfu_control", {})
                if not tinylfu_control:
                    print(f"WARNING: Received tinylfu_parameter_update without tinylfu_control")
                    continue

                adaptation_state_str = plan.get("adaptation_state", "NORMAL")
                try:
                    adaptation_state = CacheAdaptationState(adaptation_state_str)
                except ValueError:
                    print(f"WARNING: Unknown adaptation_state '{adaptation_state_str}', defaulting to NORMAL")
                    adaptation_state = CacheAdaptationState.NORMAL
                
                print(f"Received TinyLFU parameter update (adaptation_state={adaptation_state.value})")
                
                with tinylfu_lock:
                    global tinylfu_aura_instance
                    if not tinylfu_aura_instance:
                        print(f"ERROR: TinyLFU instance not available")
                        continue
                    
                    current_stats = tinylfu_aura_instance.get_stats()
                    current_reset_interval = current_stats.get("reset_interval", TINYLFU_RESET_INTERVAL)
                    
                    tinylfu_control = plan.get("tinylfu_control", {})

                    decay_factor = tinylfu_control.get("decay_factor")
                    reset_interval = tinylfu_control.get("reset_interval")
                    reset_sketch = tinylfu_control.get("reset_sketch")
                    admission_bias = tinylfu_control.get("admission_bias")
                    
                    if reset_sketch is True:
                        try:
                            tinylfu_aura_instance.force_reset_sketch()
                            print(f"Applied force_reset_sketch: Count-Min Sketch and Doorkeeper completely reset")
                        except Exception as e:
                            print(f"ERROR: Failed to force reset sketch: {e}")
                    
                    if decay_factor is not None:
                        try:
                            tinylfu_aura_instance.apply_decay(decay_factor)
                            print(f"Applied decay_factor={decay_factor}")
                        except Exception as e:
                            print(f"ERROR: Failed to apply decay_factor {decay_factor}: {e}")
                    
                    if reset_interval is not None:
                        try:
                            tinylfu_aura_instance.set_reset_interval(reset_interval)
                            print(f"Applied reset_interval={reset_interval}")
                        except Exception as e:
                            print(f"ERROR: Failed to set reset_interval {reset_interval}: {e}")
                    
                    if admission_bias is not None:
                        try:
                            tinylfu_aura_instance.set_admission_bias(admission_bias)
                            print(f"Applied admission_bias={admission_bias}")
                        except Exception as e:
                            print(f"ERROR: Failed to set admission_bias {admission_bias}: {e}")

                    final_stats = tinylfu_aura_instance.get_stats()
                    final_reset_interval = final_stats.get("reset_interval", current_reset_interval)
                    final_decay_factor = decay_factor if decay_factor is not None else current_stats.get("decay_factor", None)
                
                print(f"TinyLFU parameter update applied successfully")
            
            else:
                print(f"WARNING: Unknown plan type '{plan_type}' - ignoring")
                
        except Exception as e:
            print(f"Failed to parse policy: {e}")
            import traceback
            traceback.print_exc()
    consumer.close()

shutdown_requested = threading.Event()

def signal_handler(signum, frame):
    print(f"\nReceived signal {signum}, initiating graceful shutdown...")
    shutdown_requested.set()

def get_lru_cache_full_percent(r_lru, limit_bytes):
    try:
        mem = r_lru.info("memory")["used_memory"]
        return round((mem / limit_bytes) * 100, 1)
    except Exception:
        return 0.0

def normalize_path(path_str: str) -> str:
    """
    Normalizza un percorso convertendo percorsi Docker in percorsi locali se necessario.
    
    Args:
        path_str: Percorso da normalizzare (può essere Docker /app/data/... o locale)
    
    Returns:
        Percorso normalizzato che funziona nell'ambiente corrente
    """
    if not path_str:
        return path_str
    
    if path_str.startswith("/app/data/"):
        local_path = path_str.replace("/app/data/", "data/")
        local_path_obj = Path(local_path)
        
        if local_path_obj.exists():
            return str(local_path_obj.resolve())
        
        project_root = Path(__file__).parent.parent.parent
        project_path = project_root / local_path
        if project_path.exists():
            return str(project_path.resolve())
    
    path_obj = Path(path_str)
    if path_obj.exists():
        return str(path_obj.resolve())
    
    if path_str.startswith("/app/"):
        local_equivalent = path_str.replace("/app/", "")
        local_path_obj = Path(local_equivalent)
        if local_path_obj.exists():
            return str(local_path_obj.resolve())
    
    return path_str

def run_streamer():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    check_ollama_active()

    print("Initializing resources...")
    r = get_redis_aura()
    r_tinylfu = get_redis_lru()
    kafka_producer = get_kafka_producer(client_id="aura-streamer")

    print("Initializing TinyLFU baseline (standard policy, fixed parameters)...")
    tinylfu = TinyLFU(
        redis_client=r_tinylfu,
        sketch_width=TINYLFU_SKETCH_WIDTH,
        sketch_depth=TINYLFU_SKETCH_DEPTH,
        doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
        reset_interval=TINYLFU_RESET_INTERVAL,
        sample_size=TINYLFU_SAMPLE_SIZE
    )

    print("Initializing TinyLFU for LLM-driven cache (same policy, LLM-adaptive parameters)...")
    tinylfu_aura = TinyLFU(
        redis_client=r,
        sketch_width=TINYLFU_SKETCH_WIDTH,
        sketch_depth=TINYLFU_SKETCH_DEPTH,
        doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
        reset_interval=TINYLFU_RESET_INTERVAL,
        sample_size=TINYLFU_SAMPLE_SIZE
    )
    
    enable_debug = os.getenv("TINYLFU_DEBUG", "false").lower() == "true"
    if enable_debug:
        tinylfu_aura.enable_debug_logging(True)
        print("TinyLFU debug logging ENABLED for AURA instance")

    global tinylfu_aura_instance, tinylfu_baseline_instance
    with tinylfu_lock:
        tinylfu_aura_instance = tinylfu_aura
        tinylfu_baseline_instance = tinylfu

    test_dataset = os.getenv("TEST_DATASET", None)
    max_events = int(os.getenv("MAX_EVENTS", "0"))
    
    test_config_file = Path("data/current_test.json")
    if test_config_file.exists() and not test_dataset:
        try:
            with open(test_config_file, 'r') as f:
                test_config = json.load(f)
                test_dataset = test_config.get("dataset_path")
                if test_dataset:
                    test_dataset = normalize_path(test_dataset)
                max_events = test_config.get("max_events", max_events)
                print(f"Loaded test config: {test_config.get('test_name', 'unknown')}")
        except Exception as e:
            print(f"Failed to load test config: {e}")
    
    if test_dataset:
        print(f"Using test dataset: {test_dataset}")
        if '/test_data/' in test_dataset or '\\test_data\\' in test_dataset or test_dataset.endswith('_100k.json'):
            print(f"Detected pre-processed test file, using PreprocessedLoader")
            generator = PreprocessedLoader(test_dataset)
        elif test_dataset.endswith('.json'):
            print(f"Detected scenario JSON, using ScenarioLoader")
            generator = ScenarioLoader(test_dataset, max_events=max_events if max_events > 0 else None)
        elif test_dataset.endswith('.csv'):
            generator = PreprocessedLoader(test_dataset)
        else:
            generator = LimitedDatasetLoader(log_file=test_dataset, max_events=max_events if max_events > 0 else 100000)
    else:
        test_data_dir = Path("data/test_data")
        default_test_files = [
            "normal_dataset_100k.json",
            "02_burst_cooldown_100k.json",
            "04_daily_pattern_100k.json",
            "01_hot_cold_shift_100k.json"
        ]
        
        default_test = None
        for test_file in default_test_files:
            test_path = test_data_dir / test_file
            if test_path.exists():
                default_test = str(test_path)
                print(f"No test config found, using default test file: {test_file}")
                break
        
        if default_test:
            generator = PreprocessedLoader(default_test)
        else:
            print(f"ERROR: No test configuration or test files found.")
            print(f"  Available options:")
            print(f"  1. Create data/current_test.json with test configuration")
            print(f"  2. Set TEST_DATASET environment variable pointing to a test file")
            print(f"  3. Place a test file in data/test_data/ (e.g., normal_dataset_100k.json)")
            print(f"")
            print(f"  Test files should be in data/test_data/ directory.")
            print(f"  The log_15M_subset.txt file is optional and only used as last resort.")
            
            log_file = Path(LOG_FILE)
            if log_file.exists():
                print(f"  Falling back to log file: {LOG_FILE}")
                generator = TrafficGenerator()
            else:
                raise FileNotFoundError(
                    f"Cannot start streamer: no test files found in data/test_data/ and log file not available at {LOG_FILE}.\n"
                    f"Please use test files from data/test_data/ instead."
                )
    
    aggregator = ContextAggregator(max_events=4500, session_history_len=10, window_seconds=300)

    start_metrics_server(8000)

    t_policy = threading.Thread(target=policy_listener, daemon=True)
    t_policy.start()

    if FLUSH_ON_STARTUP:
        try:
            r.flushdb()
            r_tinylfu.flushdb()
            print("Flushed Redis DBs")
        except Exception as e:
            print(f"WARNING: Failed to flush Redis: {e}")
    else:
        print("Skipping Redis flush (FLUSH_ON_STARTUP=False)")

    count = 0
    events_since_last_stat = 0

    baseline_hits_window = 0
    baseline_misses_window = 0
    aura_hits_window = 0
    aura_misses_window = 0

    baseline_hits_total = 0
    baseline_misses_total = 0
    aura_hits_total = 0
    aura_misses_total = 0

    previous_baseline_hr = None
    previous_aura_hr = None

    print("Starting event loop...")
    print("Getting first event from generator...")
    event_gen = generator.generate_events(shutdown_requested)
    print("Generator created, fetching first event...")
    try:
        first_event = next(event_gen)
        print(f"First event received: item_id={first_event[0]}, user={first_event[1]}, action={first_event[2]}")
    except StopIteration:
        print("ERROR: Generator returned no events!")
        return
    except Exception as e:
        print(f"ERROR: Failed to get first event: {e}")
        return

    current_first_event = first_event
    current_event_gen = event_gen
    
    print("Entering main event loop...")
    events_processed = 0
    last_config_check = time.time()
    config_check_interval = 5.0
    current_test_name = None
    if test_config_file.exists():
        try:
            with open(test_config_file, 'r') as f:
                test_config = json.load(f)
                current_test_name = test_config.get("test_name", None)
        except:
            pass
    
    should_exit = False
    while not should_exit and not shutdown_requested.is_set():
        def event_generator_with_first():
            yield current_first_event
            yield from current_event_gen
        
        try:
            for item_id, user, action, timestamp in event_generator_with_first():
                should_check_config = (max_events == 0) or (events_processed >= max_events)
                
                if should_check_config and time.time() - last_config_check > config_check_interval:
                    last_config_check = time.time()
                    if test_config_file.exists():
                        try:
                            with open(test_config_file, 'r') as f:
                                new_config = json.load(f)
                                new_test_name = new_config.get("test_name")
                                new_dataset = new_config.get("dataset_path")
                                new_max_events = new_config.get("max_events", 0)
                                
                                if new_test_name != current_test_name and new_dataset:
                                    if max_events > 0 and events_processed < max_events * 0.95:
                                        print(f"WARNING: New test detected ({new_test_name}) but current test not complete ({events_processed}/{max_events}, {events_processed/max_events*100:.1f}%). Ignoring new test until current test completes.")
                                        break
                                    
                                    print(f"Test config changed: {current_test_name} -> {new_test_name}")
                                    print(f"Reloading generator for new test: {new_test_name}")
                                    print(f"Previous test stats: count={count}, events_processed={events_processed}, max_events={max_events}")
                                    
                                    events_processed = 0
                                    count = 0
                                    print(f"Reset counters for new test. New max_events: {new_max_events}")
                                    events_since_last_stat = 0
                                    baseline_hits_window = 0
                                    baseline_misses_window = 0
                                    aura_hits_window = 0
                                    aura_misses_window = 0
                                    baseline_hits_total = 0
                                    baseline_misses_total = 0
                                    aura_hits_total = 0
                                    aura_misses_total = 0
                                    previous_baseline_hr = None
                                    previous_aura_hr = None
                                    
                                    aggregator.reset_window()
                                    
                                    
                                    
                                    if '/test_data/' in new_dataset or '\\test_data\\' in new_dataset or new_dataset.endswith('_100k.json'):
                                        generator = PreprocessedLoader(new_dataset)
                                    elif new_dataset.endswith('.json'):
                                        generator = ScenarioLoader(new_dataset, max_events=new_max_events if new_max_events > 0 else None)
                                    elif new_dataset.endswith('.csv'):
                                        generator = PreprocessedLoader(new_dataset)
                                    else:
                                        generator = LimitedDatasetLoader(log_file=new_dataset, max_events=new_max_events if new_max_events > 0 else 100000)
                                    
                                    test_dataset = new_dataset
                                    max_events = new_max_events
                                    current_test_name = new_test_name
                                    
                                    new_event_gen = generator.generate_events(shutdown_requested)
                                    try:
                                        new_first_event = next(new_event_gen)
                                        print(f"New generator ready, first event: item_id={new_first_event[0]}")
                                    except StopIteration:
                                        print("ERROR: New generator returned no events!")
                                        should_exit = True
                                        break
                                    except Exception as e:
                                        print(f"ERROR: Failed to get first event from new generator: {e}")
                                        should_exit = True
                                        break
                                    
                                    current_first_event = new_first_event
                                    current_event_gen = new_event_gen
                                    
                                    break
                                elif new_max_events != max_events:
                                    max_events = new_max_events
                                    print(f"Updated max_events to {max_events}")
                        except Exception as e:
                            pass
                
                if max_events > 0:
                    events_processed += 1
                    if events_processed >= max_events:
                        print(f"Reached max_events limit ({max_events}/{max_events})")
                        print(f"Final count: {count}, events_processed: {events_processed}")
                        
                        
                        try:
                            metrics_file = Path("data/streamer_final_metrics.json")
                            metrics_file.parent.mkdir(parents=True, exist_ok=True)
                            final_metrics_data = {
                                "timestamp": time.time(),
                                "events_processed": events_processed,
                                "baseline_hits": baseline_hits_total,
                                "baseline_misses": baseline_misses_total,
                                "aura_hits": aura_hits_total,
                                "aura_misses": aura_misses_total,
                                "baseline_hit_rate": baseline_hits_total / (baseline_hits_total + baseline_misses_total) if (baseline_hits_total + baseline_misses_total) > 0 else 0.0,
                                "aura_hit_rate": aura_hits_total / (aura_hits_total + aura_misses_total) if (aura_hits_total + aura_misses_total) > 0 else 0.0,
                                "test_name": current_test_name,
                                "max_events": max_events
                            }
                            with open(metrics_file, 'w') as f:
                                json.dump(final_metrics_data, f, indent=2)
                            print(f"Final metrics saved to {metrics_file}")
                        except Exception as e:
                            print(f"WARNING: Failed to save final metrics: {e}")
                        
                        print("Waiting 5 seconds to allow metrics collection...")
                        time.sleep(5)
                        
                        print(f"Sending final snapshot with test_complete flag for test: {current_test_name}")
                        try:
                            ts_float = time.time()
                            snapshot = aggregator.build_snapshot(ts_float)
                            snapshot["test_complete"] = True
                            snapshot["test_name"] = current_test_name
                            snapshot["final_events"] = events_processed
                            
                            baseline_total = baseline_hits_window + baseline_misses_window
                            aura_total = aura_hits_window + aura_misses_window
                            baseline_hit_ratio = baseline_hits_window / baseline_total if baseline_total > 0 else 0.0
                            aura_hit_ratio = aura_hits_window / aura_total if aura_total > 0 else 0.0
                            improvement_over_baseline = aura_hit_ratio - baseline_hit_ratio
                            
                            snapshot["baseline_comparison"] = {
                                "baseline_hit_ratio": baseline_hit_ratio,
                                "aura_hit_ratio": aura_hit_ratio,
                                "improvement": improvement_over_baseline,
                                "baseline_hits": baseline_hits_window,
                                "baseline_misses": baseline_misses_window,
                                "aura_hits": aura_hits_window,
                                "aura_misses": aura_misses_window
                            }
                            
                            msg = json.dumps(snapshot).encode('utf-8')
                            kafka_producer.produce(KAFKA_TOPIC_STATS, msg)
                            kafka_producer.flush(0)
                            print(f"Final snapshot sent with test_complete=True for test: {current_test_name}")
                        except Exception as e:
                            print(f"ERROR: Failed to send final snapshot: {e}")
                        
                        new_test_detected = False
                        if test_config_file.exists():
                            try:
                                with open(test_config_file, 'r') as f:
                                    new_config = json.load(f)
                                    new_test_name = new_config.get("test_name")
                                    new_dataset = new_config.get("dataset_path")
                                    new_max_events = new_config.get("max_events", 0)
                                    if new_test_name != current_test_name and new_dataset:
                                        print(f"New test detected after completing current test: {new_test_name}")
                                        print(f"Switching to new test...")
                                        new_test_detected = True
                                        
                                        print(f"Reloading generator for new test: {new_test_name}")
                                        
                                        events_processed = 0
                                        count = 0
                                        print(f"Reset counters for new test. New max_events: {new_max_events}")
                                        events_since_last_stat = 0
                                        baseline_hits_window = 0
                                        baseline_misses_window = 0
                                        aura_hits_window = 0
                                        aura_misses_window = 0
                                        baseline_hits_total = 0
                                        baseline_misses_total = 0
                                        aura_hits_total = 0
                                        aura_misses_total = 0
                                        previous_baseline_hr = None
                                        previous_aura_hr = None
                                        
                                        aggregator.reset_window()
                                        
                                        if '/test_data/' in new_dataset or '\\test_data\\' in new_dataset or new_dataset.endswith('_100k.json') or new_dataset.endswith('_5k.json'):
                                            generator = PreprocessedLoader(new_dataset)
                                        elif new_dataset.endswith('.json'):
                                            generator = ScenarioLoader(new_dataset, max_events=new_max_events if new_max_events > 0 else None)
                                        elif new_dataset.endswith('.csv'):
                                            generator = LimitedDatasetLoader(filepath=Path(new_dataset), max_events=new_max_events if new_max_events > 0 else 100000)
                                        else:
                                            print(f"WARNING: Unknown dataset type for {new_dataset}, defaulting to TrafficGenerator.")
                                            generator = TrafficGenerator()
                                        
                                        try:
                                            current_event_gen = generator.generate_events()
                                            current_first_event = next(current_event_gen)
                                            current_test_name = new_test_name
                                            max_events = new_max_events
                                            print(f"New generator ready, first event: item_id={current_first_event[0]}")
                                        except StopIteration:
                                            print(f"ERROR: New generator is empty!")
                                            should_exit = True
                                            break
                                        except Exception as e:
                                            print(f"ERROR: Failed to get first event from new generator: {e}")
                                            should_exit = True
                                            break
                                        
                                        last_config_check = time.time()
                                        
                                        break
                            except Exception as e:
                                print(f"WARNING: Failed to check for new test: {e}")
                        
                        if not new_test_detected:
                            print("No new test detected yet, waiting 2 seconds and checking again...")
                            time.sleep(2)
                            
                            if test_config_file.exists():
                                try:
                                    with open(test_config_file, 'r') as f:
                                        retry_config = json.load(f)
                                        retry_test_name = retry_config.get("test_name")
                                        retry_dataset = retry_config.get("dataset_path")
                                        if retry_test_name != current_test_name and retry_dataset:
                                            print(f"New test detected on retry: {retry_test_name}")
                                            new_test_detected = True
                                            last_config_check = 0
                                            break
                                except:
                                    pass
                            
                            if not new_test_detected:
                                print("No new test detected after retry, terminating...")
                                should_exit = True
                                break
                    elif events_processed % 1000 == 0:
                        print(f"Progress: {events_processed}/{max_events} events processed ({events_processed/max_events*100:.1f}%)")
                        
                        try:
                            metrics_file = Path("data/streamer_final_metrics.json")
                            metrics_file.parent.mkdir(parents=True, exist_ok=True)
                            periodic_metrics_data = {
                                "timestamp": time.time(),
                                "events_processed": events_processed,
                                "baseline_hits": baseline_hits_total,
                                "baseline_misses": baseline_misses_total,
                                "aura_hits": aura_hits_total,
                                "aura_misses": aura_misses_total,
                                "baseline_hit_rate": baseline_hits_total / (baseline_hits_total + baseline_misses_total) if (baseline_hits_total + baseline_misses_total) > 0 else 0.0,
                                "aura_hit_rate": aura_hits_total / (aura_hits_total + aura_misses_total) if (aura_hits_total + aura_misses_total) > 0 else 0.0,
                                "test_name": current_test_name,
                                "max_events": max_events
                            }
                            with open(metrics_file, 'w') as f:
                                json.dump(periodic_metrics_data, f, indent=2)
                        except Exception as e:
                            pass
                
                item_key = f"item:{item_id}"

                try:
                    tinylfu_hit = tinylfu.exists(item_key)
                    if tinylfu_hit:
                        TINYLFU_HITS.inc()
                        baseline_hits_window += 1
                        baseline_hits_total += 1
                    else:
                        TINYLFU_MISSES.inc()
                        baseline_misses_window += 1
                        baseline_misses_total += 1
                        effective_ttl = 30
                        if TTL_TIME_COMPRESSION > 1.0:
                            effective_ttl = max(1, int(30 / TTL_TIME_COMPRESSION))
                        tinylfu.set(item_key, "1", ttl=effective_ttl)
                except Exception as e:
                    TINYLFU_MISSES.inc()
                    baseline_misses_window += 1
                    print(f"WARNING: TinyLFU baseline error: {e}")

                aura_hit = False
                try:
                    aura_hit = tinylfu_aura.exists(item_key)
                    if aura_hit:
                        CACHE_HITS.inc()
                        aura_hits_window += 1
                        aura_hits_total += 1
                    else:
                        CACHE_MISSES.inc()
                        aura_misses_window += 1
                        aura_misses_total += 1
                except Exception as e:
                    CACHE_MISSES.inc()
                    aura_misses_window += 1
                    print(f"WARNING: TinyLFU Aura error: {e}")

                if count == 0:
                    print(f"Recording first event in aggregator...")
                aggregator.record_event(user, item_id, action, aura_hit, timestamp, baseline_hit=tinylfu_hit)
                if count == 0:
                    print(f"First event recorded, continuing loop...")
                
                if not aura_hit:
                    effective_ttl = 30
                    if TTL_TIME_COMPRESSION > 1.0:
                        effective_ttl = max(1, int(30 / TTL_TIME_COMPRESSION))
                    
                    try:
                        tinylfu_aura.set(item_key, "1", ttl=effective_ttl)
                    except Exception as e:
                        print(f"ERROR: Failed to set key {item_key} in TinyLFU cache: {e}")

                events_since_last_stat += 1
                count += 1

                current_snapshot_threshold = PLAN_EVERY_N_EVENTS

                if count % 200 == 0:
                    print(f"Progress: events={count}, since_last_plan={events_since_last_stat}/{current_snapshot_threshold}")
                
                if events_since_last_stat >= current_snapshot_threshold:
                    print(f"Sending stats snapshot to Brain (Count: {count}, events_since_last_stat={events_since_last_stat})")
                    print(f"Brain will call LLM and save temporal metrics (check aura-brain logs)")
                    try:
                        ts_float = float(timestamp)
                    except:
                        ts_float = time.time()
                    snapshot = aggregator.build_snapshot(ts_float)
                    snapshot["test_name"] = current_test_name

                    workload_volatility = snapshot.get("workload_volatility", 0.0)
                    if workload_volatility > VOLATILITY_THRESHOLD_FOR_ATTACK_MODE:
                        current_snapshot_threshold = PLAN_EVERY_N_EVENTS_ATTACK_MODE
                        print(f"ATTACK MODE: High volatility ({workload_volatility:.3f} > {VOLATILITY_THRESHOLD_FOR_ATTACK_MODE}), using ultra-fast snapshot frequency: {PLAN_EVERY_N_EVENTS_ATTACK_MODE} events")
                    elif workload_volatility > VOLATILITY_THRESHOLD_FOR_FAST_SNAPSHOTS:
                        current_snapshot_threshold = PLAN_EVERY_N_EVENTS_VOLATILE
                        print(f"High volatility detected ({workload_volatility:.3f}), will use faster snapshot frequency: {PLAN_EVERY_N_EVENTS_VOLATILE} events for next window")
                    else:
                        current_snapshot_threshold = PLAN_EVERY_N_EVENTS

                    baseline_total = baseline_hits_window + baseline_misses_window
                    aura_total = aura_hits_window + aura_misses_window
                    baseline_hit_ratio = baseline_hits_window / baseline_total if baseline_total > 0 else 0.0
                    aura_hit_ratio = aura_hits_window / aura_total if aura_total > 0 else 0.0
                    improvement_over_baseline = aura_hit_ratio - baseline_hit_ratio

                    try:
                        baseline_stats = tinylfu.get_stats()
                        aura_stats = tinylfu_aura.get_stats()
                        baseline_decay = baseline_stats.get("decay_factor_applied", None)
                        aura_decay = aura_stats.get("decay_factor_applied", None)
                        baseline_reset = baseline_stats.get("reset_interval", TINYLFU_RESET_INTERVAL)
                        aura_reset = aura_stats.get("reset_interval", TINYLFU_RESET_INTERVAL)
                        baseline_tracked = baseline_stats.get("tracked_items", 0)
                        aura_tracked = aura_stats.get("tracked_items", 0)
                        print(f"DEBUG - Baseline TinyLFU: decay={baseline_decay}, reset={baseline_reset}, tracked={baseline_tracked}, sketch_additions={baseline_stats.get('sketch_total_additions', 0)}")
                        print(f"DEBUG - Aura TinyLFU: decay={aura_decay}, reset={aura_reset}, tracked={aura_tracked}, sketch_additions={aura_stats.get('sketch_total_additions', 0)}")
                        if baseline_decay == aura_decay and baseline_reset == aura_reset:
                            print(f"WARNING: Baseline and Aura have IDENTICAL parameters! This explains why results are identical.")
                            print(f"NOTE: Aura should have different parameters from LLM updates. Check if policy_listener is receiving and applying plans.")
                        else:
                            print(f"Parameters differ: Baseline(decay={baseline_decay}, reset={baseline_reset}) vs Aura(decay={aura_decay}, reset={aura_reset})")
                            if baseline_tracked == aura_tracked and baseline_hits_window == aura_hits_window:
                                print(f"WARNING: Despite different parameters, both caches have identical hits/misses. This may indicate:")
                                print(f"    - Cache is too small to show differences (only {baseline_tracked} items tracked)")
                                print(f"    - Items are so different that both caches admit different sets with same hit rate")
                                print(f"    - Need higher cache usage (85-90%) to see parameter effects")
                    except Exception as e:
                        print(f"WARNING: Failed to get TinyLFU stats for comparison: {e}")

                    delta_baseline_hr = 0.0
                    delta_aura_hr = 0.0
                    if previous_baseline_hr is not None:
                        delta_baseline_hr = baseline_hit_ratio - previous_baseline_hr
                    if previous_aura_hr is not None:
                        delta_aura_hr = aura_hit_ratio - previous_aura_hr

                    previous_baseline_hr = baseline_hit_ratio
                    previous_aura_hr = aura_hit_ratio

                    snapshot["baseline_comparison"] = {
                        "baseline_hit_ratio": baseline_hit_ratio,
                        "aura_hit_ratio": aura_hit_ratio,
                        "improvement_over_baseline": improvement_over_baseline,
                        "delta_baseline_hit_ratio": delta_baseline_hr,
                        "delta_aura_hit_ratio": delta_aura_hr,
                        "baseline_hits": baseline_hits_window,
                        "baseline_misses": baseline_misses_window,
                        "aura_hits": aura_hits_window,
                        "aura_misses": aura_misses_window
                    }


                    baseline_hits_window = 0
                    baseline_misses_window = 0
                    aura_hits_window = 0
                    aura_misses_window = 0

                    try:
                        tlfu_stats = tinylfu_aura.get_stats()
                        eviction_count = tlfu_stats.get("eviction_count", 0)
                        ghost_hits = tlfu_stats.get("ghost_hits", 0)
                        efficiency = 1.0
                        if eviction_count > 0:
                            efficiency = 1.0 - (ghost_hits / eviction_count)
                        
                        snapshot["tinylfu_stats"] = {
                            "efficiency": efficiency,
                            "eviction_count": eviction_count,
                            "ghost_hits": ghost_hits,
                            "stats": tlfu_stats
                        }

                        snapshot = aggregator.enrich_snapshot_with_tinylfu_stats(snapshot, tlfu_stats)

                        print(f"TinyLFU Stats: Evictions={eviction_count}, Efficiency={efficiency:.2f}, Regret={snapshot.get('eviction_regret', 0.0):.3f}")
                        print(f"Baseline Comparison: Baseline={baseline_hit_ratio:.3f}, Aura={aura_hit_ratio:.3f}, Improvement={improvement_over_baseline:+.3f}")
                    except Exception as e:
                        print(f"WARNING: Failed to get TinyLFU stats: {e}")

                    try:
                        redis_info = r.info("memory")
                        used_memory = redis_info.get("used_memory", 0)
                        REDIS_MEMORY_USAGE.set(used_memory)
                    except Exception as e:
                        print(f"WARNING: Failed to update Redis memory metric: {e}")

                    try:
                        msg = json.dumps(snapshot).encode('utf-8')
                        kafka_producer.produce(KAFKA_TOPIC_STATS, msg)
                        kafka_producer.flush(0)
                    except Exception as e:
                        print(f"ERROR: Failed to send stats to Kafka: {e}")

                    events_since_last_stat = 0

                if count % 1000 == 0:
                    baseline_total_events = baseline_hits_total + baseline_misses_total
                    aura_total_events = aura_hits_total + aura_misses_total
                    baseline_hr_total = baseline_hits_total / baseline_total_events if baseline_total_events > 0 else 0.0
                    aura_hr_total = aura_hits_total / aura_total_events if aura_total_events > 0 else 0.0
                    improvement_total = aura_hr_total - baseline_hr_total
                    print(f"Processed {count} events... | Aggregate: Baseline HR={baseline_hr_total:.3f}, Aura HR={aura_hr_total:.3f}, Improvement={improvement_total:+.3f}")
                    
                    with tinylfu_lock:
                        if tinylfu_aura_instance:
                            aura_stats = tinylfu_aura_instance.get_stats()
                            admit_calls = aura_stats.get("should_admit_calls", 0)
                            admit_cache_full = aura_stats.get("should_admit_cache_full_calls", 0)
                            admit_rejections = aura_stats.get("should_admit_rejections", 0)
                            admission_bias = aura_stats.get("admission_bias", 0)
                            
                            if admit_cache_full > 0:
                                rejection_rate = (admit_rejections / admit_cache_full) * 100 if admit_cache_full > 0 else 0.0
                                print(f"  Admission Stats: calls={admit_calls}, cache_full_calls={admit_cache_full}, "
                                      f"rejections={admit_rejections} ({rejection_rate:.1f}%), bias={admission_bias}")
                            elif count > 0:
                                print(f"  Admission Stats: should_admit NEVER called with cache full! (total calls={admit_calls}, bias={admission_bias})")
        except StopIteration:
            break
        except Exception as e:
            print(f"ERROR in event loop: {e}")
            import traceback
            traceback.print_exc()
            break
        
        if should_exit:
            break
        
    
    print("\n" + "="*80)
    print("FINAL AGGREGATE STATISTICS")
    print("="*80)
    baseline_total_events = baseline_hits_total + baseline_misses_total
    aura_total_events = aura_hits_total + aura_misses_total
    
    if baseline_total_events > 0 and aura_total_events > 0:
        baseline_hr_total = baseline_hits_total / baseline_total_events
        aura_hr_total = aura_hits_total / aura_total_events
        improvement_total = aura_hr_total - baseline_hr_total
        improvement_percent = (improvement_total / baseline_hr_total * 100) if baseline_hr_total > 0 else 0.0
        
        print(f"Total Events Processed: {count}")
        print(f"\nBaseline (TinyLFU):")
        print(f"  Hits: {baseline_hits_total:,}")
        print(f"  Misses: {baseline_misses_total:,}")
        print(f"  Hit Ratio: {baseline_hr_total:.4f} ({baseline_hr_total*100:.2f}%)")
        print(f"\nAura (TinyLFU + LLM):")
        print(f"  Hits: {aura_hits_total:,}")
        print(f"  Misses: {aura_misses_total:,}")
        print(f"  Hit Ratio: {aura_hr_total:.4f} ({aura_hr_total*100:.2f}%)")
        print(f"\nImprovement:")
        print(f"  Absolute: {improvement_total:+.4f} ({improvement_percent:+.2f}%)")
        print(f"  Relative: {(improvement_total / baseline_hr_total * 100) if baseline_hr_total > 0 else 0.0:+.2f}%")
    else:
        print(f"Total Events Processed: {count}")
        print("Insufficient data for comparison")
    print("="*80 + "\n")

    if count > 0 or events_processed > 0:
        try:
            metrics_file = Path("data/streamer_final_metrics.json")
            metrics_file.parent.mkdir(parents=True, exist_ok=True)
            
            final_events_processed = events_processed if events_processed > 0 else count
            print(f"Saving final metrics: events_processed={events_processed}, count={count}, final={final_events_processed}, max_events={max_events}")
            
            final_metrics_data = {
                "timestamp": time.time(),
                "events_processed": final_events_processed,
                "baseline_hits": baseline_hits_total,
                "baseline_misses": baseline_misses_total,
                "aura_hits": aura_hits_total,
                "aura_misses": aura_misses_total,
                "baseline_hit_rate": baseline_hits_total / (baseline_hits_total + baseline_misses_total) if (baseline_hits_total + baseline_misses_total) > 0 else 0.0,
                "aura_hit_rate": aura_hits_total / (aura_hits_total + aura_misses_total) if (aura_hits_total + aura_misses_total) > 0 else 0.0,
                "test_name": current_test_name,
                "max_events": max_events
            }
            
            with open(metrics_file, 'w') as f:
                json.dump(final_metrics_data, f, indent=2)
            print(f"Final metrics saved to {metrics_file}")
        except Exception as e:
            print(f"WARNING: Failed to save final metrics: {e}")
    
    print("Shutdown complete.")

if __name__ == "__main__":
    run_streamer()

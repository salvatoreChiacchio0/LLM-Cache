import time
import signal
import json
import threading
import requests
from ..core.config import (
    TTL_TIME_COMPRESSION,
    KAFKA_TOPIC_STATS, KAFKA_TOPIC_PLAN,
    FLUSH_ON_STARTUP,
    TINYLFU_SKETCH_WIDTH, TINYLFU_SKETCH_DEPTH, TINYLFU_DOORKEEPER_SIZE,
    TINYLFU_RESET_INTERVAL, TINYLFU_SAMPLE_SIZE,
    CacheAdaptationState,
    OLLAMA_HOST, USE_GROQ
)
from ..core.db import get_redis_aura, get_redis_lru, get_kafka_producer, get_kafka_consumer
from ..modules.generator import TrafficGenerator
from ..modules.aggregator import ContextAggregator
from ..utils.clusters import get_item_cluster
from ..modules.data import get_item_category_cached
from ..modules.metrics import (
    start_metrics_server, CACHE_HITS, CACHE_MISSES, 
    TINYLFU_HITS, TINYLFU_MISSES,
    SAFETY_GUARD_REJECTED_PLANS, SAFETY_GUARD_ROLLBACKS, SAFETY_GUARD_ROLLBACK_REASON
)
from ..modules.safety_guard import OptimizationSafetyGuard
from ..modules.tinylfu import TinyLFU

PLAN_EVERY_N_EVENTS = 5000
PLAN_EVERY_N_EVENTS_VOLATILE = 2000
PLAN_EVERY_N_EVENTS_ATTACK_MODE = 1500
VOLATILITY_THRESHOLD_FOR_FAST_SNAPSHOTS = 0.3
VOLATILITY_THRESHOLD_FOR_ATTACK_MODE = 0.35

tinylfu_aura_instance = None
tinylfu_baseline_instance = None
tinylfu_lock = threading.Lock()

safety_guard = OptimizationSafetyGuard()

def check_ollama_active():
    if USE_GROQ:
        return
    try:
        version_url = f"http://{OLLAMA_HOST}:11434/api/version"
        resp = requests.get(version_url, timeout=3)
        resp.raise_for_status()
        print(f"[STREAMER] Ollama is active: {resp.json().get('version', 'unknown version')}")
    except Exception as e:
        print(f"[STREAMER] WARNING: Ollama not reachable at http://{OLLAMA_HOST}:11434 ({e})")

def policy_listener():
    print("[STREAMER] Policy Listener thread started")
    consumer = None
    retry_count = 0
    max_retries = 30
    
    while consumer is None and retry_count < max_retries and not shutdown_requested.is_set():
        try:
            consumer = get_kafka_consumer("aura_streamer_policy_group", [KAFKA_TOPIC_PLAN])
            msg = consumer.poll(0.5)
            if msg and msg.error() and "UNKNOWN_TOPIC_OR_PART" in str(msg.error()):
                print(f"[STREAMER] Topic {KAFKA_TOPIC_PLAN} not ready yet, retrying... ({retry_count}/{max_retries})")
                consumer.close()
                consumer = None
                retry_count += 1
                time.sleep(2)
                continue
            print(f"[STREAMER] Successfully connected to Kafka topic {KAFKA_TOPIC_PLAN}")
            break
        except Exception as e:
            print(f"[STREAMER] Failed to connect to Kafka, retrying... ({retry_count}/{max_retries}): {e}")
            if consumer:
                try:
                    consumer.close()
                except:
                    pass
            consumer = None
            retry_count += 1
            time.sleep(2)
    
    if consumer is None:
        print(f"[STREAMER] ERROR: Failed to connect to Kafka after {max_retries} retries. Policy updates will not work.")
        return
    
    while not shutdown_requested.is_set():
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            error_str = str(msg.error())
            if "UNKNOWN_TOPIC_OR_PART" in error_str:
                print(f"[STREAMER] Topic {KAFKA_TOPIC_PLAN} not available yet, waiting...")
                time.sleep(5)
                continue
            print(f"[STREAMER] Policy Consumer Error: {msg.error()}")
            continue
            
        try:
            plan = json.loads(msg.value().decode('utf-8'))
            plan_type = plan.get("type", "unknown")

            if plan_type == "tinylfu_parameter_update":
                tinylfu_control = plan.get("tinylfu_control", {})
                if not tinylfu_control:
                    print(f"[STREAMER] WARNING: Received tinylfu_parameter_update without tinylfu_control")
                    continue

                adaptation_state_str = plan.get("adaptation_state", "NORMAL")
                try:
                    adaptation_state = CacheAdaptationState(adaptation_state_str)
                except ValueError:
                    print(f"[STREAMER] WARNING: Unknown adaptation_state '{adaptation_state_str}', defaulting to NORMAL")
                    adaptation_state = CacheAdaptationState.NORMAL
                
                print(f"[STREAMER] Received TinyLFU parameter update (adaptation_state={adaptation_state.value})")
                
                with tinylfu_lock:
                    global tinylfu_aura_instance
                    if not tinylfu_aura_instance:
                        print(f"[STREAMER] ERROR: TinyLFU instance not available")
                        continue
                    
                    current_stats = tinylfu_aura_instance.get_stats()
                    current_reset_interval = current_stats.get("reset_interval", TINYLFU_RESET_INTERVAL)
                    current_config = safety_guard.get_current_config()
                    current_decay_factor = current_config.get("decay_factor")

                    should_rollback, rollback_config = safety_guard.check_rollback_condition()

                    if should_rollback and rollback_config is not None:
                        print(f"[STREAMER] ⚠️ SAFETY GUARD: Rollback triggered - hit ratio decreased >0.01 for 2 consecutive snapshots")
                        print(f"[STREAMER] Rolling back to last stable configuration: {rollback_config}")

                        SAFETY_GUARD_ROLLBACKS.inc()
                        SAFETY_GUARD_ROLLBACK_REASON.set(1)

                        rollback_decay = rollback_config.get("decay_factor")
                        rollback_reset = rollback_config.get("reset_interval")
                        rollback_bias = rollback_config.get("doorkeeper_bias")

                        if rollback_decay is not None:
                            try:
                                tinylfu_aura_instance.apply_decay(rollback_decay)
                                print(f"[STREAMER] Rollback: Applied decay_factor={rollback_decay}")
                            except Exception as e:
                                print(f"[STREAMER] ERROR: Failed to apply rollback decay_factor: {e}")

                        if rollback_reset is not None:
                            try:
                                tinylfu_aura_instance.set_reset_interval(rollback_reset)
                                print(f"[STREAMER] Rollback: Applied reset_interval={rollback_reset}")
                            except Exception as e:
                                print(f"[STREAMER] ERROR: Failed to apply rollback reset_interval: {e}")

                        if rollback_bias is not None:
                            tinylfu_aura_instance.clear_doorkeeper_bias()
                            for category, bias_value in rollback_bias.items():
                                try:
                                    tinylfu_aura_instance.set_doorkeeper_bias(category, bias_value)
                                    print(f"[STREAMER] Rollback: Applied doorkeeper_bias for {category}={bias_value}")
                                except Exception as e:
                                    print(f"[STREAMER] ERROR: Failed to apply rollback doorkeeper_bias: {e}")
                        elif rollback_bias is None and "doorkeeper_bias" in rollback_config:
                            tinylfu_aura_instance.clear_doorkeeper_bias()
                            print(f"[STREAMER] Rollback: Cleared all doorkeeper_bias")

                        safety_guard.update_applied_config(rollback_decay, rollback_reset, rollback_bias)

                        print(f"[STREAMER] Plan rejected - rollback applied instead")
                        continue

                    is_valid, validated_plan_or_rollback, reason, was_clamped = safety_guard.validate_plan(
                        plan, current_decay_factor, current_reset_interval, adaptation_state=adaptation_state
                    )

                    if not is_valid:
                        print(f"[STREAMER] ⚠️ SAFETY GUARD: Plan rejected - {reason}")
                        SAFETY_GUARD_REJECTED_PLANS.inc()
                        continue

                    if was_clamped:
                        print(f"[STREAMER] ⚠️ SAFETY GUARD: Plan clamped - {reason}")

                    plan = validated_plan_or_rollback
                    tinylfu_control = plan.get("tinylfu_control", {})

                    param_count = sum(1 for p in [
                        tinylfu_control.get("decay_factor"),
                        tinylfu_control.get("reset_interval"),
                        tinylfu_control.get("doorkeeper_bias")
                    ] if p is not None)

                    if param_count > 0:
                        safety_guard.save_stable_config(
                            current_decay_factor,
                            current_reset_interval,
                            current_config.get("doorkeeper_bias"),
                            plan.get("timestamp")
                        )

                    decay_factor = tinylfu_control.get("decay_factor")
                    reset_interval = tinylfu_control.get("reset_interval")
                    doorkeeper_bias = tinylfu_control.get("doorkeeper_bias")

                    if adaptation_state == CacheAdaptationState.NORMAL:
                        applied_decay = decay_factor
                        applied_reset = reset_interval
                        applied_bias = doorkeeper_bias
                        print(f"[STREAMER] State=NORMAL: Applying all parameters as-is")

                    elif adaptation_state == CacheAdaptationState.UNSTABLE:
                        applied_bias = None

                        if decay_factor is not None:
                            applied_decay = max(0.95, min(0.99, decay_factor))
                            if applied_decay != decay_factor:
                                print(f"[STREAMER] State=UNSTABLE: Clamped decay_factor from {decay_factor} to {applied_decay}")
                        else:
                            applied_decay = None

                        if reset_interval is not None:
                            max_delta_percent = 0.20
                            min_reset = int(current_reset_interval * (1 - max_delta_percent))
                            max_reset = int(current_reset_interval * (1 + max_delta_percent))
                            min_reset = max(50000, min_reset)
                            max_reset = min(500000, max_reset)
                            applied_reset = max(min_reset, min(max_reset, reset_interval))
                            if applied_reset != reset_interval:
                                print(f"[STREAMER] State=UNSTABLE: Clamped reset_interval from {reset_interval} to {applied_reset} (±20% of current {current_reset_interval})")
                        else:
                            applied_reset = None
                        
                        if doorkeeper_bias is not None:
                            print(f"[STREAMER] State=UNSTABLE: Ignoring doorkeeper_bias updates")

                    elif adaptation_state == CacheAdaptationState.RECOVERY:
                        param_count = sum(1 for p in [decay_factor, reset_interval, doorkeeper_bias] if p is not None)

                        if param_count > 1:
                            print(f"[STREAMER] State=RECOVERY: {param_count} parameters requested, but only 1 allowed. Selecting first non-null.")
                            if decay_factor is not None:
                                applied_decay = decay_factor
                                applied_reset = None
                                applied_bias = None
                                print(f"[STREAMER] State=RECOVERY: Applied decay_factor only")
                            elif reset_interval is not None:
                                applied_decay = None
                                applied_reset = reset_interval
                                applied_bias = None
                                print(f"[STREAMER] State=RECOVERY: Applied reset_interval only")
                            elif doorkeeper_bias is not None:
                                applied_decay = None
                                applied_reset = None
                                applied_bias = doorkeeper_bias
                                print(f"[STREAMER] State=RECOVERY: Applied doorkeeper_bias only")
                            else:
                                applied_decay = None
                                applied_reset = None
                                applied_bias = None
                        else:
                            applied_decay = decay_factor
                            applied_reset = reset_interval
                            applied_bias = doorkeeper_bias
                            if param_count == 1:
                                print(f"[STREAMER] State=RECOVERY: Applied single parameter change")
                    
                    elif adaptation_state == CacheAdaptationState.STABLE_BUT_INEFFECTIVE:
                        applied_decay = decay_factor
                        applied_reset = reset_interval
                        applied_bias = doorkeeper_bias

                        if decay_factor is not None:
                            applied_decay = max(0.85, min(1.0, decay_factor))
                            if applied_decay != decay_factor:
                                print(f"[STREAMER] State=STABLE_BUT_INEFFECTIVE: Clamped decay_factor from {decay_factor} to {applied_decay}")
                            else:
                                print(f"[STREAMER] State=STABLE_BUT_INEFFECTIVE: Allowing aggressive decay_factor={applied_decay} to unclog sketch")

                        if reset_interval is not None:
                            applied_reset = max(50000, min(500000, reset_interval))
                            if applied_reset != reset_interval:
                                print(f"[STREAMER] State=STABLE_BUT_INEFFECTIVE: Clamped reset_interval from {reset_interval} to {applied_reset}")
                            else:
                                print(f"[STREAMER] State=STABLE_BUT_INEFFECTIVE: Allowing reset_interval={applied_reset} for more frequent sketch resets")

                        param_count = sum(1 for p in [applied_decay, applied_reset, applied_bias] if p is not None)
                        if param_count > 0:
                            print(f"[STREAMER] State=STABLE_BUT_INEFFECTIVE: Applying {param_count} parameter change(s) to force sketch adaptation")
                    
                    else:
                        print(f"[STREAMER] WARNING: Unknown adaptation state, falling back to NORMAL")
                        applied_decay = decay_factor
                        applied_reset = reset_interval
                        applied_bias = doorkeeper_bias

                    if applied_decay is not None:
                        try:
                            tinylfu_aura_instance.apply_decay(applied_decay)
                            print(f"[STREAMER] Applied decay_factor={applied_decay}")
                        except Exception as e:
                            print(f"[STREAMER] ERROR: Failed to apply decay_factor {applied_decay}: {e}")
                    
                    if applied_reset is not None:
                        try:
                            tinylfu_aura_instance.set_reset_interval(applied_reset)
                            print(f"[STREAMER] Applied reset_interval={applied_reset}")
                        except Exception as e:
                            print(f"[STREAMER] ERROR: Failed to set reset_interval {applied_reset}: {e}")
                    
                    if applied_bias is not None:
                        if isinstance(applied_bias, dict):
                            for category, bias_value in applied_bias.items():
                                try:
                                    tinylfu_aura_instance.set_doorkeeper_bias(category, bias_value)
                                    print(f"[STREAMER] Applied doorkeeper_bias for {category}={bias_value}")
                                except Exception as e:
                                    print(f"[STREAMER] ERROR: Failed to set doorkeeper_bias for {category}: {e}")
                        else:
                            print(f"[STREAMER] WARNING: Invalid doorkeeper_bias format: {applied_bias}")
                    elif applied_bias is None and "doorkeeper_bias" in tinylfu_control and doorkeeper_bias is None:
                        if adaptation_state != CacheAdaptationState.UNSTABLE:
                            tinylfu_aura_instance.clear_doorkeeper_bias()
                            print(f"[STREAMER] Cleared all doorkeeper_bias")

                    final_stats = tinylfu_aura_instance.get_stats()
                    final_reset_interval = final_stats.get("reset_interval", current_reset_interval)
                    final_decay_factor = applied_decay if applied_decay is not None else current_decay_factor

                    final_bias = applied_bias if applied_bias is not None else (
                        current_config.get("doorkeeper_bias") if "doorkeeper_bias" in current_config else None
                    )

                    safety_guard.update_applied_config(
                        final_decay_factor,
                        final_reset_interval,
                        final_bias
                    )
                
                print(f"[STREAMER] TinyLFU parameter update applied successfully (state={adaptation_state.value})")
            
            elif plan_type in ["cluster_policy", "semantic_plan"]:
                print(f"[STREAMER] WARNING: Received legacy plan type '{plan_type}' - ignoring (LLM should only output tinylfu_parameter_update)")
            else:
                print(f"[STREAMER] WARNING: Unknown plan type '{plan_type}' - ignoring")
                
        except Exception as e:
            print(f"[STREAMER] Failed to parse policy: {e}")
            import traceback
            traceback.print_exc()
    consumer.close()

shutdown_requested = threading.Event()

def signal_handler(signum, frame):
    print(f"\n[INFO] Received signal {signum}, initiating graceful shutdown...")
    shutdown_requested.set()

def get_lru_cache_full_percent(r_lru, limit_bytes):
    try:
        mem = r_lru.info("memory")["used_memory"]
        return round((mem / limit_bytes) * 100, 1)
    except Exception:
        return 0.0

def run_streamer():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    check_ollama_active()

    print("[STREAMER] Initializing resources...")
    r = get_redis_aura()
    r_tinylfu = get_redis_lru()
    kafka_producer = get_kafka_producer(client_id="aura-streamer")

    print("[STREAMER] Initializing TinyLFU baseline (standard policy, fixed parameters)...")
    tinylfu = TinyLFU(
        redis_client=r_tinylfu,
        sketch_width=TINYLFU_SKETCH_WIDTH,
        sketch_depth=TINYLFU_SKETCH_DEPTH,
        doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
        reset_interval=TINYLFU_RESET_INTERVAL,
        sample_size=TINYLFU_SAMPLE_SIZE
    )

    print("[STREAMER] Initializing TinyLFU for LLM-driven cache (same policy, LLM-adaptive parameters)...")
    tinylfu_aura = TinyLFU(
        redis_client=r,
        sketch_width=TINYLFU_SKETCH_WIDTH,
        sketch_depth=TINYLFU_SKETCH_DEPTH,
        doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
        reset_interval=TINYLFU_RESET_INTERVAL,
        sample_size=TINYLFU_SAMPLE_SIZE
    )

    global tinylfu_aura_instance, tinylfu_baseline_instance
    with tinylfu_lock:
        tinylfu_aura_instance = tinylfu_aura
        tinylfu_baseline_instance = tinylfu

    generator = TrafficGenerator()
    aggregator = ContextAggregator(max_events=4500, session_history_len=10, window_seconds=300)

    start_metrics_server(8000)

    t_policy = threading.Thread(target=policy_listener, daemon=True)
    t_policy.start()

    if FLUSH_ON_STARTUP:
        try:
            r.flushdb()
            r_tinylfu.flushdb()
            print("[STREAMER] Flushed Redis DBs")
        except Exception as e:
            print(f"[WARNING] Failed to flush Redis: {e}")
    else:
        print("[STREAMER] Skipping Redis flush (FLUSH_ON_STARTUP=False)")

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

    print("[STREAMER] Starting event loop...")
    print("[STREAMER] Getting first event from generator...")
    event_gen = generator.generate_events(shutdown_requested)
    print("[STREAMER] Generator created, fetching first event...")
    try:
        first_event = next(event_gen)
        print(f"[STREAMER] First event received: item_id={first_event[0]}, user={first_event[1]}, action={first_event[2]}")
    except StopIteration:
        print("[STREAMER] ERROR: Generator returned no events!")
        return
    except Exception as e:
        print(f"[STREAMER] ERROR: Failed to get first event: {e}")
        return

    def event_generator_with_first():
        yield first_event
        yield from event_gen

    print("[STREAMER] Entering main event loop...")
    for item_id, user, action, timestamp in event_generator_with_first():
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
            print(f"[WARNING] TinyLFU baseline error: {e}")

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
             print(f"[WARNING] TinyLFU Aura error: {e}")

        category = None
        try:
            category = get_item_category_cached(item_id)
        except Exception as e:
            if count <= 5:
                print(f"[WARNING] Failed to get category for item_id={item_id}: {e}")
        
        try:
            if count == 0:
                print(f"[STREAMER] Getting cluster for first event item_id={item_id}...")
            cid = get_item_cluster(item_id)
            if cid is None:
                cid = f"cluster_{hash(item_id) % 1000}"
                if count == 0:
                    print(f"[STREAMER] Cluster was None, using hash fallback: {cid}")
        except Exception as e:
            cid = f"cluster_{hash(item_id) % 1000}"
            if count <= 5:
                print(f"[WARNING] get_item_cluster failed for item_id={item_id}, using hash fallback: {e}")

        if count == 0:
            print(f"[STREAMER] Recording first event in aggregator...")
        aggregator.record_event(user, item_id, action, category, aura_hit, timestamp, cid, baseline_hit=tinylfu_hit)
        if count == 0:
            print(f"[STREAMER] First event recorded, continuing loop...")
        
        if not aura_hit:
            effective_ttl = 30
            if TTL_TIME_COMPRESSION > 1.0:
                effective_ttl = max(1, int(30 / TTL_TIME_COMPRESSION))
            
            try:
                tinylfu_aura.set(item_key, "1", ttl=effective_ttl, category=category)
            except Exception as e:
                print(f"[ERROR] Failed to set key {item_key} in TinyLFU cache: {e}")

        events_since_last_stat += 1
        count += 1

        current_snapshot_threshold = PLAN_EVERY_N_EVENTS

        if count % 200 == 0:
            print(f"[STREAMER] Progress: events={count}, since_last_plan={events_since_last_stat}/{current_snapshot_threshold}")
        
        if events_since_last_stat >= current_snapshot_threshold:
            print(f"[STREAMER] Sending stats snapshot to Brain (Count: {count})")
            try:
                ts_float = float(timestamp)
            except:
                ts_float = time.time()
            snapshot = aggregator.build_snapshot(ts_float)

            workload_volatility = snapshot.get("workload_volatility", 0.0)
            if workload_volatility > VOLATILITY_THRESHOLD_FOR_ATTACK_MODE:
                current_snapshot_threshold = PLAN_EVERY_N_EVENTS_ATTACK_MODE
                print(f"[STREAMER] ⚡ ATTACK MODE: High volatility ({workload_volatility:.3f} > {VOLATILITY_THRESHOLD_FOR_ATTACK_MODE}), using ultra-fast snapshot frequency: {PLAN_EVERY_N_EVENTS_ATTACK_MODE} events")
            elif workload_volatility > VOLATILITY_THRESHOLD_FOR_FAST_SNAPSHOTS:
                current_snapshot_threshold = PLAN_EVERY_N_EVENTS_VOLATILE
                print(f"[STREAMER] High volatility detected ({workload_volatility:.3f}), will use faster snapshot frequency: {PLAN_EVERY_N_EVENTS_VOLATILE} events for next window")
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
                print(f"[STREAMER] DEBUG - Baseline TinyLFU: decay={baseline_decay}, reset={baseline_reset}, tracked={baseline_tracked}, sketch_additions={baseline_stats.get('sketch_total_additions', 0)}")
                print(f"[STREAMER] DEBUG - Aura TinyLFU: decay={aura_decay}, reset={aura_reset}, tracked={aura_tracked}, sketch_additions={aura_stats.get('sketch_total_additions', 0)}")
                if baseline_decay == aura_decay and baseline_reset == aura_reset:
                    print(f"[STREAMER] ⚠️ WARNING: Baseline and Aura have IDENTICAL parameters! This explains why results are identical.")
                    print(f"[STREAMER] ⚠️ NOTE: Aura should have different parameters from LLM updates. Check if policy_listener is receiving and applying plans.")
                else:
                    print(f"[STREAMER] ✓ Parameters differ: Baseline(decay={baseline_decay}, reset={baseline_reset}) vs Aura(decay={aura_decay}, reset={aura_reset})")
                    if baseline_tracked == aura_tracked and baseline_hits_window == aura_hits_window:
                        print(f"[STREAMER] ⚠️ WARNING: Despite different parameters, both caches have identical hits/misses. This may indicate:")
                        print(f"    - Cache is too small to show differences (only {baseline_tracked} items tracked)")
                        print(f"    - Items are so different that both caches admit different sets with same hit rate")
                        print(f"    - Need higher cache usage (85-90%) to see parameter effects")
            except Exception as e:
                print(f"[STREAMER] WARNING: Failed to get TinyLFU stats for comparison: {e}")

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

            safety_guard.update_hit_ratio(aura_hit_ratio, ts_float)

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

                print(f"[STREAMER] TinyLFU Stats: Evictions={eviction_count}, Efficiency={efficiency:.2f}, Regret={snapshot.get('eviction_regret', 0.0):.3f}")
                print(f"[STREAMER] Baseline Comparison: Baseline={baseline_hit_ratio:.3f}, Aura={aura_hit_ratio:.3f}, Improvement={improvement_over_baseline:+.3f}")
            except Exception as e:
                print(f"[WARNING] Failed to get TinyLFU stats: {e}")

            try:
                msg = json.dumps(snapshot).encode('utf-8')
                kafka_producer.produce(KAFKA_TOPIC_STATS, msg)
                kafka_producer.flush(0)
            except Exception as e:
                print(f"[ERROR] Failed to send stats to Kafka: {e}")

            events_since_last_stat = 0

        if count % 1000 == 0:
            baseline_total_events = baseline_hits_total + baseline_misses_total
            aura_total_events = aura_hits_total + aura_misses_total
            baseline_hr_total = baseline_hits_total / baseline_total_events if baseline_total_events > 0 else 0.0
            aura_hr_total = aura_hits_total / aura_total_events if aura_total_events > 0 else 0.0
            improvement_total = aura_hr_total - baseline_hr_total
            print(f"[STREAMER] Processed {count} events... | Aggregate: Baseline HR={baseline_hr_total:.3f}, Aura HR={aura_hr_total:.3f}, Improvement={improvement_total:+.3f}")

    print("\n" + "="*80)
    print("[STREAMER] FINAL AGGREGATE STATISTICS")
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

    print("[STREAMER] Shutdown complete.")

if __name__ == "__main__":
    run_streamer()

import json
import time
import requests
import threading
import signal
from pathlib import Path
from ..core.config import (
    KAFKA_TOPIC_STATS, KAFKA_TOPIC_PLAN,
    USE_GROQ,
    LLM_API_URL, LLM_MODEL, OLLAMA_HOST,
    OLLAMA_TIMEOUT_SEC, OLLAMA_NUM_PREDICT, OLLAMA_NUM_THREAD, OLLAMA_NUM_CTX,
    AURA_CACHE_LIMIT_BYTES,
    CacheAdaptationState, VOLATILITY_HIGH, VOLATILITY_LOW,
    LOW_VOLATILITY_THRESHOLD, OPPORTUNITY_LOSS_THRESHOLD,
    REQUIRED_CONSECUTIVE_SNAPSHOTS, MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE
)
from ..core.db import get_kafka_consumer, get_kafka_producer, get_redis_aura
from ..modules.prompts import build_global_prompt, build_global_prompt_small, _is_small_model
from ..modules.metrics import LLM_CALLS, LATENCY_MS, LLM_ERROR_RATE
from queue import Queue, Empty

shutdown_requested = threading.Event()
last_generated_policy = {}
policy_lock = threading.Lock()
parameter_history = []
MAX_PARAMETER_HISTORY = 5

current_adaptation_state = CacheAdaptationState.NORMAL
volatility_history = []
MAX_VOLATILITY_HISTORY = 3
previous_state = CacheAdaptationState.NORMAL

opportunity_loss_history = []
MAX_OPPORTUNITY_LOSS_HISTORY = REQUIRED_CONSECUTIVE_SNAPSHOTS + 1
state_entry_snapshot_count = {}

snapshot_history = []
MAX_SNAPSHOT_HISTORY = 7

best_strategy = None
best_strategy_lock = threading.Lock()

reset_sketch_call_count = 0
llm_call_count = 0
RESET_SKETCH_COOLDOWN = 5

test_complete_time = None
test_complete_lock = threading.Lock()
last_test_name = None

def check_ollama_active():
    try:
        version_url = f"http://{OLLAMA_HOST}:11434/api/version"
        resp = requests.get(version_url, timeout=3)
        resp.raise_for_status()
        print(f"Ollama is active: {resp.json().get('version', 'unknown version')}")
    except Exception as e:
        print(f"WARNING: Ollama not reachable at http://{OLLAMA_HOST}:11434 ({e})")

def reset_brain_state():
    """Reset all brain state when a test completes or a new test starts."""
    global current_adaptation_state, volatility_history, opportunity_loss_history
    global state_entry_snapshot_count, snapshot_history, best_strategy
    global parameter_history, last_generated_policy, reset_sketch_call_count, llm_call_count
    global previous_state
    
    with best_strategy_lock:
        best_strategy = None
    
    with policy_lock:
        current_adaptation_state = CacheAdaptationState.NORMAL
        previous_state = CacheAdaptationState.NORMAL
        volatility_history = []
        opportunity_loss_history = []
        state_entry_snapshot_count = {}
        snapshot_history = []
        parameter_history = []
        last_generated_policy = {}
        reset_sketch_call_count = 0
        llm_call_count = 0
    
    print("Brain state reset: cleared all adaptation history, strategies, and parameters.")

def signal_handler(signum, frame):
    print(f"\nReceived signal {signum}, initiating graceful shutdown...")
    shutdown_requested.set()

def compute_adaptation_state(workload_volatility, current_state, volatility_history, 
                              window_aura_hr=None, window_baseline_hr=None,
                              opportunity_loss_history=None, state_entry_snapshot_count=None,
                              delta_baseline_hr=None, delta_aura_hr=None):
    updated_history = volatility_history.copy()
    updated_history.append(workload_volatility)
    if len(updated_history) > MAX_VOLATILITY_HISTORY:
        updated_history.pop(0)

    updated_opp_loss_history = (opportunity_loss_history.copy() if opportunity_loss_history else [])
    updated_state_entry_count = (state_entry_snapshot_count.copy() if state_entry_snapshot_count else {})

    opportunity_loss = 0.0
    if delta_baseline_hr is not None and delta_aura_hr is not None:
        opportunity_loss = float(delta_baseline_hr) - float(delta_aura_hr)
        opportunity_loss = round(opportunity_loss, 6)
    elif window_baseline_hr is not None and window_aura_hr is not None:
        opportunity_loss = float(window_baseline_hr) - float(window_aura_hr)
        opportunity_loss = round(opportunity_loss, 6)

    updated_opp_loss_history.append(opportunity_loss)
    if len(updated_opp_loss_history) > MAX_OPPORTUNITY_LOSS_HISTORY:
        updated_opp_loss_history.pop(0)

    state_key = current_state.value
    if state_key not in updated_state_entry_count:
        updated_state_entry_count[state_key] = 0
    updated_state_entry_count[state_key] += 1

    new_state = current_state

    if workload_volatility > VOLATILITY_HIGH:
        if current_state != CacheAdaptationState.UNSTABLE:
            print(f"State transition: {current_state} → UNSTABLE (volatility={workload_volatility:.3f} > {VOLATILITY_HIGH})")
            updated_state_entry_count[CacheAdaptationState.UNSTABLE.value] = 0
        new_state = CacheAdaptationState.UNSTABLE
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.UNSTABLE:
        print(f"State transition: UNSTABLE → RECOVERY (volatility decreased to {workload_volatility:.3f})")
        updated_state_entry_count[CacheAdaptationState.RECOVERY.value] = 0
        new_state = CacheAdaptationState.RECOVERY
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.RECOVERY:
        if len(updated_history) >= 2:
            last_two_volatilities = updated_history[-2:]
            if all(v < VOLATILITY_LOW for v in last_two_volatilities):
                print(f"State transition: RECOVERY → NORMAL (2 consecutive low volatility: {last_two_volatilities})")
                updated_state_entry_count[CacheAdaptationState.NORMAL.value] = 0
                new_state = CacheAdaptationState.NORMAL
            else:
                print(f"State remains RECOVERY (volatility history: {last_two_volatilities})")
        else:
            print(f"State remains RECOVERY (insufficient history: {len(updated_history)} snapshots)")
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.STABLE_BUT_INEFFECTIVE:
        steps_in_state = updated_state_entry_count.get(CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value, 0)

        if opportunity_loss < OPPORTUNITY_LOSS_THRESHOLD:
            if steps_in_state >= MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE:
                print(f"State transition: STABLE_BUT_INEFFECTIVE → NORMAL (opportunity_loss={opportunity_loss:.3f} < {OPPORTUNITY_LOSS_THRESHOLD}, catch-up achieved)")
                updated_state_entry_count[CacheAdaptationState.NORMAL.value] = 0
                new_state = CacheAdaptationState.NORMAL
            else:
                print(f"State remains STABLE_BUT_INEFFECTIVE (hysteresis: {steps_in_state}/{MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE} steps, opportunity_loss={opportunity_loss:.3f})")
            return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

        if workload_volatility > VOLATILITY_HIGH:
            print(f"State transition: STABLE_BUT_INEFFECTIVE → UNSTABLE (volatility spike: {workload_volatility:.3f} > {VOLATILITY_HIGH})")
            updated_state_entry_count[CacheAdaptationState.UNSTABLE.value] = 0
            new_state = CacheAdaptationState.UNSTABLE
            return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

        print(f"State remains STABLE_BUT_INEFFECTIVE (steps={steps_in_state}, opportunity_loss={opportunity_loss:.3f}, volatility={workload_volatility:.3f})")
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.NORMAL:
        is_stable = workload_volatility < LOW_VOLATILITY_THRESHOLD
        has_opportunity_loss = opportunity_loss > OPPORTUNITY_LOSS_THRESHOLD

        if is_stable and has_opportunity_loss:
            if len(updated_opp_loss_history) >= REQUIRED_CONSECUTIVE_SNAPSHOTS:
                recent_opp_losses = updated_opp_loss_history[-REQUIRED_CONSECUTIVE_SNAPSHOTS:]
                recent_volatilities = updated_history[-REQUIRED_CONSECUTIVE_SNAPSHOTS:] if len(updated_history) >= REQUIRED_CONSECUTIVE_SNAPSHOTS else [workload_volatility]

                all_stable = all(v < LOW_VOLATILITY_THRESHOLD for v in recent_volatilities)
                all_underperforming = all(ol > OPPORTUNITY_LOSS_THRESHOLD for ol in recent_opp_losses)

                if all_stable and all_underperforming:
                    print(f"State transition: NORMAL → STABLE_BUT_INEFFECTIVE "
                          f"(volatility={workload_volatility:.3f} < {LOW_VOLATILITY_THRESHOLD}, "
                          f"opportunity_loss={opportunity_loss:.3f} > {OPPORTUNITY_LOSS_THRESHOLD}, "
                          f"persisted for {REQUIRED_CONSECUTIVE_SNAPSHOTS} snapshots)")
                    updated_state_entry_count[CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value] = 0
                    new_state = CacheAdaptationState.STABLE_BUT_INEFFECTIVE
                    return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count
            elif force_attack_mode:
                print(f"ATTACK MODE: State transition: NORMAL → STABLE_BUT_INEFFECTIVE "
                      f"(volatility={workload_volatility:.3f} > 0.35, gap={improvement:.3f} < 5%)")
                updated_state_entry_count[CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value] = 0
                new_state = CacheAdaptationState.STABLE_BUT_INEFFECTIVE
                return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

def call_llm(prompt):
    print("Calling LLM...")
    t0 = time.time()
    policy_data = None
    
    LLM_CALLS.inc()
    
    try:
        schema_hint = """Output JSON Schema:
{
  "type": "object",
  "properties": {
    "reasoning": {"type": "string"},
    "tinylfu_control": {
      "type": "object",
      "properties": {
        "decay_factor": {"type": ["number", "null"]},
        "reset_interval": {"type": ["integer", "null"]},
        "reset_sketch": {"type": ["boolean", "null"]},
        "admission_bias": {"type": ["integer", "null"]}
      },
      "required": ["decay_factor", "reset_interval", "reset_sketch", "admission_bias"],
      "additionalProperties": false
    }
  },
  "required": ["reasoning", "tinylfu_control"],
  "additionalProperties": false
}
"""

        system_prompt = """You are a cache optimizer. You tune TinyLFU admission parameters to BEAT the baseline cache.

Your goal: make Aura Hit Ratio HIGHER than Baseline Hit Ratio (positive improvement).

RULES:
1. You can ONLY set these 4 parameters (set null to keep unchanged)
2. NEVER repeat the same parameter values if improvement was zero or negative
3. If your last parameters didn't help, you MUST try different values
4. Focus on admission_bias - it has the biggest impact

Respond with valid JSON only:
""" + schema_hint
        
        tinylfu_schema = {
            "type": "object",
            "properties": {
                "reasoning": {"type": "string"},
                "tinylfu_control": {
                    "type": "object",
                    "properties": {
                        "decay_factor": {"type": ["number", "null"]},
                        "reset_interval": {"type": ["integer", "null"]},
                        "reset_sketch": {"type": ["boolean", "null"]},
                        "admission_bias": {"type": ["integer", "null"]}
                    },
                    "required": ["decay_factor", "reset_interval", "reset_sketch", "admission_bias"],
                    "additionalProperties": False
                }
            },
            "required": ["reasoning", "tinylfu_control"],
            "additionalProperties": False
        }

        payload = {
            "model": LLM_MODEL,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt}
            ],
            "format": tinylfu_schema,
            "stream": False,
            "options": {
                "temperature": 0,
                "num_predict": OLLAMA_NUM_PREDICT,
                "num_thread": OLLAMA_NUM_THREAD,
                "num_ctx": OLLAMA_NUM_CTX,
                "repeat_penalty": 1.1,
                "top_k": 40,
                "top_p": 0.9
            }
        }
        
        print(f"Calling Ollama API with model: {LLM_MODEL}")
        print(f"API URL: {LLM_API_URL}")
        print(f"Optimized settings: num_predict={OLLAMA_NUM_PREDICT}, num_thread={OLLAMA_NUM_THREAD}, num_ctx={OLLAMA_NUM_CTX}")
        resp = requests.post(LLM_API_URL, json=payload, timeout=OLLAMA_TIMEOUT_SEC)
        
        if resp.status_code == 404:
            print(f"ERROR: Model '{LLM_MODEL}' not found in Ollama. Available models:")
            try:
                list_resp = requests.get(f"http://{OLLAMA_HOST}:11434/api/tags", timeout=5)
                if list_resp.status_code == 200:
                    models = list_resp.json().get("models", [])
                    for m in models:
                        print(f"  - {m.get('name', 'unknown')}")
            except:
                print("  (Could not fetch model list)")
            LLM_ERROR_RATE.inc()
            return None
        
        resp.raise_for_status()
        response_data = resp.json()
        content = response_data.get("message", {}).get("content", "")
        
        if not content:
            print("ERROR: Empty response from Ollama API")
            print(f"DEBUG: Full response: {response_data}")
            LLM_ERROR_RATE.inc()
            return None
        
        policy_data = json.loads(content)
        
    except Exception as e:
        print(f"ERROR: LLM Call failed: {e}")
        LLM_ERROR_RATE.inc()
        return None
    
    latency_seconds = time.time() - t0
    latency_ms = latency_seconds * 1000
    LATENCY_MS.set(latency_ms)
        
    print(f"LLM Latency: {latency_seconds:.2f}s ({latency_ms:.2f}ms)")
    return policy_data

llm_queue = Queue()

def save_temporal_metrics(cache_metrics):
    try:
        metrics_dir = Path("data/temporal_metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        
        metrics_file = metrics_dir / "llm_calls_temporal.json"
        
        if metrics_file.exists():
            try:
                with open(metrics_file, 'r') as f:
                    all_metrics = json.load(f)
            except (json.JSONDecodeError, IOError):
                all_metrics = []
        else:
            all_metrics = []
        
        all_metrics.append(cache_metrics)
        
        with open(metrics_file, 'w') as f:
            json.dump(all_metrics, f, indent=2)
        
        print(f"Saved temporal metrics: LLM call #{len(all_metrics)}, AURA HR={cache_metrics.get('aura_hit_ratio', 0):.4f}, Baseline HR={cache_metrics.get('baseline_hit_ratio', 0):.4f}")
    except Exception as e:
        print(f"WARNING: Failed to save temporal metrics: {e}")
        import traceback
        traceback.print_exc()

def llm_worker_thread(producer):
    global llm_call_count, reset_sketch_call_count, test_complete_time, test_complete_lock, last_test_name
    print("LLM Worker Thread started")
    while not shutdown_requested.is_set():
        try:
            task = llm_queue.get(timeout=1.0)
            if task is None: continue
            
            with test_complete_lock:
                if test_complete_time is not None:
                    print(f"LLM Worker: Skipping task - test already completed. Waiting for new test...")
                    llm_queue.task_done()
                    continue
            
            prompt = task.get("prompt")
            mode = task.get("mode", "tinylfu_parameter_update")
            cache_metrics = task.get("cache_metrics")
            
            if cache_metrics:
                save_temporal_metrics(cache_metrics)
            else:
                print("WARNING: No cache_metrics in LLM task, skipping temporal metrics save")
            
            with policy_lock:
                llm_call_count += 1
                if llm_call_count >= RESET_SKETCH_COOLDOWN:
                    reset_sketch_call_count = max(0, reset_sketch_call_count - 1)
                    llm_call_count = 0
            
            policy_dict = call_llm(prompt)
            
            if policy_dict:
                tinylfu_control = policy_dict.get("tinylfu_control")
                if not tinylfu_control:
                    print(f"ERROR: LLM output missing 'tinylfu_control' field: {policy_dict}")
                    llm_queue.task_done()
                    continue

                decay_factor = tinylfu_control.get("decay_factor")
                reset_interval = tinylfu_control.get("reset_interval")
                reset_sketch = tinylfu_control.get("reset_sketch")
                admission_bias = tinylfu_control.get("admission_bias")

                if decay_factor is not None:
                    if not isinstance(decay_factor, (int, float)) or not (0.0 <= decay_factor <= 1.0):
                        print(f"ERROR: Invalid decay_factor {decay_factor}, must be float in [0.0, 1.0]")
                        decay_factor = None

                if reset_interval is not None:
                    if not isinstance(reset_interval, int) or not (50000 <= reset_interval <= 500000):
                        print(f"ERROR: Invalid reset_interval {reset_interval}, must be int in [50000, 500000]")
                        reset_interval = None
                
                if admission_bias is not None:
                    if not isinstance(admission_bias, int) or not (-5 <= admission_bias <= 5):
                        print(f"ERROR: Invalid admission_bias {admission_bias}, must be int in [-5, 5]")
                        admission_bias = None
                
                if reset_sketch is not None:
                    if not isinstance(reset_sketch, bool):
                        print(f"ERROR: Invalid reset_sketch {reset_sketch}, must be boolean")
                        reset_sketch = None
                    elif reset_sketch is False:
                        reset_sketch = None
                    elif reset_sketch is True:
                        with policy_lock:
                            if reset_sketch_call_count >= RESET_SKETCH_COOLDOWN:
                                print(f"WARNING: Reset sketch requested but cooldown active. {reset_sketch_call_count} resets in last {llm_call_count} LLM calls (max {RESET_SKETCH_COOLDOWN} allowed). Forcing reset_sketch to null.")
                                reset_sketch = None
                            else:
                                reset_sketch_call_count += 1
                                print(f"Reset sketch approved. Total resets in last {llm_call_count} calls: {reset_sketch_call_count}/{RESET_SKETCH_COOLDOWN}")
                
                
                with policy_lock:
                    adaptation_state = current_adaptation_state

                reasoning = policy_dict.get("reasoning", "")
                if not reasoning or not isinstance(reasoning, str):
                    print(f"WARNING: LLM did not provide reasoning field")
                    reasoning = "No reasoning provided"
                else:
                    print(f"LLM Reasoning: {reasoning}..." if len(reasoning) > 200 else f"LLM Reasoning: {reasoning}")

                plan_output = {
                    "type": "tinylfu_parameter_update",
                    "timestamp": time.time(),
                    "adaptation_state": adaptation_state.value,
                    "reasoning": reasoning,
                    "tinylfu_control": {
                        "decay_factor": decay_factor,
                        "reset_interval": reset_interval,
                        "reset_sketch": reset_sketch,
                        "admission_bias": admission_bias
                    }
                }

                with policy_lock:
                    global last_generated_policy, parameter_history
                    last_generated_policy = plan_output.copy()

                    parameter_history.append(plan_output["tinylfu_control"].copy())
                    if len(parameter_history) > MAX_PARAMETER_HISTORY:
                        parameter_history.pop(0)

                    if len(parameter_history) >= 3:
                        all_same = all(
                            p == parameter_history[0] 
                            for p in parameter_history[-3:]
                        )
                        if all_same:
                            print(f"WARNING: LLM using same TinyLFU params for 3+ iterations")

                changes = []
                if decay_factor is not None:
                    changes.append(f"decay_factor={decay_factor}")
                if reset_interval is not None:
                    changes.append(f"reset_interval={reset_interval}")
                if reset_sketch is not None:
                    changes.append(f"reset_sketch={reset_sketch}")
                if admission_bias is not None:
                    changes.append(f"admission_bias={admission_bias}")
                
                if changes:
                    print(f"TinyLFU parameter update: {', '.join(changes)}")
                else:
                    print(f"TinyLFU parameter update: no changes (all null)")
                
                print("Producing Plan to Kafka...")
                producer.produce(KAFKA_TOPIC_PLAN, json.dumps(plan_output).encode('utf-8'))
                producer.flush()
            
            llm_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"ERROR: LLM Worker Error: {e}")
            import traceback
            traceback.print_exc()

def run_brain():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"LLM backend: OLLAMA (local)")
    print(f"OLLAMA_HOST: {OLLAMA_HOST}")
    print(f"OLLAMA_MODEL: {LLM_MODEL}")

    check_ollama_active()
    
    print("Initializing Kafka Consumer/Producer...")
    producer = get_kafka_producer("aura_brain_producer")
    
    worker = threading.Thread(target=llm_worker_thread, args=(producer,), daemon=True)
    worker.start()

    print("Connecting to Kafka topic aura-stats...")
    consumer = None
    retry_count = 0
    max_retries = 30
    
    while consumer is None and retry_count < max_retries and not shutdown_requested.is_set():
        try:
            consumer = get_kafka_consumer("aura_brain_group", [KAFKA_TOPIC_STATS])
            msg = consumer.poll(0.5)
            if msg and msg.error() and "UNKNOWN_TOPIC_OR_PART" in str(msg.error()):
                print(f"Topic {KAFKA_TOPIC_STATS} not ready yet, retrying... ({retry_count}/{max_retries})")
                consumer.close()
                consumer = None
                retry_count += 1
                time.sleep(2)
                continue
            print(f"Successfully connected to Kafka topic {KAFKA_TOPIC_STATS}")
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
        print(f"ERROR: Failed to connect to Kafka after {max_retries} retries. Exiting.")
        return
    
    print("Entering loop...")
    last_policy = None
    last_policy_copy = None
    last_snapshot_time = time.time()
    global test_complete_time, last_test_name
    
    try:
        while not shutdown_requested.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                error_str = str(msg.error())
                if "UNKNOWN_TOPIC_OR_PART" in error_str:
                    print(f"Topic {KAFKA_TOPIC_STATS} not available yet, waiting...")
                    time.sleep(5)
                    continue
                print(f"ERROR: Kafka Consumer: {msg.error()}")
                continue
                
            try:
                stats_json = json.loads(msg.value().decode('utf-8'))
                
                test_complete = stats_json.get('test_complete', False)
                current_test_name = stats_json.get('test_name', None)
                
                if test_complete:
                    test_name = stats_json.get('test_name', 'unknown')
                    final_events = stats_json.get('final_events', 0)
                    print(f"Received test_complete signal for test: {test_name} (final_events: {final_events})")
                    print(f"Resetting brain state and waiting for new test...")
                    reset_brain_state()
                    with test_complete_lock:
                        test_complete_time = time.time()
                        last_test_name = test_name
                    last_snapshot_time = time.time()
                    continue
                
                with test_complete_lock:
                    if test_complete_time is not None:
                        if current_test_name and current_test_name != last_test_name:
                            print(f"New test detected: {current_test_name} (was: {last_test_name}). Resetting state and resuming processing...")
                            reset_brain_state()
                            test_complete_time = None
                            last_test_name = current_test_name
                        else:
                            print(f"Ignoring snapshot - test already completed. Waiting for new test...")
                            continue
                
                if current_test_name:
                    with test_complete_lock:
                        if last_test_name is None or current_test_name != last_test_name:
                            if last_test_name is not None:
                                print(f"New test started: {current_test_name} (was: {last_test_name}). Resetting state...")
                                reset_brain_state()
                            last_test_name = current_test_name
                
                total_events = stats_json.get('total_events', 0)
                recent_sequence = stats_json.get('recent_access_sequence', [])
                last_snapshot_time = time.time()
                
                print(f"Received stats snapshot. Total events: {total_events}")
                
                cache_full = 50.0 
                try:
                    r = get_redis_aura()
                    info = r.info("memory")
                    used = info.get("used_memory", 0)
                    cache_full = (used / AURA_CACHE_LIMIT_BYTES) * 100.0
                    cache_full = min(100.0, max(0.0, cache_full))
                except Exception as e:
                    print(f"WARNING: Failed to query Redis memory: {e}. Defaulting to 50%")
                
                print(f"Current Cache Usage: {cache_full:.1f}%")

                workload_volatility = stats_json.get("workload_volatility", 0.0)

                window_aura_hr = None
                window_baseline_hr = None
                delta_baseline_hr = None
                delta_aura_hr = None
                baseline_hits = None
                baseline_misses = None
                aura_hits = None
                aura_misses = None
                if "baseline_comparison" in stats_json:
                    baseline_comp = stats_json["baseline_comparison"]
                    window_aura_hr = baseline_comp.get("aura_hit_ratio")
                    window_baseline_hr = baseline_comp.get("baseline_hit_ratio")
                    delta_baseline_hr = baseline_comp.get("delta_baseline_hit_ratio")
                    delta_aura_hr = baseline_comp.get("delta_aura_hit_ratio")
                    baseline_hits = baseline_comp.get("baseline_hits")
                    baseline_misses = baseline_comp.get("baseline_misses")
                    aura_hits = baseline_comp.get("aura_hits")
                    aura_misses = baseline_comp.get("aura_misses")

                    baseline_hr_str = f"{window_baseline_hr:.4f}" if window_baseline_hr is not None else "None"
                    baseline_delta_str = f"{delta_baseline_hr:.4f}" if delta_baseline_hr is not None else "None"
                    aura_hr_str = f"{window_aura_hr:.4f}" if window_aura_hr is not None else "None"
                    aura_delta_str = f"{delta_aura_hr:.4f}" if delta_aura_hr is not None else "None"
                    print(f"DEBUG - Baseline: hits={baseline_hits}, misses={baseline_misses}, HR={baseline_hr_str}, delta={baseline_delta_str}")
                    print(f"DEBUG - Aura: hits={aura_hits}, misses={aura_misses}, HR={aura_hr_str}, delta={aura_delta_str}")
                
                with policy_lock:
                    global current_adaptation_state, volatility_history, opportunity_loss_history, state_entry_snapshot_count
                    new_state, volatility_history, opportunity_loss_history, state_entry_snapshot_count = compute_adaptation_state(
                        workload_volatility, current_adaptation_state, volatility_history,
                        window_aura_hr=window_aura_hr,
                        window_baseline_hr=window_baseline_hr,
                        opportunity_loss_history=opportunity_loss_history,
                        state_entry_snapshot_count=state_entry_snapshot_count,
                        delta_baseline_hr=delta_baseline_hr,
                        delta_aura_hr=delta_aura_hr
                    )
                    current_adaptation_state = new_state

                    opp_loss_str = ""
                    opp_loss = 0.0
                    snapshot_events = stats_json.get('total_events', 5000)

                    if delta_baseline_hr is not None and delta_aura_hr is not None:
                        opp_loss = float(delta_baseline_hr) - float(delta_aura_hr)
                        opp_loss = round(opp_loss, 6)
                        opp_loss_str = f", opportunity_loss={opp_loss:.6f} (delta-based: {delta_baseline_hr:.6f} - {delta_aura_hr:.6f})"

                        print(f"DEBUG: WindowHits[Aura: {aura_hits}, Base: {baseline_hits}] -> DeltaHR[Aura: {delta_aura_hr:.6f}, Base: {delta_baseline_hr:.6f}] -> OppLoss: {opp_loss:.6f}")

                        if abs(opp_loss) < 0.0001:
                            print(f"WARNING: Opportunity loss is near zero ({opp_loss:.6f}). This may indicate:")
                            print(f"    - First snapshot (no previous values for delta)")
                            print(f"    - Both caches performing identically")
                            print(f"    - Precision issue (differences < 0.0001)")
                            if baseline_hits is not None and aura_hits is not None:
                                baseline_total = baseline_hits + baseline_misses if baseline_misses is not None else 0
                                aura_total = aura_hits + aura_misses if aura_misses is not None else 0
                                print(f"    - Baseline total: {baseline_total}, Aura total: {aura_total}")
                                if baseline_total > 0 and aura_total > 0:
                                    baseline_hr_calc = baseline_hits / baseline_total
                                    aura_hr_calc = aura_hits / aura_total
                                    print(f"    - Calculated HR: Baseline={baseline_hr_calc:.6f}, Aura={aura_hr_calc:.6f}, Diff={baseline_hr_calc - aura_hr_calc:.6f}")
                    elif window_baseline_hr is not None and window_aura_hr is not None:
                        opp_loss = float(window_baseline_hr) - float(window_aura_hr)
                        opp_loss = round(opp_loss, 6)
                        opp_loss_str = f", opportunity_loss={opp_loss:.6f} (absolute: {window_baseline_hr:.6f} - {window_aura_hr:.6f})"
                        if abs(opp_loss) < 0.0001:
                            print(f"WARNING: Opportunity loss is near zero ({opp_loss:.6f}) using absolute values")
                    
                    print(f"Adaptation State: {current_adaptation_state.value} (volatility={workload_volatility:.3f}{opp_loss_str})")
                
                metrics_feedback = {}

                if "tinylfu_stats" in stats_json:
                    metrics_feedback["eviction_efficiency"] = stats_json["tinylfu_stats"].get("efficiency", 1.0)

                if "baseline_comparison" in stats_json:
                    baseline_comp = stats_json["baseline_comparison"]
                    metrics_feedback["baseline_hit_ratio"] = baseline_comp.get("baseline_hit_ratio", 0.0)
                    metrics_feedback["improvement_over_baseline"] = baseline_comp.get("improvement_over_baseline", 0.0)
                    metrics_feedback["baseline_vs_aura"] = {
                        "baseline": baseline_comp.get("baseline_hit_ratio", 0.0),
                        "aura": baseline_comp.get("aura_hit_ratio", 0.0),
                        "improvement": baseline_comp.get("improvement_over_baseline", 0.0)
                    }

                with policy_lock:
                    adaptation_state = current_adaptation_state
                    opp_loss = None
                    if delta_baseline_hr is not None and delta_aura_hr is not None:
                        opp_loss = delta_baseline_hr - delta_aura_hr
                    elif window_baseline_hr is not None and window_aura_hr is not None:
                        opp_loss = window_baseline_hr - window_aura_hr
                
                if adaptation_state == CacheAdaptationState.STABLE_BUT_INEFFECTIVE:
                    metrics_feedback["adaptation_context"] = {
                        "state": adaptation_state.value,
                        "opportunity_loss": opp_loss if opp_loss is not None else 0.0,
                        "guidance": "Cache is stable but underperforming baseline. TinyLFU Sketch likely saturated with stale heavy hitters. Consider aggressive forgetting via lower decay_factor or more frequent resets."
                    }

                with policy_lock:
                    last_policy_copy = last_generated_policy.copy() if last_generated_policy else None
                    history_copy = snapshot_history.copy() if snapshot_history else []

                    params_applied = {}
                    if last_policy_copy and "tinylfu_control" in last_policy_copy:
                        params_applied = last_policy_copy["tinylfu_control"].copy()

                    aura_hr = window_aura_hr if window_aura_hr is not None else 0.0
                    improvement = metrics_feedback.get("improvement_over_baseline", 0.0) if metrics_feedback else 0.0

                    history_entry = {
                        "timestamp": stats_json.get("timestamp", time.time()),
                        "adaptation_state": current_adaptation_state.value,
                        "parameters_applied": params_applied,
                        "hit_ratio_aura": aura_hr,
                        "improvement_vs_baseline": improvement,
                        "workload_volatility": workload_volatility
                    }

                    global best_strategy
                    with best_strategy_lock:
                        current_time = time.time()
                        if best_strategy is None or improvement > best_strategy.get("improvement_vs_baseline", -999):
                            if best_strategy is None or (current_time - best_strategy.get("timestamp", 0)) < 3600:
                                best_strategy = {
                                    "parameters_applied": params_applied.copy() if params_applied else {},
                                    "improvement_vs_baseline": improvement,
                                    "workload_volatility": workload_volatility,
                                    "timestamp": current_time,
                                    "hit_ratio_aura": aura_hr
                                }
                                print(f"New Best Strategy: Improvement={improvement:.4f}, Volatility={workload_volatility:.3f}, Params={params_applied}")
                        elif best_strategy and (current_time - best_strategy.get("timestamp", 0)) >= 3600:
                            best_strategy = None

                    snapshot_history.append(history_entry)
                    if len(snapshot_history) > MAX_SNAPSHOT_HISTORY:
                        snapshot_history.pop(0)

                    history_copy = snapshot_history.copy()

                with best_strategy_lock:
                    best_strategy_copy = best_strategy.copy() if best_strategy else None
                
                with policy_lock:
                    reset_info = {
                        "recent_resets": reset_sketch_call_count,
                        "calls_since_last_reset": llm_call_count,
                        "cooldown_active": reset_sketch_call_count >= RESET_SKETCH_COOLDOWN
                    }
                    if metrics_feedback is None:
                        metrics_feedback = {}
                    metrics_feedback["reset_sketch_info"] = reset_info
                
                # if not USE_GROQ and _is_small_model(LLM_MODEL):  # Commentato per test locali - per groq cloud llm test
                if _is_small_model(LLM_MODEL):
                    prompt = build_global_prompt_small(stats_json, last_policy_copy, metrics_feedback)
                    print(f"Using SMALL prompt for model: {LLM_MODEL}")
                else:
                    prompt = build_global_prompt(
                        stats_json,
                        cache_full,
                        last_policy_copy,
                        metrics_feedback,
                        history_copy,
                        current_adaptation_state,
                        best_strategy_copy
                    )
                
                params_applied = {}
                if last_policy_copy and "tinylfu_control" in last_policy_copy:
                    params_applied = last_policy_copy["tinylfu_control"].copy()
                
                cache_metrics = {
                    "timestamp": time.time(),
                    "baseline_hit_ratio": window_baseline_hr if window_baseline_hr is not None else 0.0,
                    "aura_hit_ratio": window_aura_hr if window_aura_hr is not None else 0.0,
                    "baseline_hits": baseline_hits if baseline_hits is not None else 0,
                    "baseline_misses": baseline_misses if baseline_misses is not None else 0,
                    "aura_hits": aura_hits if aura_hits is not None else 0,
                    "aura_misses": aura_misses if aura_misses is not None else 0,
                    "improvement_over_baseline": improvement if improvement is not None else 0.0,
                    "workload_volatility": workload_volatility,
                    "adaptation_state": current_adaptation_state.value,
                    "total_events": total_events,
                    "decay_factor": params_applied.get("decay_factor"),
                    "reset_interval": params_applied.get("reset_interval"),
                    "reset_sketch": params_applied.get("reset_sketch"),
                    "admission_bias": params_applied.get("admission_bias")
                }
                
                with test_complete_lock:
                    if test_complete_time is not None:
                        print(f"Skipping LLM call - test already completed.")
                        continue
                
                llm_queue.put({
                    "prompt": prompt, 
                    "mode": "tinylfu_parameter_update",
                    "cache_metrics": cache_metrics
                })
                
            except Exception as e:
                print(f"ERROR: Brain Loop Error: {e}")
                import traceback
                traceback.print_exc()
                
    finally:
        consumer.close()
        print("Shutdown.")

if __name__ == "__main__":
    run_brain()

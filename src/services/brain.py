import json
import time
import requests
import threading
import signal
from ..core.config import (
    KAFKA_TOPIC_STATS, KAFKA_TOPIC_PLAN,
    USE_GROQ, GROQ_API_KEY, GROQ_API_URL, GROQ_MODEL,
    LLM_API_URL, LLM_MODEL, OLLAMA_HOST,
    OLLAMA_TIMEOUT_SEC, OLLAMA_NUM_PREDICT, OLLAMA_NUM_THREAD, OLLAMA_NUM_CTX,
    AURA_CACHE_LIMIT_BYTES,
    CacheAdaptationState, VOLATILITY_HIGH, VOLATILITY_LOW,
    LOW_VOLATILITY_THRESHOLD, OPPORTUNITY_LOSS_THRESHOLD,
    REQUIRED_CONSECUTIVE_SNAPSHOTS, MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE
)
from ..core.db import get_kafka_consumer, get_kafka_producer, get_redis_aura
from ..modules.prompts import build_global_prompt, build_global_prompt_small, _is_small_model
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

def check_ollama_active():
    if USE_GROQ:
        return
    try:
        version_url = f"http://{OLLAMA_HOST}:11434/api/version"
        resp = requests.get(version_url, timeout=3)
        resp.raise_for_status()
        print(f"[BRAIN] Ollama is active: {resp.json().get('version', 'unknown version')}")
    except Exception as e:
        print(f"[BRAIN] WARNING: Ollama not reachable at http://{OLLAMA_HOST}:11434 ({e})")

def signal_handler(signum, frame):
    print(f"\n[BRAIN] Received signal {signum}, initiating graceful shutdown...")
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
            print(f"[BRAIN] State transition: {current_state} → UNSTABLE (volatility={workload_volatility:.3f} > {VOLATILITY_HIGH})")
            updated_state_entry_count[CacheAdaptationState.UNSTABLE.value] = 0
        new_state = CacheAdaptationState.UNSTABLE
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.UNSTABLE:
        print(f"[BRAIN] State transition: UNSTABLE → RECOVERY (volatility decreased to {workload_volatility:.3f})")
        updated_state_entry_count[CacheAdaptationState.RECOVERY.value] = 0
        new_state = CacheAdaptationState.RECOVERY
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.RECOVERY:
        if len(updated_history) >= 2:
            last_two_volatilities = updated_history[-2:]
            if all(v < VOLATILITY_LOW for v in last_two_volatilities):
                print(f"[BRAIN] State transition: RECOVERY → NORMAL (2 consecutive low volatility: {last_two_volatilities})")
                updated_state_entry_count[CacheAdaptationState.NORMAL.value] = 0
                new_state = CacheAdaptationState.NORMAL
            else:
                print(f"[BRAIN] State remains RECOVERY (volatility history: {last_two_volatilities})")
        else:
            print(f"[BRAIN] State remains RECOVERY (insufficient history: {len(updated_history)} snapshots)")
        return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    if current_state == CacheAdaptationState.STABLE_BUT_INEFFECTIVE:
        steps_in_state = updated_state_entry_count.get(CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value, 0)

        if opportunity_loss < OPPORTUNITY_LOSS_THRESHOLD:
            if steps_in_state >= MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE:
                print(f"[BRAIN] State transition: STABLE_BUT_INEFFECTIVE → NORMAL (opportunity_loss={opportunity_loss:.3f} < {OPPORTUNITY_LOSS_THRESHOLD}, catch-up achieved)")
                updated_state_entry_count[CacheAdaptationState.NORMAL.value] = 0
                new_state = CacheAdaptationState.NORMAL
            else:
                print(f"[BRAIN] State remains STABLE_BUT_INEFFECTIVE (hysteresis: {steps_in_state}/{MIN_STEPS_IN_STABLE_BUT_INEFFECTIVE} steps, opportunity_loss={opportunity_loss:.3f})")
            return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

        if workload_volatility > VOLATILITY_HIGH:
            print(f"[BRAIN] State transition: STABLE_BUT_INEFFECTIVE → UNSTABLE (volatility spike: {workload_volatility:.3f} > {VOLATILITY_HIGH})")
            updated_state_entry_count[CacheAdaptationState.UNSTABLE.value] = 0
            new_state = CacheAdaptationState.UNSTABLE
            return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

        print(f"[BRAIN] State remains STABLE_BUT_INEFFECTIVE (steps={steps_in_state}, opportunity_loss={opportunity_loss:.3f}, volatility={workload_volatility:.3f})")
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
                    print(f"[BRAIN] State transition: NORMAL → STABLE_BUT_INEFFECTIVE "
                          f"(volatility={workload_volatility:.3f} < {LOW_VOLATILITY_THRESHOLD}, "
                          f"opportunity_loss={opportunity_loss:.3f} > {OPPORTUNITY_LOSS_THRESHOLD}, "
                          f"persisted for {REQUIRED_CONSECUTIVE_SNAPSHOTS} snapshots)")
                    updated_state_entry_count[CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value] = 0
                    new_state = CacheAdaptationState.STABLE_BUT_INEFFECTIVE
                    return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count
            elif force_attack_mode:
                print(f"[BRAIN] ⚡ ATTACK MODE: State transition: NORMAL → STABLE_BUT_INEFFECTIVE "
                      f"(volatility={workload_volatility:.3f} > 0.35, gap={improvement:.3f} < 5%)")
                updated_state_entry_count[CacheAdaptationState.STABLE_BUT_INEFFECTIVE.value] = 0
                new_state = CacheAdaptationState.STABLE_BUT_INEFFECTIVE
                return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

    return new_state, updated_history, updated_opp_loss_history, updated_state_entry_count

def call_llm(prompt):
    print("[BRAIN] Calling LLM...")
    t0 = time.time()
    policy_data = None
    
    try:
        if USE_GROQ:
            if not GROQ_API_KEY or not GROQ_API_KEY.strip():
                print("[ERROR] GROQ_API_KEY is empty or not set!")
                return None
            
            headers = {
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            }
            
            system_content = """You are an Intelligent TinyLFU meta-controller with memory and reasoning capabilities. You adapt TinyLFU admission policy parameters based on workload dynamics, historical performance, and cluster trends.

CRITICAL: You MUST NOT evict keys, prefetch keys, or set TTLs. You can ONLY adjust TinyLFU parameters.

You MUST provide Chain-of-Thought reasoning before making parameter decisions. Analyze:
1. Historical performance patterns (what worked, what didn't)
2. Cluster volume trends (RISING/DROPPING/STABLE)
3. Workload volatility and hit ratio changes
4. Relationship between previous decisions and outcomes

Respond ONLY with valid JSON using this exact format:
{
    "reasoning": "Your technical analysis explaining why these parameter changes are needed (2-4 sentences)",
    "tinylfu_control": {
        "decay_factor": 0.95 | null,
        "reset_interval": 80000 | null,
        "doorkeeper_bias": {
            "category_name": 1
        } | null
    }
}

Constraints:
- reasoning: REQUIRED - Brief technical analysis (2-4 sentences) explaining your decision
- tinylfu_control: REQUIRED (but all fields inside may be null)
- decay_factor: null if no change, otherwise float in [0.0, 1.0] - scales down frequency counters
- reset_interval: null if no change, otherwise int in [50000, 500000] - accesses before reset
- doorkeeper_bias: null if no change, otherwise dict with category->int mappings (positive = more likely to admit)
- NO other fields allowed
- NO cluster admission decisions
- NO evict/prefetch/TTL operations

Respond ONLY with valid JSON. Do NOT use markdown formatting (no ```json or ```). Return RAW JSON only."""
            
            payload = {
                "model": GROQ_MODEL,
                "messages": [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 2048,
                "response_format": {
                    "type": "json_object"
                }
            }
            
            print(f"[BRAIN] Calling Groq API with model: {GROQ_MODEL}")
            resp = requests.post(GROQ_API_URL, headers=headers, json=payload, timeout=30)
            
            if resp.status_code == 400:
                error_body = resp.text
                print(f"[ERROR] Groq API returned 400 Bad Request")
                print(f"[ERROR] Response body: {error_body}")
                try:
                    error_json = resp.json()
                    if "error" in error_json:
                        print(f"[ERROR] Error message: {error_json['error'].get('message', 'Unknown error')}")
                        print(f"[ERROR] Error type: {error_json['error'].get('type', 'Unknown')}")
                except:
                    pass
                return None
            
            try:
                resp.raise_for_status()
            except requests.exceptions.HTTPError as e:
                print(f"[ERROR] Groq API Error: {e}")
                print(f"[ERROR] Status Code: {resp.status_code}")
                print(f"[ERROR] Response Body: {resp.text}")
                return None
                
            response_data = resp.json()
            content = response_data.get("choices", [{}])[0].get("message", {}).get("content", "")
            
            if not content:
                print("[ERROR] Empty response from Groq API")
                return None
            
            policy_data = json.loads(content)
            
        else:
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
        "doorkeeper_bias": {
          "type": ["object", "null"],
          "additionalProperties": {"type": "integer"}
        }
      },
      "required": ["decay_factor", "reset_interval", "doorkeeper_bias"],
      "additionalProperties": false
    }
  },
  "required": ["reasoning", "tinylfu_control"],
  "additionalProperties": false
}
"""

            system_prompt = """You are an Intelligent TinyLFU meta-controller with memory and reasoning capabilities. You adapt TinyLFU admission policy parameters based on workload dynamics, historical performance, and cluster trends.

CRITICAL: You MUST NOT evict keys, prefetch keys, or set TTLs. You can ONLY adjust TinyLFU parameters.

Respond ONLY with valid JSON using this exact format:
{
    "reasoning": "Your technical analysis explaining why these parameter changes are needed (2-4 sentences)",
    "tinylfu_control": {
        "decay_factor": 0.95 | null,
        "reset_interval": 80000 | null,
        "doorkeeper_bias": {"category_name": 1} | null
    }
}

STRICTLY follow the schema below:
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
                            "doorkeeper_bias": {
                                "type": ["object", "null"],
                                "additionalProperties": {"type": "integer"}
                            }
                        },
                        "required": ["decay_factor", "reset_interval", "doorkeeper_bias"],
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
            
            print(f"[BRAIN] Calling Ollama API with model: {LLM_MODEL}")
            print(f"[BRAIN] API URL: {LLM_API_URL}")
            print(f"[BRAIN] Optimized settings: num_predict={OLLAMA_NUM_PREDICT}, num_thread={OLLAMA_NUM_THREAD}, num_ctx={OLLAMA_NUM_CTX}")
            resp = requests.post(LLM_API_URL, json=payload, timeout=OLLAMA_TIMEOUT_SEC)
            
            if resp.status_code == 404:
                print(f"[ERROR] Model '{LLM_MODEL}' not found in Ollama. Available models:")
                try:
                    list_resp = requests.get("http://ollama:11434/api/tags", timeout=5)
                    if list_resp.status_code == 200:
                        models = list_resp.json().get("models", [])
                        for m in models:
                            print(f"  - {m.get('name', 'unknown')}")
                except:
                    print("  (Could not fetch model list)")
                return None
            
            resp.raise_for_status()
            response_data = resp.json()
            content = response_data.get("message", {}).get("content", "")
            
            if not content:
                print("[ERROR] Empty response from Ollama API")
                print(f"[DEBUG] Full response: {response_data}")
                return None
            
            policy_data = json.loads(content)
            
    except Exception as e:
        print(f"[ERROR] LLM Call failed: {e}")
        return None
        
    print(f"[BRAIN] LLM Latency: {time.time()-t0:.2f}s")
    return policy_data

llm_queue = Queue()

def llm_worker_thread(producer):
    print("[BRAIN] LLM Worker Thread started")
    while not shutdown_requested.is_set():
        try:
            task = llm_queue.get(timeout=1.0)
            if task is None: continue
            
            prompt = task.get("prompt")
            mode = task.get("mode", "tinylfu_parameter_update")
            
            policy_dict = call_llm(prompt)
            
            if policy_dict:
                tinylfu_control = policy_dict.get("tinylfu_control")
                if not tinylfu_control:
                    print(f"[BRAIN] ERROR: LLM output missing 'tinylfu_control' field: {policy_dict}")
                    llm_queue.task_done()
                    continue

                decay_factor = tinylfu_control.get("decay_factor")
                reset_interval = tinylfu_control.get("reset_interval")
                doorkeeper_bias = tinylfu_control.get("doorkeeper_bias")

                if decay_factor is not None:
                    if not isinstance(decay_factor, (int, float)) or not (0.0 <= decay_factor <= 1.0):
                        print(f"[BRAIN] ERROR: Invalid decay_factor {decay_factor}, must be float in [0.0, 1.0]")
                        decay_factor = None

                if reset_interval is not None:
                    if not isinstance(reset_interval, int) or not (50000 <= reset_interval <= 500000):
                        print(f"[BRAIN] ERROR: Invalid reset_interval {reset_interval}, must be int in [50000, 500000]")
                        reset_interval = None

                if doorkeeper_bias is not None:
                    if not isinstance(doorkeeper_bias, dict):
                        print(f"[BRAIN] ERROR: Invalid doorkeeper_bias {doorkeeper_bias}, must be dict")
                        doorkeeper_bias = None
                    else:
                        validated_bias = {}
                        for cat, bias_val in doorkeeper_bias.items():
                            if isinstance(bias_val, int):
                                validated_bias[cat] = bias_val
                            else:
                                print(f"[BRAIN] WARNING: Invalid bias value for {cat}: {bias_val}, skipping")
                        doorkeeper_bias = validated_bias if validated_bias else None
                
                with policy_lock:
                    adaptation_state = current_adaptation_state

                reasoning = policy_dict.get("reasoning", "")
                if not reasoning or not isinstance(reasoning, str):
                    print(f"[BRAIN] ⚠️ WARNING: LLM did not provide reasoning field")
                    reasoning = "No reasoning provided"
                else:
                    print(f"[BRAIN] ✓ LLM Reasoning: {reasoning}..." if len(reasoning) > 200 else f"[BRAIN] ✓ LLM Reasoning: {reasoning}")

                plan_output = {
                    "type": "tinylfu_parameter_update",
                    "timestamp": time.time(),
                    "adaptation_state": adaptation_state.value,
                    "reasoning": reasoning,
                    "tinylfu_control": {
                        "decay_factor": decay_factor,
                        "reset_interval": reset_interval,
                        "doorkeeper_bias": doorkeeper_bias
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
                            print(f"[BRAIN] ⚠️ WARNING: LLM using same TinyLFU params for 3+ iterations")

                changes = []
                if decay_factor is not None:
                    changes.append(f"decay_factor={decay_factor}")
                if reset_interval is not None:
                    changes.append(f"reset_interval={reset_interval}")
                if doorkeeper_bias is not None:
                    changes.append(f"doorkeeper_bias={len(doorkeeper_bias)} categories")
                
                if changes:
                    print(f"[BRAIN] TinyLFU parameter update: {', '.join(changes)}")
                else:
                    print(f"[BRAIN] TinyLFU parameter update: no changes (all null)")
                
                print("[BRAIN] Producing Plan to Kafka...")
                producer.produce(KAFKA_TOPIC_PLAN, json.dumps(plan_output).encode('utf-8'))
                producer.flush()
            
            llm_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"[ERROR] LLM Worker Error: {e}")
            import traceback
            traceback.print_exc()

def run_brain():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"[BRAIN] LLM backend: {'GROQ' if USE_GROQ else 'OLLAMA'}")
    print(f"[BRAIN] GROQ_API_KEY set: {bool(GROQ_API_KEY)}")
    print(f"[BRAIN] GROQ_MODEL: {GROQ_MODEL}")
    print(f"[BRAIN] OLLAMA_HOST: {OLLAMA_HOST}")
    print(f"[BRAIN] OLLAMA_MODEL: {LLM_MODEL}")

    check_ollama_active()
    
    print("[BRAIN] Initializing Kafka Consumer/Producer...")
    producer = get_kafka_producer("aura_brain_producer")
    
    worker = threading.Thread(target=llm_worker_thread, args=(producer,), daemon=True)
    worker.start()

    print("[BRAIN] Connecting to Kafka topic aura-stats...")
    consumer = None
    retry_count = 0
    max_retries = 30
    
    while consumer is None and retry_count < max_retries and not shutdown_requested.is_set():
        try:
            consumer = get_kafka_consumer("aura_brain_group", [KAFKA_TOPIC_STATS])
            msg = consumer.poll(0.5)
            if msg and msg.error() and "UNKNOWN_TOPIC_OR_PART" in str(msg.error()):
                print(f"[BRAIN] Topic {KAFKA_TOPIC_STATS} not ready yet, retrying... ({retry_count}/{max_retries})")
                consumer.close()
                consumer = None
                retry_count += 1
                time.sleep(2)
                continue
            print(f"[BRAIN] Successfully connected to Kafka topic {KAFKA_TOPIC_STATS}")
            break
        except Exception as e:
            print(f"[BRAIN] Failed to connect to Kafka, retrying... ({retry_count}/{max_retries}): {e}")
            if consumer:
                try:
                    consumer.close()
                except:
                    pass
            consumer = None
            retry_count += 1
            time.sleep(2)
    
    if consumer is None:
        print(f"[BRAIN] ERROR: Failed to connect to Kafka after {max_retries} retries. Exiting.")
        return
    
    print("[BRAIN] Entering loop...")
    last_policy = None
    last_policy_copy = None
    
    try:
        while not shutdown_requested.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                error_str = str(msg.error())
                if "UNKNOWN_TOPIC_OR_PART" in error_str:
                    print(f"[BRAIN] Topic {KAFKA_TOPIC_STATS} not available yet, waiting...")
                    time.sleep(5)
                    continue
                print(f"[ERROR] Kafka Consumer: {msg.error()}")
                continue
                
            try:
                stats_json = json.loads(msg.value().decode('utf-8'))
                
                total_events = stats_json.get('total_events', 0)
                recent_sequence = stats_json.get('recent_access_sequence', [])
                
                cluster_stats = stats_json.get("cluster_stats", {})
                print(f"[BRAIN] Received stats snapshot. Total events: {total_events}, Clusters in stats: {len(cluster_stats) if cluster_stats else 0}")
                if cluster_stats:
                    sample_cluster_ids = list(cluster_stats.keys())[:10]
                    print(f"[BRAIN] Sample cluster IDs from streamer: {sample_cluster_ids}")
                
                cache_full = 50.0 
                try:
                    r = get_redis_aura()
                    info = r.info("memory")
                    used = info.get("used_memory", 0)
                    cache_full = (used / AURA_CACHE_LIMIT_BYTES) * 100.0
                    cache_full = min(100.0, max(0.0, cache_full))
                except Exception as e:
                    print(f"[WARNING] Failed to query Redis memory: {e}. Defaulting to 50%")
                
                print(f"[BRAIN] Current Cache Usage: {cache_full:.1f}%")

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
                    print(f"[BRAIN] DEBUG - Baseline: hits={baseline_hits}, misses={baseline_misses}, HR={baseline_hr_str}, delta={baseline_delta_str}")
                    print(f"[BRAIN] DEBUG - Aura: hits={aura_hits}, misses={aura_misses}, HR={aura_hr_str}, delta={aura_delta_str}")
                
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

                        print(f"[BRAIN_DEBUG] WindowHits[Aura: {aura_hits}, Base: {baseline_hits}] -> DeltaHR[Aura: {delta_aura_hr:.6f}, Base: {delta_baseline_hr:.6f}] -> OppLoss: {opp_loss:.6f}")

                        if abs(opp_loss) < 0.0001:
                            print(f"[BRAIN] ⚠️ WARNING: Opportunity loss is near zero ({opp_loss:.6f}). This may indicate:")
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
                            print(f"[BRAIN] ⚠️ WARNING: Opportunity loss is near zero ({opp_loss:.6f}) using absolute values")
                    
                    print(f"[BRAIN] Adaptation State: {current_adaptation_state.value} (volatility={workload_volatility:.3f}{opp_loss_str})")
                
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
                                print(f"[BRAIN] 🏆 New Best Strategy: Improvement={improvement:.4f}, Volatility={workload_volatility:.3f}, Params={params_applied}")
                        elif best_strategy and (current_time - best_strategy.get("timestamp", 0)) >= 3600:
                            best_strategy = None

                    snapshot_history.append(history_entry)
                    if len(snapshot_history) > MAX_SNAPSHOT_HISTORY:
                        snapshot_history.pop(0)

                    history_copy = snapshot_history.copy()

                with best_strategy_lock:
                    best_strategy_copy = best_strategy.copy() if best_strategy else None
                
                if not USE_GROQ and _is_small_model(LLM_MODEL):
                    prompt = build_global_prompt_small(stats_json, last_policy_copy, metrics_feedback)
                    print(f"[BRAIN] Using SMALL prompt for model: {LLM_MODEL}")
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
                
                llm_queue.put({
                    "prompt": prompt, 
                    "mode": "tinylfu_parameter_update"
                })
                
            except Exception as e:
                print(f"[ERROR] Brain Loop Error: {e}")
                import traceback
                traceback.print_exc()
                
    finally:
        consumer.close()
        print("[BRAIN] Shutdown.")

if __name__ == "__main__":
    run_brain()

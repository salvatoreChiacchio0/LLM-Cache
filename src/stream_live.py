import csv, os, redis, time, requests, pymongo, json, re
from collections import defaultdict, deque, Counter as CounterCollection
from threading import Lock, Thread
from queue import Queue, Empty
from functools import lru_cache
from confluent_kafka import Producer
from pymilvus import Collection, connections, MilvusException
from prometheus_client import start_http_server, Counter, Gauge

CHARSET = "utf-8"
LOG_FILE = "/app/data/log_15M_subset.txt"
LOG_DELIMITER = chr(1)
FILE_ENCODING = "latin-1"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
LLM_API_URL = "http://ollama:11434/api/generate"
LLM_MODEL = "llama3:8b"
USE_GROQ = bool(GROQ_API_KEY)
KAFKA_TOPIC = "aura-plan"
PRIORITY_TTL_MULT = 3.0
EVICT_TTL_MULT = 0.1
TTL_MIN_SECONDS = 60
TTL_MAX_SECONDS = 3600
GLOBAL_TTL_FACTOR_MIN = 0.2
GLOBAL_TTL_FACTOR_MAX = 3.0
AURA_CACHE_LIMIT_BYTES = 10_000_000.0
LRU_CACHE_LIMIT_BYTES = 10 * 1024 * 1024
MEMORY_REFRESH_INTERVAL_SEC = 1.0
TTL_TIME_COMPRESSION = float(os.getenv("AURA_TTL_TIME_COMPRESSION", "1.0"))
current_policy = {
    "global_ttl_factor": 1.0,
    "evict_categories": [],
    "priority_categories": [],
    "reasoning": "",
}
policy_lock = Lock()

time.sleep(1)

conf = {
    'bootstrap.servers': 'kafka:29092', 
    'client.id': 'llm-producer',
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.kbytes': 1048576,
    'batch.num.messages': 100,
    'linger.ms': 10,
    'compression.type': 'none',
    'acks': 'all',
    'retries': 3,
    'max.in.flight.requests.per.connection': 1,
    'enable.idempotence': True,
}
kafka_producer = Producer(conf)
kafka_delivery_failures = 0
kafka_delivery_successes = 0

print("[DEBUG] Connecting to Redis...")
r = redis.Redis(host="redis-aura", port=6379, encoding=CHARSET, protocol=2)
r_lru = redis.Redis(host="redis-lru", port=6380, encoding=CHARSET, protocol=2)
r.ping()
r_lru.ping()
print("[DEBUG] Redis connected successfully")

if TTL_TIME_COMPRESSION > 1.0:
    print(f"[DEBUG] Using TTL time compression factor: {TTL_TIME_COMPRESSION}")
if USE_GROQ:
    print(f"[DEBUG] Using Groq API with model: {GROQ_MODEL}")
    print("[DEBUG] Testing Groq API connection...")
    try:
        test_resp = requests.post(
            GROQ_API_URL,
            headers={
                "Authorization": f"Bearer {GROQ_API_KEY}",
                "Content-Type": "application/json",
            },
            json={
                "model": GROQ_MODEL,
                "messages": [{"role": "user", "content": "Say 'OK'"}],
                "temperature": 0.9,
                "max_tokens": 1024,
            },
            timeout=10,
        )
        test_resp.raise_for_status()
        print(f"[DEBUG] Groq API test successful (status: {test_resp.status_code})")
    except Exception as e:
        print(f"[ERROR] Groq API test failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[ERROR] Response status: {e.response.status_code}")
            print(f"[ERROR] Response body: {e.response.text[:500]}")
        print("[ERROR] Continuing anyway, but Groq calls may fail...")
else:
    print(f"[DEBUG] Using local Ollama with model: {LLM_MODEL}")

print("[DEBUG] Flushing Redis databases...")
try:
    r.flushdb()
    r_lru.flushdb()
    print("[DEBUG] Redis databases flushed")
except Exception as e:
    print(f"[DEBUG] Error flushing Redis: {e}")

_last_aura_mem = 0
_last_aura_mem_ts = 0.0
_last_lru_mem = 0
_last_lru_mem_ts = 0.0


def get_aura_cache_full_percent():
    global _last_aura_mem, _last_aura_mem_ts
    now = time.time()
    if _last_aura_mem_ts == 0.0 or now - _last_aura_mem_ts >= MEMORY_REFRESH_INTERVAL_SEC:
        try:
            _last_aura_mem = r.info("memory")["used_memory"]
        except Exception:
            _last_aura_mem = 0
        _last_aura_mem_ts = now
    return round((_last_aura_mem / AURA_CACHE_LIMIT_BYTES) * 100, 1)


def get_lru_cache_full_percent():
    global _last_lru_mem, _last_lru_mem_ts
    now = time.time()
    if _last_lru_mem_ts == 0.0 or now - _last_lru_mem_ts >= MEMORY_REFRESH_INTERVAL_SEC:
        try:
            _last_lru_mem = r_lru.info("memory")["used_memory"]
        except Exception:
            _last_lru_mem = 0
        _last_lru_mem_ts = now
    return round((_last_lru_mem / LRU_CACHE_LIMIT_BYTES) * 100, 1)

print("[DEBUG] Starting Prometheus metrics server...")
start_http_server(8000)
print("[DEBUG] Prometheus metrics server started on port 8000")
CACHE_MISSES = Counter("cache_misses", "Cache Misses Count (Aura)")
CACHE_HITS = Counter("cache_hits", "Cache Hits Count (Aura)")
LRU_HITS = Counter("lru_hits", "Cache Hits Count (LRU Baseline)")
LRU_MISSES = Counter("lru_misses", "Cache Misses Count (LRU Baseline)")
LATENCY_MS = Gauge("llm_reasoning_latency_ms", "LLM Response Latency in milliseconds")
RETROACTIVE_EVICTIONS = Counter("retroactive_evictions_total", "Items evicted retroactively by policy")
RETROACTIVE_EXTENSIONS = Counter("retroactive_ttl_extensions_total", "TTLs extended retroactively by policy")
RETROACTIVE_UPDATES = Counter("retroactive_ttl_updates_total", "TTLs updated retroactively by policy")
POLICY_APPLICATIONS = Counter("policy_applications_total", "Total policy applications")

print("[DEBUG] Connecting to MongoDB...")
mongo = pymongo.MongoClient("mongodb://mongo:27017").aura
print("[DEBUG] MongoDB connected")

_item_metadata_cache = {}
_item_metadata_cache_lock = Lock()
ITEM_METADATA_CACHE_SIZE = 10000
_category_cache = {}  # Cache per categorie: item_id -> category
_category_cache_lock = Lock()
CATEGORY_CACHE_SIZE = 50000

def get_item_metadata(item_id, fields=None):
    global _item_metadata_cache
    if fields is None:
        fields = {"category": 1}
    
    cache_key = (item_id, tuple(sorted(fields.keys())))
    
    with _item_metadata_cache_lock:
        if cache_key in _item_metadata_cache:
            return _item_metadata_cache[cache_key]
    
    try:
        doc = mongo.catalog.find_one({"item_id": item_id}, fields)
        result = doc if doc else {}
        
        with _item_metadata_cache_lock:
            if len(_item_metadata_cache) >= ITEM_METADATA_CACHE_SIZE:
                _item_metadata_cache.clear()
            _item_metadata_cache[cache_key] = result
        
        # Aggiorna anche category cache se category è richiesta
        if "category" in fields and "category" in result:
            with _category_cache_lock:
                if len(_category_cache) >= CATEGORY_CACHE_SIZE:
                    _category_cache.clear()
                _category_cache[item_id] = result.get("category")
        
        return result
    except Exception as e:
        print(f"[WARNING] MongoDB query failed for item_id={item_id}: {e}")
        return {}

def get_item_category_cached(item_id):
    """Ottiene la categoria di un item usando la cache, evitando query MongoDB se possibile."""
    with _category_cache_lock:
        if item_id in _category_cache:
            return _category_cache[item_id]
    
    # Se non in cache, usa get_item_metadata che aggiornerà anche la cache
    doc = get_item_metadata(item_id, {"category": 1})
    category = doc.get("category") if doc else None
    
    if category:
        with _category_cache_lock:
            if len(_category_cache) >= CATEGORY_CACHE_SIZE:
                _category_cache.clear()
            _category_cache[item_id] = category
    
    return category

print("[DEBUG] Connecting to Milvus...")
connections.connect("default", host="milvus", port=19530)
print("[DEBUG] Milvus connected")

try:
    coll = Collection("catalog")
    coll.load()
    print("[DEBUG] Milvus collection 'catalog' loaded")
except MilvusException as e:
    print(f"[ERROR] Failed to load Milvus collection: {e}")
    exit()

def get_item_embedding(item_id):
    try:
        results = coll.query(
            expr=f"item_id == {item_id}",
            output_fields=["embedding"],
            limit=1
        )
        if results and len(results) > 0:
            embedding = results[0].get("embedding")
            if embedding is not None:
                return embedding
    except Exception as e:
        print(f"[WARNING] Failed to get embedding for item_id={item_id}: {e}")
    return None


def estimate_ttl_from_neighbors(item_id, k=5):
    try:
        embedding = get_item_embedding(item_id)
        if embedding is None:
            return None
        
        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        results = coll.search(
            data=[embedding],
            anns_field="embedding",
            param=search_params,
            limit=k+1,
            output_fields=["item_id"]
        )
        
        if not results or len(results) == 0:
            return None
        
        neighbor_ids = []
        for hit in results[0]:
            neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
            if neighbor_id is None and hasattr(hit, 'id'):
                neighbor_id = hit.id
            if neighbor_id is not None and neighbor_id != item_id:
                neighbor_ids.append(neighbor_id)
        
        if not neighbor_ids:
            return None
        
        neighbor_ttls = []
        for neighbor_id in neighbor_ids:
            neighbor_key = f"item:{neighbor_id}"
            try:
                ttl = r.ttl(neighbor_key)
                if ttl > 0:
                    neighbor_ttls.append(ttl)
            except Exception:
                continue
        
        if neighbor_ttls:
            avg_ttl = int(sum(neighbor_ttls) / len(neighbor_ttls))
            return avg_ttl
    except Exception as e:
        print(f"[WARNING] Failed to estimate TTL from neighbors for item_id={item_id}: {e}")
    return None


def compute_semantic_novelty(item_ids, k=5):
    if not item_ids:
        return None
    
    novelty_scores = []
    for item_id in item_ids:
        try:
            embedding = get_item_embedding(item_id)
            if embedding is None:
                continue
            
            search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            results = coll.search(
                data=[embedding],
                anns_field="embedding",
                param=search_params,
                limit=k+1,
                output_fields=["item_id"]
            )
            
            if not results or len(results) == 0:
                continue
            
            max_similarity = 0.0
            for hit in results[0]:
                neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
                if neighbor_id is None and hasattr(hit, 'id'):
                    neighbor_id = hit.id
                if neighbor_id is not None and neighbor_id != item_id:
                    distance = hit.distance
                    similarity = 1.0 / (1.0 + distance)
                    max_similarity = max(max_similarity, similarity)
                    break
            
            if max_similarity > 0:
                novelty = 1.0 - max_similarity
                novelty_scores.append(novelty)
        except Exception as e:
            print(f"[WARNING] Failed to compute novelty for item_id={item_id}: {e}")
            continue
    
    if novelty_scores:
        return sum(novelty_scores) / len(novelty_scores)
    return None


def compute_semantic_redundancy(item_ids, k=5):
    if not item_ids:
        return None
    
    redundancy_scores = []
    for item_id in item_ids:
        try:
            embedding = get_item_embedding(item_id)
            if embedding is None:
                continue
            
            search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            results = coll.search(
                data=[embedding],
                anns_field="embedding",
                param=search_params,
                limit=k+1,
                output_fields=["item_id"]
            )
            
            if not results or len(results) == 0:
                continue
            
            max_similarity = 0.0
            for hit in results[0]:
                neighbor_id = hit.entity.get("item_id")
                if neighbor_id != item_id:
                    distance = hit.distance
                    similarity = 1.0 / (1.0 + distance)
                    max_similarity = max(max_similarity, similarity)
                    break
            
            if max_similarity > 0:
                redundancy_scores.append(max_similarity)
        except Exception as e:
            print(f"[WARNING] Failed to compute redundancy for item_id={item_id}: {e}")
            continue
    
    if redundancy_scores:
        return sum(redundancy_scores) / len(redundancy_scores)
    return None


def fix_json_common_errors(json_str):
    import re
    pattern = r'("(?:\w+)":)([a-zA-Z]\w*)'

    def fix_match(match):
        key_part = match.group(1)
        value = match.group(2)
        if value.lower() not in ['true', 'false', 'null']:
            return key_part + '"' + value + '"'
        return match.group(0)

    json_str = re.sub(pattern, fix_match, json_str)
    return json_str


class ContextAggregator:
    def __init__(self, max_events=1000, session_history_len=10, window_seconds=None):
        self.max_events = max_events
        self.window_seconds = window_seconds
        self.events = deque(maxlen=max_events)
        self.user_sessions = defaultdict(lambda: deque(maxlen=session_history_len))
        self.total_events = 0
        self.previous_snapshot = None

    def record_event(self, user, item_id, action, category, aura_hit, timestamp=None):
        ts_val = None
        if timestamp is not None:
            try:
                ts_val = float(timestamp)
            except Exception:
                ts_val = None
        self.events.append(
            {
                "user": user,
                "item_id": item_id,
                "action": action,
                "category": category,
                "aura_hit": aura_hit,
                "ts": ts_val,
            }
        )
        self.user_sessions[user].append(
            {"item_id": item_id, "action": action, "ts": ts_val}
        )
        self.total_events = len(self.events)

    def _prune_old_events(self, now_ts):
        if self.window_seconds is None or now_ts is None:
            return
        cutoff = now_ts - self.window_seconds
        while self.events and self.events[0]["ts"] is not None and self.events[0]["ts"] < cutoff:
            self.events.popleft()
        self.total_events = len(self.events)

    def build_snapshot(self, now_ts=None, max_sessions=5, events_per_session=5):
        self._prune_old_events(now_ts)
        item_counts = CounterCollection()
        category_counts = CounterCollection()
        category_misses = CounterCollection()
        aura_hits = 0
        aura_misses = 0
        for e in self.events:
            item_counts[e["item_id"]] += 1
            if e["category"] is not None:
                category_counts[e["category"]] += 1
                if not e["aura_hit"]:
                    category_misses[e["category"]] += 1
            if e["aura_hit"]:
                aura_hits += 1
            else:
                aura_misses += 1
        total_aura = aura_hits + aura_misses
        aura_ratio = aura_hits / total_aura if total_aura > 0 else 0.0
        top_items = item_counts.most_common(10)
        top_categories = category_counts.most_common(5)
        sessions_data = []
        for user, events in list(self.user_sessions.items())[:max_sessions]:
            recent = list(events)[-events_per_session:]
            sessions_data.append(
                {
                    "user": user,
                    "events": [
                        {"item_id": e["item_id"], "action": e["action"], "ts": e["ts"]}
                        for e in recent
                    ],
                }
            )
        items_meta = []
        for item_id, count in top_items:
            doc = get_item_metadata(item_id, {"title": 1, "category": 1, "brand": 1})
            if doc:
                items_meta.append(
                    {
                        "item_id": item_id,
                        "count": count,
                        "title": doc.get("title"),
                        "category": doc.get("category"),
                        "brand": doc.get("brand"),
                    }
                )
            else:
                items_meta.append(
                    {
                        "item_id": item_id,
                        "count": count,
                        "title": None,
                        "category": None,
                        "brand": None,
                    }
                )
        top_item_ids = [item["item_id"] for item in items_meta]
        semantic_novelty_score = compute_semantic_novelty(top_item_ids, k=5)
        semantic_redundancy_score = compute_semantic_redundancy(top_item_ids, k=5)
        
        snapshot = {
            "total_events": len(self.events),
            "aura_hit_ratio": aura_ratio,
            "top_items": items_meta,
            "top_categories": [{"category": c, "count": n} for c, n in top_categories],
            "miss_categories": [{"category": c, "count": n} for c, n in category_misses.most_common(5)],
            "sample_sessions": sessions_data,
            "semantic_novelty_score": semantic_novelty_score,
            "semantic_redundancy_score": semantic_redundancy_score,
        }
        
        delta = None
        if self.previous_snapshot is not None:
            prev_ratio = self.previous_snapshot.get("aura_hit_ratio", 0.0)
            delta = {
                "hit_ratio_change": aura_ratio - prev_ratio,
                "hit_ratio_previous": prev_ratio,
                "events_change": snapshot["total_events"] - self.previous_snapshot.get("total_events", 0),
            }
        
        self.previous_snapshot = snapshot.copy()
        snapshot["delta"] = delta
        
        return snapshot

    def reset_window(self):
        return


def build_global_prompt(snapshot, cache_full, previous_policy=None):
    lines = []
    lines.append(
        "TASK: Generate a high-level cache policy as JSON based on recent traffic and Aura cache state."
    )
    lines.append("")
    lines.append("CONTEXT:")
    lines.append(f"- Cache usage percent: {cache_full}")
    lines.append(f"- Events in window: {snapshot['total_events']}")
    lines.append(f"- Aura hit ratio: {snapshot['aura_hit_ratio']:.3f}")
    
    if snapshot.get("delta") is not None:
        delta = snapshot["delta"]
        lines.append("")
        lines.append("PERFORMANCE DELTA (since last policy):")
        lines.append(f"- Hit ratio changed from {delta['hit_ratio_previous']:.3f} to {snapshot['aura_hit_ratio']:.3f} (delta: {delta['hit_ratio_change']:+.3f})")
        if delta['hit_ratio_change'] < -0.05:
            lines.append("  WARNING: Hit ratio decreased significantly - previous policy may need adjustment")
        elif delta['hit_ratio_change'] > 0.05:
            lines.append("  NOTE: Hit ratio improved - current policy direction seems effective")
    
    if previous_policy:
        lines.append("")
        lines.append("PREVIOUS POLICY:")
        lines.append(f"- global_ttl_factor: {previous_policy.get('global_ttl_factor', 1.0)}")
        lines.append(f"- priority_categories: {previous_policy.get('priority_categories', [])}")
        lines.append(f"- evict_categories: {previous_policy.get('evict_categories', [])}")
    lines.append("- Top items by requests (with metadata):")
    for entry in snapshot["top_items"]:
        title = entry["title"] or ""
        category = entry["category"] or ""
        brand = entry["brand"] or ""
        lines.append(
            f"  - item_id={entry['item_id']}, count={entry['count']}, "
            f"title=\"{title}\", category=\"{category}\", brand=\"{brand}\""
        )
    lines.append("- Top categories by requests:")
    for cat in snapshot["top_categories"]:
        lines.append(f"  - category=\"{cat['category']}\", count={cat['count']}")
    lines.append("- Categories with highest CACHE MISS count:")
    if snapshot.get("miss_categories"):
        for cat in snapshot["miss_categories"]:
            lines.append(f"  - category=\"{cat['category']}\", misses={cat['count']}")
    else:
        lines.append("  - No miss data available.")
    lines.append("- Sample user sessions (most recent events):")
    for sess in snapshot["sample_sessions"]:
        events_repr = ", ".join(
            f"{e['action']}:{e['item_id']}" for e in sess["events"]
        )
        lines.append(f"  - user={sess['user']}: {events_repr}")
    
    lines.append("")
    lines.append("SEMANTIC SIGNALS (from Milvus):")
    novelty_score = snapshot.get("semantic_novelty_score")
    redundancy_score = snapshot.get("semantic_redundancy_score")
    if novelty_score is not None:
        lines.append(f"- Semantic novelty score: {novelty_score:.3f} (higher = more diverse items, lower = more similar items)")
    else:
        lines.append("- Semantic novelty score: N/A (computation unavailable)")
    if redundancy_score is not None:
        lines.append(f"- Semantic redundancy score: {redundancy_score:.3f} (higher = more redundant/similar items, lower = more diverse items)")
    else:
        lines.append("- Semantic redundancy score: N/A (computation unavailable)")
    
    lines.append("")
    lines.append("INSTRUCTIONS:")
    lines.append("- Focus only on managing the Aura cache (conceptual policy, not specific item IDs).")
    lines.append(
        "- You MUST return a single JSON object with exactly these top-level keys: "
        "global_ttl_factor, evict_categories, priority_categories, reasoning."
    )
    lines.append(
        "- global_ttl_factor: float in range [0.2, 3.0]; values < 1.0 make TTLs shorter, values > 1.0 make TTLs longer."
    )
    lines.append(
        "- evict_categories: array of unique category names (strings) to de-prioritize (shorter TTL)."
    )
    lines.append(
        "- priority_categories: array of unique category names (strings) to protect (longer TTL)."
    )
    lines.append(
        "- reasoning: short natural language string (max ~2 sentences) explaining the choice."
    )
    lines.append("- Do NOT include item IDs, user IDs, or per-item policies.")
    lines.append("")
    lines.append("CRITICAL: OUTPUT FORMAT REQUIREMENTS:")
    lines.append(
        "- You MUST output ONLY a valid JSON object. No explanations, no reasoning, no markdown, no code fences."
    )
    lines.append("- Your response must start with '{' and end with '}'.")
    lines.append("- Do NOT include any text before or after the JSON object.")
    lines.append("- Do NOT wrap the JSON in backticks, code blocks, or any other formatting.")
    lines.append("- Use double quotes for all JSON keys and string values.")
    lines.append("- The JSON must be syntactically valid and parseable by a standard JSON parser.")
    lines.append("")
    lines.append("REQUIRED OUTPUT FORMAT (copy this structure exactly, adapt values to context):")
    lines.append(
        '{"global_ttl_factor": 1.0, "evict_categories": ["books"], "priority_categories": ["smartphone"], "reasoning": "short explanation"}'
    )
    lines.append("")
    lines.append("YOUR RESPONSE MUST BE ONLY THIS JSON OBJECT, NOTHING ELSE:")
    return "\n".join(lines)


def call_llm_and_send_plan(prompt):
    t0 = time.time()
    plan = None
    plan_valid = False
    try:
        if USE_GROQ:
            print("[DEBUG] Calling Groq API...")
            try:
                resp = requests.post(
                    GROQ_API_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": GROQ_MODEL,
                        "messages": [
                            {"role": "system", "content": "You are a JSON-only response generator. You must respond with ONLY valid JSON, no explanations, no reasoning, no markdown. Your response must start with '{' and end with '}'."},
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.7,
                        "max_tokens": 1024,
                    },
                    timeout=30,
                )
                print(f"[DEBUG] Groq API response status: {resp.status_code}")
                if resp.status_code == 429:
                    raise requests.exceptions.HTTPError("Rate limit reached", response=resp)
                resp.raise_for_status()
                plan = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "").strip()
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 429:
                    print("[WARNING] Groq rate limit reached, falling back to Ollama...")
                    raise
                raise
        else:
            resp = requests.post(
                LLM_API_URL,
                json={
                    "model": LLM_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.9,
                        "top_p": 0.9,
                        "num_predict": 256,
                    },
                },
                timeout=300,
            )
            plan = resp.json().get("response", "").strip()
        LATENCY_MS.set((time.time() - t0) * 1000)
        print("[DEBUG] Raw LLM response from LLM (full, first 1000 chars):")
        print(plan[:1000])
        if len(plan) > 1000:
            print(f"[DEBUG] ... (truncated, total length: {len(plan)} chars)")
        if plan.startswith("```"):
            lines = plan.split("\n")
            if lines[0].strip().startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            plan = "\n".join(lines).strip()
        # Try to find JSON in the response - be more aggressive
        json_start = plan.find("{")
        if json_start == -1:
            # Try to find JSON-like structure even if it's not at the start
            # Look for patterns like "{" or '{' or even just look for key-value pairs
            json_start = plan.find('"global_ttl_factor"')
            if json_start != -1:
                # Find the opening brace before this
                for i in range(json_start, -1, -1):
                    if plan[i] == "{":
                        json_start = i
                        break
        
        if json_start != -1:
            brace_count = 0
            json_end = -1
            for i in range(json_start, len(plan)):
                if plan[i] == "{":
                    brace_count += 1
                elif plan[i] == "}":
                    brace_count -= 1
                    if brace_count == 0:
                        json_end = i + 1
                        break
            if json_end != -1:
                plan = plan[json_start:json_end]
        else:
            # Last resort: try to extract JSON by looking for common patterns
            # Look for "global_ttl_factor" which should be in every valid response
            if '"global_ttl_factor"' in plan or "'global_ttl_factor'" in plan:
                # Try to find a JSON-like structure around this
                idx = plan.find('"global_ttl_factor"')
                if idx == -1:
                    idx = plan.find("'global_ttl_factor'")
                if idx != -1:
                    # Look backwards for opening brace
                    for i in range(max(0, idx - 50), idx):
                        if plan[i] == "{":
                            json_start = i
                            # Look forwards for closing brace
                            brace_count = 0
                            for j in range(json_start, min(len(plan), json_start + 500)):
                                if plan[j] == "{":
                                    brace_count += 1
                                elif plan[j] == "}":
                                    brace_count -= 1
                                    if brace_count == 0:
                                        plan = plan[json_start:j+1]
                                        break
                            break
        if plan and plan.startswith("{") and plan.endswith("}"):
            plan = fix_json_common_errors(plan)
        if plan:
            print("[DEBUG] Candidate LLM plan after JSON extraction/fix (truncated to 500 chars):")
            print(plan[:500])
        if plan and plan.startswith("{") and plan.endswith("}"):
            try:
                json.loads(plan)
                plan_valid = True
            except json.JSONDecodeError as e:
                print(
                    f"[DEBUG] JSON malformato (dopo fix): {plan[:100]}... (errore: {e.msg} alla posizione {e.pos})"
                )
        if not plan_valid:
            if not plan:
                print("[DEBUG] LLM ha ritornato risposta vuota")
            elif not plan.startswith("{"):
                print(
                    f"[DEBUG] Risposta LLM non inizia con {{: {plan[:200]}..."
                )
                # Try one more time to extract JSON with more aggressive methods
                if '"global_ttl_factor"' in plan or "'global_ttl_factor'" in plan:
                    print("[DEBUG] Tentativo di estrazione JSON da risposta malformata...")
                    # Try regex to find JSON object (with both single and double quotes)
                    json_match = re.search(r'\{[^{}]*["\']global_ttl_factor["\'][^{}]*\}', plan, re.DOTALL)
                    if json_match:
                        candidate = json_match.group(0)
                        # Try to expand to find complete JSON
                        start_idx = plan.find(candidate)
                        if start_idx != -1:
                            # Look for complete JSON structure
                            brace_start = plan.rfind('{', 0, start_idx + 1)
                            if brace_start != -1:
                                brace_count = 0
                                for i in range(brace_start, min(len(plan), brace_start + 1000)):
                                    if plan[i] == "{":
                                        brace_count += 1
                                    elif plan[i] == "}":
                                        brace_count -= 1
                                        if brace_count == 0:
                                            candidate = plan[brace_start:i+1]
                                            # Replace single quotes with double quotes for JSON
                                            candidate = candidate.replace("'", '"')
                                            try:
                                                json.loads(candidate)
                                                plan = candidate
                                                plan_valid = True
                                                print("[DEBUG] JSON estratto con successo tramite regex avanzata")
                                                break
                                            except:
                                                pass
                    # If still not valid, try to fix common issues
                    if not plan_valid and '"global_ttl_factor"' in plan:
                        # Try to manually construct JSON from found values
                        try:
                            # Extract values using regex
                            factor_match = re.search(r'["\']global_ttl_factor["\']\s*:\s*([0-9.]+)', plan)
                            priority_match = re.search(r'["\']priority_categories["\']\s*:\s*\[(.*?)\]', plan, re.DOTALL)
                            evict_match = re.search(r'["\']evict_categories["\']\s*:\s*\[(.*?)\]', plan, re.DOTALL)
                            
                            if factor_match:
                                factor = float(factor_match.group(1))
                                priority = []
                                evict = []
                                
                                if priority_match:
                                    priority_str = priority_match.group(1)
                                    priority = [p.strip().strip('"\'') for p in re.findall(r'["\']([^"\']+)["\']', priority_str)]
                                
                                if evict_match:
                                    evict_str = evict_match.group(1)
                                    evict = [e.strip().strip('"\'') for e in re.findall(r'["\']([^"\']+)["\']', evict_str)]
                                
                                # Construct valid JSON
                                plan = json.dumps({
                                    "global_ttl_factor": factor,
                                    "priority_categories": priority,
                                    "evict_categories": evict,
                                    "reasoning": "Extracted from malformed response"
                                })
                                plan_valid = True
                                print("[DEBUG] JSON ricostruito manualmente da valori estratti")
                        except Exception as e:
                            print(f"[DEBUG] Fallito tentativo di ricostruzione JSON: {e}")
            elif not plan.endswith("}"):
                print(
                    f"[DEBUG] JSON incompleto (non finisce con }}): {plan[:200]}..."
                )

        else:
            print("[DEBUG] Final LLM plan JSON (truncated to 500 chars):")
            print(plan)
            policy_data = None
            try:
                policy_data = json.loads(plan)
            except json.JSONDecodeError:
                policy_data = None
            if isinstance(policy_data, dict):
                raw_factor = policy_data.get("global_ttl_factor", 1.0)
                try:
                    factor = float(raw_factor)
                    if not (GLOBAL_TTL_FACTOR_MIN <= factor <= GLOBAL_TTL_FACTOR_MAX):
                        print(f"[WARNING] LLM returned global_ttl_factor={factor}, clamping to [{GLOBAL_TTL_FACTOR_MIN}, {GLOBAL_TTL_FACTOR_MAX}]")
                        factor = max(GLOBAL_TTL_FACTOR_MIN, min(factor, GLOBAL_TTL_FACTOR_MAX))
                except (TypeError, ValueError) as e:
                    print(f"[WARNING] Invalid global_ttl_factor from LLM: {raw_factor}, using default 1.0")
                    factor = 1.0
                
                if factor < GLOBAL_TTL_FACTOR_MIN:
                    print(f"[ERROR] Factor {factor} below minimum {GLOBAL_TTL_FACTOR_MIN}, clamping")
                    factor = GLOBAL_TTL_FACTOR_MIN
                elif factor > GLOBAL_TTL_FACTOR_MAX:
                    print(f"[ERROR] Factor {factor} above maximum {GLOBAL_TTL_FACTOR_MAX}, clamping")
                    factor = GLOBAL_TTL_FACTOR_MAX

                raw_priority = policy_data.get("priority_categories", [])
                if isinstance(raw_priority, list):
                    priority_categories = [
                        c.strip()
                        for c in raw_priority
                        if isinstance(c, str) and c.strip()
                    ]
                else:
                    priority_categories = []

                raw_evict = policy_data.get("evict_categories", [])
                if isinstance(raw_evict, list):
                    evict_categories = [
                        c.strip()
                        for c in raw_evict
                        if isinstance(c, str) and c.strip()
                    ]
                else:
                    evict_categories = []

                reasoning = policy_data.get("reasoning", "")
                if not isinstance(reasoning, str):
                    reasoning = str(reasoning)

                with policy_lock:
                    old_factor = current_policy.get("global_ttl_factor", 1.0)
                    old_priority = current_policy.get("priority_categories", [])
                    old_evict = current_policy.get("evict_categories", [])
                    
                    current_policy["global_ttl_factor"] = factor
                    current_policy["priority_categories"] = priority_categories
                    current_policy["evict_categories"] = evict_categories
                    current_policy["reasoning"] = reasoning
                
                # Applica retroattivamente la nuova policy
                POLICY_APPLICATIONS.inc()
                print(f"[POLICY] New policy applied: factor={factor:.2f}, priority={priority_categories}, evict={evict_categories}")
                print(f"[POLICY] Reasoning: {reasoning}")
                
                # Applica retroattivamente solo se la policy è cambiata significativamente
                policy_changed = (
                    abs(factor - old_factor) > 0.1 or
                    set(priority_categories) != set(old_priority) or
                    set(evict_categories) != set(old_evict)
                )
                
                if policy_changed:
                    apply_policy_retroactively(factor, priority_categories, evict_categories, max_keys_to_scan=500)
            try:
                kafka_producer.produce(
                    topic=KAFKA_TOPIC,
                    value=plan.encode("utf-8"),
                    callback=delivery_report,
                )
                kafka_producer.poll(0)
                remaining = kafka_producer.flush(timeout=5)
                if remaining > 0:
                    print(f"[WARNING] {remaining} messages still in Kafka buffer after flush")
                print(f"[DEBUG] Piano inviato a Kafka: {plan[:100]}...")
            except BufferError as e:
                print(f"[ERROR] Kafka producer buffer full, waiting and retrying: {e}")
                kafka_producer.poll(10)
                try:
                    kafka_producer.produce(
                        topic=KAFKA_TOPIC,
                        value=plan.encode("utf-8"),
                        callback=delivery_report,
                    )
                    kafka_producer.poll(0)
                    kafka_producer.flush(timeout=5)
                    print("[DEBUG] Piano inviato a Kafka (retry after buffer flush)")
                except Exception as e2:
                    print(f"[ERROR] Kafka retry failed: {e2}")
            except Exception as e:
                error_str = str(e).lower()
                if "unknown topic" in error_str or "not available" in error_str:
                    time.sleep(1)
                    try:
                        kafka_producer.produce(
                            topic=KAFKA_TOPIC,
                            value=plan.encode("utf-8"),
                            callback=delivery_report,
                        )
                        kafka_producer.poll(0)
                        kafka_producer.flush(timeout=5)
                        print("[DEBUG] Piano inviato a Kafka (retry)")
                    except Exception as e2:
                        print(f"[ERROR] Kafka (retry): Impossibile inviare il piano: {e2}")
                else:
                    print(f"[ERROR] Kafka: Impossibile inviare il piano: {e}")
    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Groq API request failed: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"[ERROR] Response status: {e.response.status_code}")
            print(f"[ERROR] Response body: {e.response.text[:200]}")
            if e.response.status_code == 429:
                print("[WARNING] Rate limit reached for Groq, falling back to Ollama...")
                try:
                    resp = requests.post(
                        LLM_API_URL,
                        json={
                            "model": LLM_MODEL,
                            "prompt": prompt,
                            "stream": False,
                            "options": {
                                "temperature": 0.7,
                                "top_p": 0.9,
                                "num_predict": 512,
                            },
                        },
                        timeout=300,
                    )
                    plan = resp.json().get("response", "").strip()
                    if plan:
                        plan_valid = True
                        print("[DEBUG] Successfully used Ollama as fallback")
                except Exception as e2:
                    print(f"[ERROR] Ollama fallback also failed: {e2}")
    except Exception as e:
        print(f"[ERROR] Unexpected error in LLM call: {e}")
        import traceback
        traceback.print_exc()
    return plan if plan_valid else None

def apply_policy_retroactively(factor, priority_cats, evict_cats, max_keys_to_scan=1000):
    """
    Applica retroattivamente una policy agli elementi già in cache.
    Scansiona le chiavi Redis e applica evictions/estensioni TTL basate sulla policy.
    """
    evicted_count = 0
    extended_count = 0
    updated_count = 0
    
    try:
        # Usa SCAN invece di KEYS per evitare di bloccare Redis
        cursor = 0
        keys_scanned = 0
        
        while keys_scanned < max_keys_to_scan:
            cursor, keys = r.scan(cursor, match="item:*", count=100)
            if not keys:
                break
            
            pipe = r.pipeline()
            keys_to_check = []
            
            for key in keys:
                if keys_scanned >= max_keys_to_scan:
                    break
                keys_to_check.append(key)
                keys_scanned += 1
            
            # Ottieni TTL per tutte le chiavi in batch
            for key in keys_to_check:
                pipe.ttl(key)
            
            ttls = pipe.execute()
            
            for i, key in enumerate(keys_to_check):
                if i >= len(ttls):
                    continue
                    
                ttl = ttls[i]
                if ttl <= 0:  # Chiave non esiste o senza TTL
                    continue
                
                # Estrai item_id dalla chiave
                try:
                    item_id_str = key.decode('utf-8').replace('item:', '')
                    item_id = int(item_id_str)
                except (ValueError, AttributeError):
                    continue
                
                # Ottieni categoria (usa cache per performance)
                category = get_item_category_cached(item_id)
                
                # Applica policy
                if isinstance(category, str) and category in evict_cats:
                    # Evict: elimina la chiave
                    try:
                        r.delete(key)
                        evicted_count += 1
                        RETROACTIVE_EVICTIONS.inc()
                    except Exception as e:
                        print(f"[WARNING] Failed to evict {key}: {e}")
                
                elif isinstance(category, str) and category in priority_cats:
                    # Priority: estendi TTL
                    try:
                        new_ttl = int(ttl * PRIORITY_TTL_MULT)
                        new_ttl = max(TTL_MIN_SECONDS, min(new_ttl, TTL_MAX_SECONDS))
                        if TTL_TIME_COMPRESSION > 1.0:
                            new_ttl = max(1, int(new_ttl / TTL_TIME_COMPRESSION))
                        r.expire(key, new_ttl)
                        extended_count += 1
                        RETROACTIVE_EXTENSIONS.inc()
                    except Exception as e:
                        print(f"[WARNING] Failed to extend TTL for {key}: {e}")
                
                elif factor != 1.0:
                    # Applica global_ttl_factor
                    try:
                        new_ttl = int(ttl * factor)
                        new_ttl = max(TTL_MIN_SECONDS, min(new_ttl, TTL_MAX_SECONDS))
                        if TTL_TIME_COMPRESSION > 1.0:
                            new_ttl = max(1, int(new_ttl / TTL_TIME_COMPRESSION))
                        r.expire(key, new_ttl)
                        updated_count += 1
                        RETROACTIVE_UPDATES.inc()
                    except Exception as e:
                        print(f"[WARNING] Failed to update TTL for {key}: {e}")
            
            if cursor == 0:
                break
        
        print(f"[POLICY] Retroactive application: evicted={evicted_count}, extended={extended_count}, updated={updated_count}, scanned={keys_scanned}")
        
    except Exception as e:
        print(f"[ERROR] Failed to apply policy retroactively: {e}")
        import traceback
        traceback.print_exc()
    
    return evicted_count, extended_count, updated_count


def delivery_report(err, msg):
    global kafka_delivery_failures, kafka_delivery_successes
    if err is not None:
        kafka_delivery_failures += 1
        print(f'[ERROR] Errore nella consegna del messaggio a Kafka: {err}')
        print(f'[ERROR] Total Kafka delivery failures: {kafka_delivery_failures}')
    else:
        kafka_delivery_successes += 1
        if kafka_delivery_successes % 10 == 0:
            print(f'[SUCCESS] Messaggio consegnato a Kafka: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()} (total successes: {kafka_delivery_successes})')

llm_request_queue = Queue()
llm_worker_running = True

def llm_worker_thread():
    print("[DEBUG] LLM worker thread started")
    while llm_worker_running:
        try:
            prompt = llm_request_queue.get(timeout=1.0)
            if prompt is None:
                continue
            print("[DEBUG] LLM worker: processing request from queue")
            call_llm_and_send_plan(prompt)
            llm_request_queue.task_done()
        except Empty:
            continue
        except Exception as e:
            print(f"[ERROR] LLM worker thread error: {e}")
            import traceback
            traceback.print_exc()
            continue
    print("[DEBUG] LLM worker thread stopped")

llm_worker = Thread(target=llm_worker_thread, daemon=True)
llm_worker.start()
print("[DEBUG] LLM worker thread started in background")

print("[DEBUG] Initializing context aggregator...")
aggregator = ContextAggregator(max_events=1000, session_history_len=10, window_seconds=300)
PLAN_EVERY_N_EVENTS = 1500
events_since_last_plan = 0
count = 0
start_t = time.time()

print(f"[DEBUG] Opening log file: {LOG_FILE}")
import os
if not os.path.exists(LOG_FILE):
    print(f"[ERROR] Log file not found: {LOG_FILE}")
    exit(1)
with open(LOG_FILE, "r", encoding=FILE_ENCODING, errors="ignore") as f:
    print("[DEBUG] Log file opened, starting to process events...")
    reader = csv.reader(f, delimiter=LOG_DELIMITER)
    try:
        next(reader)
    except:
        pass

    for row in reader:
        if len(row) < 4:
            continue

        item, user, action = row[0], row[1], row[2]
        timestamp = row[3]
        try:
            item_id = int(item)
        except:
            continue

        item_key = f"item:{item_id}"

        if action != "click":
            continue

        time.sleep(0.01)
        lru_cache_full = get_lru_cache_full_percent()

        if lru_cache_full < 50:
            lru_ttl = 1800
        elif lru_cache_full < 80:
            lru_ttl = 900
        else:
            lru_ttl = 300

        lru_hit = bool(r_lru.get(item_key))
        if lru_hit:
            LRU_HITS.inc()
        else:
            LRU_MISSES.inc()
            effective_lru_ttl = lru_ttl
            if TTL_TIME_COMPRESSION > 1.0:
                effective_lru_ttl = max(1, int(lru_ttl / TTL_TIME_COMPRESSION))
            r_lru.setex(item_key, effective_lru_ttl, 1)

        doc = get_item_metadata(item_id, {"category": 1})
        category = doc.get("category") if doc else None

        aura_val = r.get(item_key)
        if aura_val is None:
            aura_hit = False
            CACHE_MISSES.inc()
            cache_full_item = get_aura_cache_full_percent()
            
            # Per Aura, usiamo una logica TTL leggermente diversa da LRU per testare meglio le policy
            neighbor_ttl = estimate_ttl_from_neighbors(item_id, k=5)
            if neighbor_ttl is not None and neighbor_ttl > 0:
                base_ttl = neighbor_ttl
            else:
                # TTL base per Aura: leggermente più conservativi per differenziare da LRU
                if cache_full_item < 50:
                    base_ttl = 2000  # Più lungo di LRU (1800) per testare policy
                elif cache_full_item < 80:
                    base_ttl = 1000  # Più lungo di LRU (900)
                else:
                    base_ttl = 400   # Più lungo di LRU (300)
            
            with policy_lock:
                factor = current_policy.get("global_ttl_factor", 1.0)
                priority_cats = current_policy.get("priority_categories", [])
                evict_cats = current_policy.get("evict_categories", [])

            if not isinstance(factor, (int, float)):
                factor = 1.0
            if factor < GLOBAL_TTL_FACTOR_MIN:
                factor = GLOBAL_TTL_FACTOR_MIN
            elif factor > GLOBAL_TTL_FACTOR_MAX:
                factor = GLOBAL_TTL_FACTOR_MAX

            ttl_val = int(base_ttl * factor)

            if isinstance(category, str):
                if category in priority_cats:
                    ttl_val = int(ttl_val * PRIORITY_TTL_MULT)
                elif category in evict_cats:
                    ttl_val = int(ttl_val * EVICT_TTL_MULT)

            if ttl_val < TTL_MIN_SECONDS:
                ttl_val = TTL_MIN_SECONDS
            if ttl_val > TTL_MAX_SECONDS:
                ttl_val = TTL_MAX_SECONDS

            effective_ttl = ttl_val
            if TTL_TIME_COMPRESSION > 1.0:
                effective_ttl = max(1, int(ttl_val / TTL_TIME_COMPRESSION))
            r.setex(item_key, effective_ttl, 1)
        else:
            aura_hit = True
            CACHE_HITS.inc()
        aggregator.record_event(
            user=user,
            item_id=item_id,
            action=action,
            category=category,
            aura_hit=aura_hit,
            timestamp=timestamp,
        )
        events_since_last_plan += 1

        if events_since_last_plan >= PLAN_EVERY_N_EVENTS and aggregator.total_events > 0:
            print(f"\n[DEBUG] Triggering LLM call: events_since_last_plan={events_since_last_plan}, total_events={aggregator.total_events}")
            cache_full_window = get_aura_cache_full_percent()
            try:
                now_ts = float(timestamp)
            except Exception:
                now_ts = None
            snapshot = aggregator.build_snapshot(now_ts=now_ts)
            
            # Mostra statistiche attuali dal snapshot
            aura_hit_rate = snapshot.get("aura_hit_ratio", 0.0) * 100
            print(f"[STATS] Before policy: Aura hit rate={aura_hit_rate:.2f}%, "
                  f"total_events={snapshot.get('total_events', 0)}, "
                  f"cache_full={cache_full_window:.1f}%")
            
            with policy_lock:
                previous_policy = current_policy.copy()
            prompt = build_global_prompt(snapshot, cache_full_window, previous_policy)
            print(f"[DEBUG] Built prompt (length: {len(prompt)} chars), queuing LLM request (non-blocking)...")

            llm_request_queue.put(prompt)
            events_since_last_plan = 0
            print(f"[DEBUG] LLM request queued, continuing event processing")
        elif count % 100 == 0:
            print(f"\n[DEBUG] Progress: events_since_last_plan={events_since_last_plan}/{PLAN_EVERY_N_EVENTS}, total_events={aggregator.total_events}")

        count += 1
kafka_producer.flush()
import csv, os, redis, time, requests, pymongo, json, re
from collections import defaultdict, deque, Counter as CounterCollection
from threading import Lock, Thread
from queue import Queue, Empty
from functools import lru_cache
from confluent_kafka import Producer
from pymilvus import Collection, connections, MilvusException
from prometheus_client import start_http_server, Counter, Gauge
from pydantic import BaseModel, Field, field_validator
from typing import List

CHARSET = "utf-8"
LOG_FILE = "/app/data/log_15M_subset.txt"
LOG_DELIMITER = chr(1)
FILE_ENCODING = "latin-1"
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
GROQ_MODEL = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
LLM_API_URL = "http://ollama:11434/api/generate"
LLM_MODEL = "meta-llama/llama-guard-4-12b"
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


class CachePolicy(BaseModel):
    """Schema per la policy di cache generata dall'LLM"""
    global_ttl_factor: float = Field(
        ge=GLOBAL_TTL_FACTOR_MIN,
        le=GLOBAL_TTL_FACTOR_MAX,
        description="Factor to multiply base TTL. Range [0.2, 3.0]"
    )
    evict_categories: List[str] = Field(
        default_factory=list,
        description="List of category names to de-prioritize (shorter TTL)"
    )
    priority_categories: List[str] = Field(
        default_factory=list,
        description="List of category names to protect (longer TTL)"
    )
    reasoning: str = Field(
        description="Short explanation of the policy choice (max ~2 sentences)"
    )
    
    @field_validator('evict_categories', 'priority_categories')
    @classmethod
    def validate_categories(cls, v):
        """Rimuove duplicati e stringhe vuote"""
        return list(dict.fromkeys(c.strip() for c in v if isinstance(c, str) and c.strip()))
    
    @classmethod
    def get_groq_schema(cls):
        """Genera lo schema JSON conforme ai requisiti di Groq Structured Outputs"""
        # Genera lo schema base da Pydantic
        schema = cls.model_json_schema()
        
        # Rimuovi campi non necessari per Groq
        schema.pop("$defs", None)
        schema.pop("title", None)
        schema.pop("description", None)
        
        # Groq richiede additionalProperties: false per tutti gli oggetti
        schema["additionalProperties"] = False
        
        # Assicurati che tutti i campi siano required
        if "properties" in schema:
            # Tutti i campi devono essere required per Groq Structured Outputs
            schema["required"] = list(schema["properties"].keys())
            
            # Pulisci le proprietà per rimuovere eventuali riferimenti a $defs
            for prop_name, prop_value in schema["properties"].items():
                if isinstance(prop_value, dict):
                    # Rimuovi title e description dalle proprietà (opzionale ma pulisce lo schema)
                    prop_value.pop("title", None)
                    prop_value.pop("description", None)
                    # Assicurati che gli array abbiano items definito correttamente
                    if prop_value.get("type") == "array":
                        items = prop_value.get("items", {})
                        if isinstance(items, dict):
                            items.pop("title", None)
                            items.pop("description", None)
        
        return schema
    
    class Config:
        json_schema_extra = {
            "example": {
                "global_ttl_factor": 1.0,
                "evict_categories": ["books"],
                "priority_categories": ["smartphone"],
                "reasoning": "Prioritizing high-value categories based on hit rate analysis"
            }
        }


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
        "TASK: Generate an AGGRESSIVE cache policy to MAXIMIZE hit ratio. Take BOLD actions based on recent traffic patterns."
    )
    lines.append("")
    lines.append("CRITICAL CONTEXT:")
    lines.append(f"- Cache usage percent: {cache_full}%")
    lines.append(f"- Events in window: {snapshot['total_events']}")
    lines.append(f"- Aura hit ratio: {snapshot['aura_hit_ratio']:.3f} ({snapshot['aura_hit_ratio']*100:.1f}%)")
    
    if snapshot.get("delta") is not None:
        delta = snapshot["delta"]
        lines.append("")
        lines.append("PERFORMANCE DELTA (since last policy):")
        lines.append(f"- Hit ratio changed from {delta['hit_ratio_previous']:.3f} to {snapshot['aura_hit_ratio']:.3f} (delta: {delta['hit_ratio_change']:+.3f})")
        if delta['hit_ratio_change'] < -0.05:
            lines.append("  ⚠️ CRITICAL: Hit ratio DECREASED significantly - PREVIOUS POLICY FAILED. Take IMMEDIATE corrective action!")
            lines.append("  ACTION REQUIRED: Aggressively adjust global_ttl_factor and category priorities NOW.")
        elif delta['hit_ratio_change'] > 0.05:
            lines.append("  ✅ Hit ratio improved - continue this direction but be ready to optimize further")
        elif abs(delta['hit_ratio_change']) < 0.01:
            lines.append("  ⚠️ STAGNATION DETECTED: Hit ratio barely changed. EXPLORE DIFFERENT strategy aggressively!")
    
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
    lines.append("AGGRESSIVE POLICY INSTRUCTIONS:")
    lines.append("")
    lines.append("🎯 PRIMARY GOAL: MAXIMIZE hit ratio through BOLD, DECISIVE actions.")
    lines.append("")
    lines.append("STRATEGY RULES:")
    lines.append("1. If hit ratio is LOW (<0.5): AGGRESSIVELY increase global_ttl_factor to 2.0-3.0 to keep more items cached")
    lines.append("2. If hit ratio is MEDIUM (0.5-0.7): Optimize category priorities - prioritize high-miss categories")
    lines.append("3. If hit ratio is HIGH (>0.7): Fine-tune but maintain aggressive protection of successful categories")
    lines.append("4. If cache is FULL (>80%): AGGRESSIVELY evict low-priority categories, protect high-value ones")
    lines.append("5. If cache is EMPTY (<50%): AGGRESSIVELY extend TTLs to fill cache with valuable items")
    lines.append("")
    lines.append("CATEGORY MANAGEMENT:")
    lines.append("- Categories with HIGH miss counts: IMMEDIATELY add to priority_categories (they need longer TTL)")
    lines.append("- Categories with LOW request counts but HIGH miss rate: AGGRESSIVELY evict (add to evict_categories)")
    lines.append("- Top requested categories: ALWAYS protect (add to priority_categories)")
    lines.append("")
    lines.append("PARAMETER GUIDELINES:")
    lines.append("- global_ttl_factor: Use EXTREME values when needed (0.2 for aggressive eviction, 3.0 for aggressive retention)")
    lines.append("  * If hit ratio dropped: INCREASE factor aggressively (1.5-2.5)")
    lines.append("  * If cache full and hit ratio low: DECREASE factor aggressively (0.3-0.5)")
    lines.append("  * If hit ratio stagnant: EXPLORE opposite direction (if was high, go low; if was low, go high)")
    lines.append("- evict_categories: Be AGGRESSIVE - list ALL underperforming categories (3-5 categories)")
    lines.append("- priority_categories: Be SELECTIVE but BOLD - protect top 2-4 high-value categories")
    lines.append("")
    lines.append("EXPLORATION MANDATE:")
    lines.append("- If hit ratio change is within ±0.01 for two consecutive windows: RADICALLY change strategy")
    lines.append("  * If previous factor was >1.0, try <0.5")
    lines.append("  * If previous factor was <1.0, try >2.0")
    lines.append("  * Completely swap priority and evict categories")
    lines.append("  * Take RISKS to find better policy")
    lines.append("")
    lines.append("TECHNICAL CONSTRAINTS:")
    lines.append("- global_ttl_factor: float in range [0.2, 3.0] - USE EXTREMES when needed")
    lines.append("- evict_categories: array of category name strings (be aggressive, 3-5 categories)")
    lines.append("- priority_categories: array of category name strings (be selective, 2-4 categories)")
    lines.append("- reasoning: Explain your AGGRESSIVE strategy choice (1-2 sentences)")
    lines.append("- Do NOT include item IDs, user IDs, or per-item policies")
    lines.append("")
    lines.append("⚠️ REMEMBER: Being CONSERVATIVE leads to STAGNATION. Be BOLD, take RISKS, MAXIMIZE performance!")    
    return "\n".join(lines)


def call_llm_and_send_plan(prompt):
    t0 = time.time()
    policy = None
    
    try:
        if USE_GROQ:
            print("[DEBUG] Calling Groq API with Structured Outputs...")
            try:
                # Ottieni lo schema JSON conforme ai requisiti Groq
                schema = CachePolicy.get_groq_schema()
                print(f"[DEBUG] Generated schema: {json.dumps(schema, indent=2)}")
                
                # Prova prima senza strict, poi con strict se necessario
                # Prova prima con Structured Outputs
                request_payload = {
                    "model": GROQ_MODEL,
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are an AGGRESSIVE cache policy optimizer. Your goal is to MAXIMIZE hit ratio through BOLD, DECISIVE actions. Take RISKS, use EXTREME parameter values when needed, and EXPLORE different strategies aggressively. Never be conservative - stagnation kills performance. Generate policies that push boundaries and optimize aggressively."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    "temperature": 0.9,
                    "max_tokens": 1024,
                    "response_format": {
                        "type": "json_schema",
                        "json_schema": {
                            "name": "cache_policy",
                            "schema": schema
                        }
                    }
                }
                
                resp = requests.post(
                    GROQ_API_URL,
                    headers={
                        "Authorization": f"Bearer {GROQ_API_KEY}",
                        "Content-Type": "application/json",
                    },
                    json=request_payload,
                    timeout=30,
                )
                print(f"[DEBUG] Groq API response status: {resp.status_code}")
                
                # Se Structured Outputs fallisce, prova con JSON Object Mode
                if resp.status_code == 400:
                    print(f"[WARNING] Structured Outputs failed, trying JSON Object Mode...")
                    print(f"[ERROR] Groq API error response: {resp.text[:1000]}")
                    try:
                        error_data = resp.json()
                        print(f"[ERROR] Groq API error details: {json.dumps(error_data, indent=2)}")
                    except:
                        pass
                    
                    # Fallback a JSON Object Mode
                    request_payload = {
                        "model": GROQ_MODEL,
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are an AGGRESSIVE cache policy optimizer. MAXIMIZE hit ratio through BOLD actions. Use EXTREME values (0.2-0.5 for eviction, 2.0-3.0 for retention). You MUST respond with ONLY valid JSON: {\"global_ttl_factor\": float (0.2-3.0), \"evict_categories\": [string], \"priority_categories\": [string], \"reasoning\": string}. No explanations, no markdown, only JSON."
                            },
                            {"role": "user", "content": prompt}
                        ],
                        "temperature": 0.7,
                        "max_tokens": 1024,
                        "response_format": {"type": "json_object"}
                    }
                    
                    resp = requests.post(
                        GROQ_API_URL,
                        headers={
                            "Authorization": f"Bearer {GROQ_API_KEY}",
                            "Content-Type": "application/json",
                        },
                        json=request_payload,
                        timeout=30,
                    )
                    print(f"[DEBUG] Groq API (JSON mode) response status: {resp.status_code}")
                
                if resp.status_code != 200:
                    print(f"[ERROR] Groq API error response: {resp.text[:1000]}")
                    try:
                        error_data = resp.json()
                        print(f"[ERROR] Groq API error details: {json.dumps(error_data, indent=2)}")
                    except:
                        pass
                
                if resp.status_code == 429:
                    raise requests.exceptions.HTTPError("Rate limit reached", response=resp)
                
                resp.raise_for_status()
                
                # La risposta è già JSON valido e conforme allo schema
                response_data = resp.json()
                content = response_data.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                if not content:
                    print("[ERROR] Empty response from Groq API")
                    return None
                
                # Parse e validazione con Pydantic
                policy_data = json.loads(content)
                policy = CachePolicy.model_validate(policy_data)
                
            except requests.exceptions.HTTPError as e:
                if e.response and e.response.status_code == 429:
                    print("[WARNING] Groq rate limit reached")
                    raise
                raise
            except json.JSONDecodeError as e:
                print(f"[ERROR] Failed to parse JSON response: {e}")
                if hasattr(e, 'response') and e.response is not None:
                    print(f"[ERROR] Response body: {e.response.text[:500]}")
                return None
            except Exception as e:
                print(f"[ERROR] Failed to validate policy schema: {e}")
                import traceback
                traceback.print_exc()
                return None
        else:
            # Fallback a Ollama (non supporta structured outputs, usa JSON mode)
            print("[DEBUG] Calling Ollama API (JSON mode)...")
            resp = requests.post(
                LLM_API_URL,
                json={
                    "model": LLM_MODEL,
                    "prompt": prompt + "\n\nIMPORTANT: Respond with ONLY valid JSON matching this schema: {\"global_ttl_factor\": float, \"evict_categories\": [string], \"priority_categories\": [string], \"reasoning\": string}",
                    "stream": False,
                    "format": "json",
                    "options": {
                        "temperature": 0.9,
                        "top_p": 0.9,
                        "num_predict": 256,
                    },
                },
                timeout=300,
            )
            content = resp.json().get("response", "").strip()
            
            if not content:
                print("[ERROR] Empty response from Ollama")
                return None
            
            try:
                policy_data = json.loads(content)
                policy = CachePolicy.model_validate(policy_data)
            except (json.JSONDecodeError, Exception) as e:
                print(f"[ERROR] Failed to parse/validate Ollama response: {e}")
                return None
        
        LATENCY_MS.set((time.time() - t0) * 1000)
        
        if policy is None:
            print("[ERROR] Policy is None after LLM call")
            return None
        
        print(f"[DEBUG] Policy validated successfully: factor={policy.global_ttl_factor:.2f}, "
              f"priority={policy.priority_categories}, evict={policy.evict_categories}")
        
        # Applica la policy
        with policy_lock:
            old_factor = current_policy.get("global_ttl_factor", 1.0)
            old_priority = current_policy.get("priority_categories", [])
            old_evict = current_policy.get("evict_categories", [])
            
            current_policy["global_ttl_factor"] = policy.global_ttl_factor
            current_policy["priority_categories"] = policy.priority_categories
            current_policy["evict_categories"] = policy.evict_categories
            current_policy["reasoning"] = policy.reasoning
        
        POLICY_APPLICATIONS.inc()
        print(f"[POLICY] New policy applied: factor={policy.global_ttl_factor:.2f}, "
              f"priority={policy.priority_categories}, evict={policy.evict_categories}")
        print(f"[POLICY] Reasoning: {policy.reasoning}")
        
        policy_changed = (
            abs(policy.global_ttl_factor - old_factor) > 0.1 or
            set(policy.priority_categories) != set(old_priority) or
            set(policy.evict_categories) != set(old_evict)
        )
        
        if policy_changed:
            apply_policy_retroactively(
                policy.global_ttl_factor,
                policy.priority_categories,
                policy.evict_categories,
                max_keys_to_scan=500
            )
        
        # Invia a Kafka
        plan_json = policy.model_dump_json()
        try:
            kafka_producer.produce(
                topic=KAFKA_TOPIC,
                value=plan_json.encode("utf-8"),
                callback=delivery_report,
            )
            kafka_producer.poll(0)
            remaining = kafka_producer.flush(timeout=5)
            if remaining > 0:
                print(f"[WARNING] {remaining} messages still in queue after flush")
        except BufferError as e:
            kafka_producer.poll(10)
            try:
                kafka_producer.produce(
                    topic=KAFKA_TOPIC,
                    value=plan_json.encode("utf-8"),
                    callback=delivery_report,
                )
                kafka_producer.poll(0)
                kafka_producer.flush(timeout=5)
            except Exception as e2:
                print(f"[ERROR] Kafka retry failed: {e2}")
        except Exception as e:
            error_str = str(e).lower()
            if "unknown topic" in error_str or "not available" in error_str:
                time.sleep(1)
                try:
                    kafka_producer.produce(
                        topic=KAFKA_TOPIC,
                        value=plan_json.encode("utf-8"),
                        callback=delivery_report,
                    )
                    kafka_producer.poll(0)
                    kafka_producer.flush(timeout=5)
                except Exception as e2:
                    print(f"[ERROR] Kafka retry after sleep failed: {e2}")
            else:
                print(f"[ERROR] Kafka: Impossibile inviare il piano: {e}")
        
        return plan_json
    
    except Exception as e:
        print(f"[ERROR] Unexpected error in LLM call: {e}")
        import traceback
        traceback.print_exc()
        return None

def apply_policy_retroactively(factor, priority_cats, evict_cats, max_keys_to_scan=1000):

    evicted_count = 0
    extended_count = 0
    updated_count = 0
    
    try:
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
            
            for key in keys_to_check:
                pipe.ttl(key)
            
            ttls = pipe.execute()
            
            for i, key in enumerate(keys_to_check):
                if i >= len(ttls):
                    continue
                    
                ttl = ttls[i]
                if ttl <= 0:  
                    continue
                
                try:
                    item_id_str = key.decode('utf-8').replace('item:', '')
                    item_id = int(item_id_str)
                except (ValueError, AttributeError):
                    continue
                
                category = get_item_category_cached(item_id)
                
                if isinstance(category, str) and category in evict_cats:
                    try:
                        r.delete(key)
                        evicted_count += 1
                        RETROACTIVE_EVICTIONS.inc()
                    except Exception as e:
                        print(f"[WARNING] Failed to evict {key}: {e}")
                
                elif isinstance(category, str) and category in priority_cats:
                    try:
                        new_ttl = int(ttl * PRIORITY_TTL_MULT)
                        new_ttl = max(TTL_MIN_SECONDS, min(new_ttl, TTL_MAX_SECONDS))
                        if TTL_TIME_COMPRESSION > 1.0:
                            new_ttl = max(1, int(new_ttl / TTL_TIME_COMPRESSION))
                        r.expire(key, new_ttl)
                        extended_count += 1
                        RETROACTIVE_EXTENSIONS.inc()
                    except Exception as e:
                        print(e)
                       
                
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
        print("Policy retroattiva non applicata")
       
    
    return evicted_count, extended_count, updated_count


def delivery_report(err, msg):
    global kafka_delivery_failures, kafka_delivery_successes
    if err is not None:
        kafka_delivery_failures += 1
        print(f'[ERROR] Errore nella consegna del messaggio a Kafka: {err}')
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
aggregator = ContextAggregator(max_events=1000, session_history_len=10, window_seconds=300)
PLAN_EVERY_N_EVENTS = 1000
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
            
            neighbor_ttl = estimate_ttl_from_neighbors(item_id, k=5)
            if neighbor_ttl is not None and neighbor_ttl > 0:
                base_ttl = neighbor_ttl
            else:
                if cache_full_item < 50:
                    base_ttl = 2000 
                elif cache_full_item < 80:
                    base_ttl = 1000
                else:
                    base_ttl = 400  
            
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
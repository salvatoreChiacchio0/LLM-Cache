import csv, redis, time, requests, pymongo, json, re
# NUOVA DIPENDENZA PER KAFKA
from confluent_kafka import Producer 
from pymilvus import Collection, connections, MilvusException
from prometheus_client import start_http_server, Counter, Gauge

CHARSET = "utf-8"
LOG_FILE = "/app/data/log_15M_subset.txt"
LOG_DELIMITER = chr(1)
FILE_ENCODING = "latin-1"
LLM_API_URL = "http://ollama:11434/api/generate"
LLM_MODEL = "llama3.2:1b"
KAFKA_TOPIC = "aura-plan"

time.sleep(5)

conf = {
    'bootstrap.servers': 'kafka:29092', 
    'client.id': 'llm-producer'
}
kafka_producer = Producer(conf)

r = redis.Redis(host="redis-aura", port=6379, encoding=CHARSET, protocol=2)
r_lru = redis.Redis(host="redis-lru", port=6380, encoding=CHARSET, protocol=2)
r.ping()
r_lru.ping()

start_http_server(8000)
CACHE_MISSES = Counter("cache_misses", "Cache Misses Count (Aura)")
CACHE_HITS = Counter("cache_hits", "Cache Hits Count (Aura)")
LRU_HITS = Counter("lru_hits", "Cache Hits Count (LRU Baseline)")
LRU_MISSES = Counter("lru_misses", "Cache Misses Count (LRU Baseline)")
LATENCY_MS = Gauge("llm_reasoning_latency_ms", "LLM Response Latency in milliseconds")

mongo = pymongo.MongoClient("mongodb://mongo:27017").aura
connections.connect("default", host="milvus", port=19530)

try:
    coll = Collection("catalog")
    coll.load()
except MilvusException:
    exit()

def get_embedding(item_id):
    res = coll.query(expr=f"item_id == {item_id}", output_fields=["embedding"], limit=1)
    return res[0]["embedding"] if res else None

def build_prompt(item_id, user, cache_full):
    doc = mongo.catalog.find_one({"item_id": item_id})
    if not doc:
        return None
    emb = get_embedding(item_id)
    if not emb:
        return None
    res = coll.search(
        data=[emb],
        anns_field="embedding",
        param={"metric_type": "L2", "params": {"nprobe": 8}},
        limit=6,
        output_fields=["item_id"],
    )
    sims = [r.entity.item_id for r in res[0] if r.entity.item_id != item_id][:3]
    return (
        f"TASK: Generate a cache management plan as JSON. Do NOT repeat the input data.\n\n"
        f"CONTEXT:\n"
        f"- Current item ID: {item_id}\n"
        f"- User: {user}\n"
        f"- Similar items (may want to prefetch): {sims}\n"
        f"- Cache usage: {cache_full}% (LIMITED to 256MB total)\n\n"
        f"INSTRUCTIONS:\n"
        f"1. Set current_item_ttl: TTL in seconds for the item being requested.\n"
        f"   - If cache > 80%: use 60-180s (shorter, allow eviction)\n"
        f"   - If cache < 50%: use 1200-3600s (much longer, plenty of memory available)\n"
        f"   - If cache 50-80%: use 600-1200s (medium-long)\n"
        f"   - Similar items present: use longer TTL (they're likely to be accessed)\n"
        f"   - Range: 30-3600 seconds (use longer when memory available!)\n"
        f"2. Set evict: Array of cache keys to remove (empty [] if none).\n"
        f"   - Use if cache > 80% and you want to free space\n"
        f"   - Format: [\"item:123\", \"item:456\"]\n"
        f"3. Set prefetch: Array of items to preload into cache.\n"
        f"   - Consider similar items: {sims}\n"
        f"   - Only prefetch if cache has space (< 80%)\n"
        f"   - Format: [{{\"k\":\"item:123\",\"v\":\"1\",\"ttl\":180}}, ...]\n\n"
        f"REQUIRED OUTPUT FORMAT (JSON only, no other text):\n"
        f'{{"current_item_ttl":360,"evict":[],"prefetch":[{{"k":"item:XXX","v":"1","ttl":180}}]}}\n\n'
        f"Generate the plan now:"
    )

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

def delivery_report(err, msg):
    if err is not None:
        print(f'[ERROR] Errore nella consegna del messaggio a Kafka: {err}')
    else:
        print(f'[SUCCESS] Messaggio consegnato a Kafka: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}')

count = 0
start_t = time.time()

with open(LOG_FILE, "r", encoding=FILE_ENCODING, errors="ignore") as f:
    reader = csv.reader(f, delimiter=LOG_DELIMITER)
    try:
        next(reader)
    except:
        pass

    for row in reader:
        if len(row) < 4:
            continue
        
        item, user, action = row[0], row[1], row[2]
        try:
            item_id = int(item)
        except:
            continue

        item_key = f"item:{item_id}"

        if action != "click":
            continue

        lru_mem = r_lru.info("memory")["used_memory"]
        lru_max_mem = 2 * 1024 * 1024
        lru_cache_full = round((lru_mem / lru_max_mem) * 100, 1)

        if lru_cache_full < 50:
            lru_ttl = 1800
        elif lru_cache_full < 80:
            lru_ttl = 900
        else:
            lru_ttl = 300

        if r_lru.get(item_key):
            LRU_HITS.inc()
        else:
            LRU_MISSES.inc()
            r_lru.setex(item_key, lru_ttl, 1)

        if r.get(item_key) is None:
            CACHE_MISSES.inc()
            mem = r.info("memory")["used_memory"]
            CACHE_LIMIT_BYTES = 2_000_000.0
            cache_full = round(mem / CACHE_LIMIT_BYTES * 100, 1)
            prompt = build_prompt(item_id, user, cache_full)
            plan_valid = False

            if not prompt:
                print(f"[DEBUG] Cache miss per item {item_id}, ma build_prompt ha ritornato None (doc o embedding non trovati)")

            if prompt:
                t0 = time.time()
                try:
                    resp = requests.post(
                        LLM_API_URL,
                        json={
                            "model": LLM_MODEL,
                            "prompt": prompt,
                            "stream": False,
                        "options": {
                            "temperature": 0,
                            "top_p": 0.9,
                            "num_predict": 512,
                        },
                        },
                        timeout=60,
                    )
                    LATENCY_MS.set((time.time() - t0) * 1000)
                    plan = resp.json().get("response", "").strip()

                    if plan.startswith("```"):
                        lines = plan.split("\n")
                        if lines[0].strip().startswith("```"):
                            lines = lines[1:]
                        if lines and lines[-1].strip() == "```":
                            lines = lines[:-1]
                        plan = "\n".join(lines).strip()

                    import json
                    import re

                    json_start = plan.find('{')
                    if json_start != -1:
                        brace_count = 0
                        json_end = -1
                        for i in range(json_start, len(plan)):
                            if plan[i] == '{':
                                brace_count += 1
                            elif plan[i] == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = i + 1
                                    break
                        if json_end != -1:
                            plan = plan[json_start:json_end]

                    if plan and plan.startswith('{') and plan.endswith('}'):
                        plan = fix_json_common_errors(plan)

                    plan_valid = False
                    if plan and plan.startswith('{') and plan.endswith('}'):
                        try:
                            json.loads(plan)
                            plan_valid = True
                        except json.JSONDecodeError as e:
                            print(f"[DEBUG] Cache miss per item {item_id}, JSON malformato (dopo fix): {plan[:100]}... (errore: {e.msg} alla posizione {e.pos})")
                    if not plan_valid:
                        if not plan:
                            print(f"[DEBUG] Cache miss per item {item_id}, ma LLM ha ritornato risposta vuota")
                        elif not plan.startswith('{'):
                            print(f"[DEBUG] Cache miss per item {item_id}, risposta non inizia con {{: {plan[:100]}...")
                        elif not plan.endswith('}'):
                            print(f"[DEBUG] Cache miss per item {item_id}, JSON incompleto (non finisce con }}): {plan[:100]}...")
                    else:
                        try:
                            kafka_producer.produce(
                                topic=KAFKA_TOPIC, 
                                value=plan.encode('utf-8'),
                                callback=delivery_report
                            )
                            kafka_producer.poll(0)
                            print(f"[DEBUG] Cache miss per item {item_id}: piano inviato a Kafka: {plan[:100]}...")
                        except Exception as e:
                            error_str = str(e).lower()
                            if "unknown topic" in error_str or "not available" in error_str:
                                time.sleep(1)
                                try:
                                    kafka_producer.produce(
                                        topic=KAFKA_TOPIC, 
                                        value=plan.encode('utf-8'),
                                        callback=delivery_report
                                    )
                                    kafka_producer.poll(0)
                                    print(f"[DEBUG] Cache miss per item {item_id}: piano inviato a Kafka (retry)")
                                except Exception as e2:
                                    print(f"ERRORE Kafka (retry): Impossibile inviare il piano per item {item_id}: {e2}")
                            else:
                                print(f"ERRORE Kafka: Impossibile inviare il piano per item {item_id}: {e}")
                except Exception as e:
                    print(f"[DEBUG] Cache miss per item {item_id}, errore nella chiamata LLM: {e}")
                
            current_item_ttl = 360

            if plan_valid:
                try:
                    plan_json = json.loads(plan)
                    if "current_item_ttl" in plan_json:
                        ttl_val = plan_json["current_item_ttl"]
                        if isinstance(ttl_val, (int, float)) and ttl_val > 0:
                            current_item_ttl = int(ttl_val)
                        else:
                            print(f"[WARN] TTL non valido per item {item_id}: {ttl_val}, uso default {current_item_ttl}")
                except (json.JSONDecodeError, KeyError, TypeError) as e:
                    print(f"[WARN] Impossibile leggere TTL dall'LLM per item {item_id}: {e}, uso default {current_item_ttl}")

            r.setex(item_key, current_item_ttl, 1)
        else:
            CACHE_HITS.inc()

        count += 1
        if count % 10 == 0:
            rate = count / (time.time() - start_t)
            kafka_producer.poll(0)
            print(
                f"[{time.strftime('%H:%M:%S')}] {count/1e6:.1f}M items processed. Rate: {rate:.0f} req/s.\r",
                end="",
            )

kafka_producer.flush()
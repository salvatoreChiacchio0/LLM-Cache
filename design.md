# LLM-Driven Cache Policy Engine

This document describes the **current implemented prototype** that dynamically manages a NoSQL cache (Redis) using a Large Language Model (LLM) as a policy engine, and compares it against an LRU-like baseline.

## 1. Core Principles

- The system tests the hypothesis that an LLM can perform **context-aware cache decisions** (TTL selection, prefetch, eviction proposals) that outperform a static TTL/LRU policy.
- In the current implementation the LLM reasons over a **compact context**:
  - current item id,
  - current user id,
  - a small set of semantically similar items,
  - a coarse measure of cache pressure (percentage of used memory).
- The system enforces **separation of concerns**:
  - Python (`stream_live.py`) handles log replay, cache probing, feature construction and LLM calls,
  - a Rust **Executor** applies LLM-generated plans to Redis via Kafka, decoupling slow reasoning from fast execution.
- A **baseline Redis instance** using a simple dynamic TTL rule (LRU-style) runs in parallel for comparison.

## 2. Architecture Overview (As Implemented)

The prototype is organized into three layers: Data Access & Evaluation, Reasoning, and Execution.

| Layer | Components | Role |
| :--- | :--- | :--- |
| **Data Access & Evaluation** | `stream_live.py`, `redis-aura`, `redis-lru`, MongoDB, Milvus ANN | Replays user requests from logs, performs cache lookups, gathers context and metrics, and maintains an LRU-style baseline. |
| **Reasoning** | LLM (`llama3.2:1b` via Ollama) | Receives a compact context and returns a structured JSON cache management plan. |
| **Execution** | Kafka topic `aura-plan`, Rust `executor` service, `redis-aura` | Consumes the LLM plan and applies evictions and prefetches atomically to Redis. |

Prometheus is used throughout to export metrics such as cache hit/miss counts, LLM latency and executor behaviour.

## 3. Component Breakdown (Current Prototype)

### 3.1 Data Access & Baseline

1. **Log Replayer (`stream_live.py`):**
   - **Function:** Streams events from the sliced log dataset `log_15M_subset.txt` and treats `"click"` events as cache requests.
   - **Output:** For each click, evaluates **two systems in parallel**:
     - `redis-lru` (baseline),
     - `redis-aura` (LLM-driven cache).

2. **Baseline Cache (`redis-lru`):**
   - **Function:** Serves as a simple dynamic-TTL baseline approximating an LRU behaviour.
   - **Policy:** On a miss, the item is inserted with a TTL that depends only on used memory:
     - cache \< 50%: long TTL (1800s),
     - 50–80%: medium TTL (900s),
     - \> 80%: short TTL (300s).
   - **Metrics:** Prometheus counters track hits and misses for the baseline.

3. **LLM-Driven Cache (`redis-aura`):**
   - **Function:** Stores the same keys as the baseline but with TTLs chosen by the LLM.
   - **Behaviour:**
     - On hit: only metrics are updated.
     - On miss: triggers context construction, an LLM call and a potential plan sent to Kafka. The item is always inserted into `redis-aura` with a TTL chosen by the LLM (or a conservative default if the plan is invalid).
   - **Metrics:** Prometheus counters track hits, misses and the per-miss LLM latency.

4. **MongoDB (catalog):**
   - **Function:** Provides the canonical catalog for items (including metadata such as title, category and brand).
   - **Current use:** The prototype uses MongoDB to locate the item and its associated embedding id; the rich textual metadata are not yet injected into the LLM prompt.

5. **Semantic Finder (Milvus ANN):**
   - **Function:** Performs Approximate Nearest Neighbor search over product embeddings.
   - **Current use:** For each requested item, Milvus returns a small set of similar item ids. A subset of these ids is exposed to the LLM as candidates for potential prefetch.

### 3.2 Reasoning Layer (LLM Policy Engine)

1. **Context Aggregation (as implemented in `stream_live.py`):**
   - On a cache miss in `redis-aura`, the system builds a **minimal context bundle** consisting of:
     - the current `item_id`,
     - the current `user` id,
     - up to three **similar item ids** from Milvus,
     - the current cache usage of `redis-aura` as a percentage of an assumed capacity.
   - No explicit user history sequence or full item metadata are included yet; they are planned as future extensions.

2. **LLM Reasoner (Ollama `llama3.2:1b`):**
   - **Invocation:** `stream_live.py` sends a prompt to a local Ollama instance exposing the small `llama3.2:1b` model.
   - **Prompt structure:**
     - describes the current item, user and similar items,
     - includes the current cache pressure (`cache_full%`),
     - encodes simple guidelines for how TTL should depend on cache usage and similarity,
     - specifies a **strict required output format**: a single JSON object with fields:
       - `current_item_ttl`: TTL in seconds for the current item,
       - `evict`: array of cache keys to delete,
       - `prefetch`: array of objects `{ "k": "...", "v": "...", "ttl": ... }`.
   - **Output handling:**
     - Markdown code fences and extra text are stripped.
     - The first complete JSON object is extracted.
     - A light post-processing step corrects some common JSON formatting issues.
     - The JSON is validated; if invalid, the system logs debug information and falls back to a default TTL.

3. **TTL Application (current item):**
   - If the plan is valid and contains `current_item_ttl`, that value (bounded to positive integers) is used as TTL when inserting the current item into `redis-aura`.
   - If the plan is missing or malformed, a conservative default TTL (e.g. 360 seconds) is used.
   - The **evict** and **prefetch** fields of the plan are not executed in the Python process; they are forwarded unchanged to Kafka for the Executor.

### 3.3 Execution Layer (Rust Policy Agent)

1. **Kafka (`aura-plan` topic):**
   - **Producer:** `stream_live.py` publishes each valid LLM plan as a UTF‑8 JSON payload to the `aura-plan` topic using `confluent_kafka.Producer`.
   - **Reliability:** Basic retry logic handles the case where the topic is not yet available at startup.

2. **Rust Executor Service (`executor`):**
   - **Consumer:** A `StreamConsumer` subscribes to `aura-plan` in the consumer group `policy-executor-group`.
   - **Redis connection:** Maintains an async multiplexed connection to `redis-aura`.
   - **Plan handling:**
     - parses each Kafka message as JSON,
     - builds a Redis pipeline:
       - for each key in `evict`: issues `DEL`,
       - for each object in `prefetch`: issues `SETEX`/`SET` with TTL using the provided key, value and TTL (defaulting when missing),
     - executes the pipeline atomically against Redis,
     - commits the Kafka offset on success.

3. **Metrics and Observability:**
   - The Executor exposes a `/metrics` HTTP endpoint (Warp + Prometheus) exporting:
     - total number of evictions and prefetch operations,
     - a histogram of plan execution latency.
   - Together with the metrics from `stream_live.py`, this allows end‑to‑end evaluation of:
     - cache hit ratio for `redis-aura` vs `redis-lru`,
     - LLM reasoning latency,
     - executor throughput and latency.

## 4. Current Limitations and Next Steps

- The context seen by the LLM is intentionally **minimal** (no explicit session history, no rich item metadata, no explicit cache content snapshot) to keep latency and token usage low.
- At this stage, the LLM primarily acts as a **TTL tuner and plan generator** rather than a full semantic policy over complex sequences.
- Future iterations will experiment with:
  - adding richer but still compact features (user segments, item categories, cache composition summaries),
  - reducing the frequency of LLM calls (batching or triggering only on specific patterns),
  - comparing this LLM‑driven policy against classical ML models on the same log replay pipeline.
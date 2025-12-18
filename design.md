# LLM-Driven Cache Policy Engine

This document describes the **current implemented prototype** that dynamically manages a NoSQL cache (Redis) using a Large Language Model (LLM) as a meta-policy engine, and compares it against an LRU-like baseline.

## 1. Core Principles

- The system tests the hypothesis that an LLM can perform **context-aware, batch-level cache decisions** (prefetch and eviction proposals) that outperform a static TTL/LRU policy when given a rich but compact summary of recent traffic and cache state.
- The LLM is treated as a **slow, global planner** (meta-policy) that operates on windows of events, while per-request latency is governed by a simple, deterministic policy.
- In the current implementation the LLM reasons over an **aggregated context**:
  - recent request counts per item and per category,
  - a small set of sample user sessions (last events per user),
  - a coarse measure of Aura cache pressure (percentage of used memory),
  - item metadata (title, category, brand) for the most requested items in the window.
- The system enforces **separation of concerns**:
  - Python (`stream_live.py`) handles log replay, cache probing, feature aggregation and LLM calls,
  - a Rust **Executor** applies LLM-generated plans (evict/prefetch) to the Aura Redis cache via Kafka, decoupling slow reasoning from fast execution.
- A **baseline Redis instance** using a simple dynamic TTL rule (LRU-style) runs in parallel for comparison and is never controlled by the LLM.

## 2. Architecture Overview (As Implemented)

The prototype is organized into three layers: Data Access & Evaluation, Reasoning, and Execution.

| Layer | Components | Role |
| :--- | :--- | :--- |
| **Data Access & Evaluation** | `stream_live.py`, `redis-aura`, `redis-lru`, MongoDB, Milvus ANN | Replays user requests from logs, performs cache lookups, gathers context and metrics, and maintains an LRU-style baseline. |
| **Reasoning** | LLM (`llama3.2:1b` via Ollama) | Periodically receives an aggregated context window and returns a structured JSON cache management plan for Aura. |
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
   - **Function:** Stores the same keys as the baseline, but its content is periodically optimized by LLM-generated plans (evict/prefetch).
   - **Per-request behaviour:**
     - On hit: only metrics are updated.
     - On miss: the item is inserted into `redis-aura` with a **simple dynamic TTL** based only on Aura memory usage (long TTL when cache is empty, shorter when full). No LLM call is made on the critical path.
   - **Metrics:** Prometheus counters track hits, misses and the LLM reasoning latency associated with batch plans.

4. **MongoDB (catalog):**
   - **Function:** Provides the canonical catalog for items (including metadata such as title, category and brand).
   - **Current use:** The prototype uses MongoDB to locate the item and its associated embedding id; the rich textual metadata are not yet injected into the LLM prompt.

5. **Semantic Finder (Milvus ANN):**
   - **Function:** Performs Approximate Nearest Neighbor search over product embeddings.
   - **Current use:** The standalone Milvus catalog is kept available for future extensions (e.g. semantic prefetching), but the current batch meta-policy does not query Milvus on the critical path.

### 3.2 Reasoning Layer (LLM Policy Engine)

1. **Context Aggregation (`ContextAggregator` in `stream_live.py`):**
   - As the log is replayed, every relevant event updates an in-memory aggregator which tracks:
     - per-item request counts within the current window,
     - per-category request counts,
     - a per-user sliding window of recent events (sample user sessions),
     - Aura hit/miss counts to compute the Aura hit ratio.
   - When a batch boundary is reached (e.g. every N events), the aggregator builds a **snapshot** that includes:
     - total events in the window,
     - Aura hit ratio,
     - a list of the top-K items with attached catalog metadata (title, category, brand) loaded from MongoDB,
     - a list of top categories by request count,
     - a small set of example user sessions (compacted as sequences of `action:item_id`).

2. **LLM Reasoner (Ollama `llama3.2:1b`):**
   - **Invocation:** At batch time, `stream_live.py` sends a prompt to a local Ollama instance exposing the small `llama3.2:1b` model.
   - **Prompt structure:**
     - describes Aura cache pressure in percentage,
     - lists the top requested items with their title, category and brand,
     - lists the top categories,
     - shows a few recent user sessions in compact form,
     - encodes high-level guidelines:
       - favor prefetch when Aura is not full,
       - favor evict when Aura is under pressure,
       - only touch Aura keys (those starting with `item:`),
       - cap the number of actions and constrain TTLs to a safe range.
     - specifies a **strict required output format**: a single JSON object with fields:
       - `evict`: array of cache keys to delete (e.g. `["item:123", "item:456"]`),
       - `prefetch`: array of objects `{ "k": "...", "v": "...", "ttl": ... }`.
   - **Output handling:**
     - Markdown code fences and extra text are stripped.
     - The first complete JSON object is extracted.
     - A light post-processing step corrects some common JSON formatting issues.
     - The JSON is validated; if invalid, the system logs debug information and discards the plan.

3. **Aura TTL Policy (per-request, non-LLM):**
   - Independently from the LLM, every Aura miss is handled with a deterministic TTL policy:
     - Aura cache \< 50%: long TTL (e.g. 1800s),
     - 50–80%: medium TTL (e.g. 900s),
     - \> 80%: short TTL (e.g. 300s).
   - This guarantees predictable per-request latency and a reasonable baseline behaviour even if the LLM is slow or unavailable.

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
       - for each key in `evict` (capped to a maximum of 20 entries): issues `DEL` on Aura keys (`item:*`) only,
       - for each object in `prefetch` (capped to a maximum of 20 entries): issues `SETEX`/`SET` with TTL using the provided key, value and TTL,
         but with the TTL clamped into a safe range (e.g. 60–3600 seconds),
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

- The context seen by the LLM is **aggregated**, but still limited:
  - no explicit long-term user profiles (only short recent sessions),
  - no direct snapshot of individual keys in Aura (only counts and metadata for top items),
  - no explicit use of semantic neighbours from Milvus in the batch prompt yet.
- At this stage, the LLM primarily acts as a **batch planner for evict/prefetch** rather than a full semantic policy over long user journeys.
- Future iterations will experiment with:
  - injecting semantic neighbours from Milvus for the hottest items in each window (to enable semantic prefetching),
  - triggering LLM plans based on adaptive conditions (e.g. drops in Aura hit ratio or spikes in cache pressure) instead of a fixed number of events,
  - comparing this LLM‑driven meta-policy against classical ML models on the same log replay pipeline,
  - exploring larger or more specialized LLMs (or distilled variants) to improve plan quality while keeping batch latency under control.
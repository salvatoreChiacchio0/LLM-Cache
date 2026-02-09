import time
from collections import defaultdict, deque, Counter as CounterCollection
from ..modules.data import get_item_metadata

class ContextAggregator:
    def __init__(self, max_events=1000, session_history_len=10, window_seconds=None):
        self.max_events = max_events
        self.window_seconds = window_seconds
        self.events = deque(maxlen=max_events)
        self.user_sessions = defaultdict(lambda: deque(maxlen=session_history_len))
        self.total_events = 0
        self.previous_snapshot = None
        self.policy_history = deque(maxlen=3)

    def record_event(self, user, item_id, action, aura_hit, timestamp=None, baseline_hit=None):
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
                "aura_hit": aura_hit,
                "baseline_hit": baseline_hit,
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
        aura_hits = 0
        aura_misses = 0
        
        for e in self.events:
            item_counts[e["item_id"]] += 1

            if e["aura_hit"]:
                aura_hits += 1
            else:
                aura_misses += 1

        total_aura = aura_hits + aura_misses
        aura_ratio = aura_hits / total_aura if total_aura > 0 else 0.0
        top_items = item_counts.most_common(10)

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
            doc = get_item_metadata(item_id, {"title": 1, "brand": 1})
            if doc:
                items_meta.append(
                    {
                        "item_id": item_id,
                        "count": count,
                        "title": doc.get("title"),
                        "brand": doc.get("brand"),
                    }
                )
            else:
                items_meta.append(
                    {
                        "item_id": item_id,
                        "count": count,
                        "title": None,
                        "brand": None,
                    }
                )

        # Calculate workload volatility based on item access distribution
        workload_volatility = 0.0
        if self.previous_snapshot:
            prev_items = {item["item_id"]: item["count"] for item in self.previous_snapshot.get("top_items", [])}
            curr_items = {item_id: count for item_id, count in item_counts.most_common(10)}
            
            if prev_items and curr_items:
                all_item_ids = set(prev_items.keys()) | set(curr_items.keys())
                prev_total = sum(prev_items.values())
                curr_total = sum(curr_items.values())
                
                if prev_total > 0 and curr_total > 0:
                    prev_dist = {item_id: count / prev_total for item_id, count in prev_items.items()}
                    curr_dist = {item_id: count / curr_total for item_id, count in curr_items.items()}
                    
                    volatility = sum(abs(curr_dist.get(item_id, 0) - prev_dist.get(item_id, 0)) for item_id in all_item_ids)
                    workload_volatility = volatility / 2.0

        snapshot = {
            "total_events": len(self.events),
            "hit_ratio": aura_ratio,
            "aura_hit_ratio": aura_ratio,
            "top_items": items_meta,
            "sample_sessions": sessions_data,
            "timestamp": now_ts or time.time(),
            "recent_item_ids": [e.get("item_id") for e in list(self.events)[-100:] if e.get("item_id")],
            "workload_volatility": workload_volatility
        }

        delta = None
        if self.previous_snapshot is not None:
            prev_ratio = self.previous_snapshot.get("aura_hit_ratio", 0.0)
            delta = {
                "hit_ratio_change": aura_ratio - prev_ratio,
                "delta_hit_ratio": aura_ratio - prev_ratio,
                "hit_ratio_previous": prev_ratio,
                "events_change": snapshot["total_events"] - self.previous_snapshot.get("total_events", 0),
            }

        self.previous_snapshot = snapshot.copy()
        snapshot["delta"] = delta

        return snapshot

    def enrich_snapshot_with_tinylfu_stats(self, snapshot, tinylfu_stats=None):
        if tinylfu_stats:
            eviction_count = tinylfu_stats.get("eviction_count", 0)
            ghost_hits = tinylfu_stats.get("ghost_hits", 0)

            eviction_regret = 0.0
            if eviction_count > 0:
                eviction_regret = ghost_hits / eviction_count

            snapshot["eviction_regret"] = eviction_regret
            snapshot["tinylfu_stats"] = tinylfu_stats

        return snapshot

    def reset_window(self):
        return

    def record_policy_application(self, policy, hit_ratio_before, hit_ratio_after, timestamp=None):
        policy_entry = {
            "policy": policy.copy(),
            "hit_ratio_before": hit_ratio_before,
            "hit_ratio_after": hit_ratio_after,
            "hit_ratio_change": hit_ratio_after - hit_ratio_before,
            "timestamp": timestamp or time.time(),
        }
        self.policy_history.append(policy_entry)

    def get_policy_history(self):
        return list(self.policy_history)

    def update_latest_policy_hit_ratio(self, hit_ratio_after):
        if len(self.policy_history) > 0:
            latest_entry = self.policy_history[-1]
            latest_entry["hit_ratio_after"] = hit_ratio_after
            latest_entry["hit_ratio_change"] = hit_ratio_after - latest_entry.get("hit_ratio_before", 0.0)

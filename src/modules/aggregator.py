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
        self.previous_cluster_stats = {}
        self.policy_history = deque(maxlen=3)

    def record_event(self, user, item_id, action, category, aura_hit, timestamp=None, cluster_id=None, baseline_hit=None):
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
                "baseline_hit": baseline_hit,
                "cluster_id": cluster_id,
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
        cluster_stats = {}
        aura_hits = 0
        aura_misses = 0
        
        for e in self.events:
            item_counts[e["item_id"]] += 1
            if e["category"] is not None:
                category_counts[e["category"]] += 1
                if not e["aura_hit"]:
                    category_misses[e["category"]] += 1

            cid = e.get("cluster_id")
            if cid is not None:
                if cid not in cluster_stats:
                    cluster_stats[cid] = {
                        "hits": 0, "misses": 0, "total": 0,
                        "baseline_hits": 0, "baseline_misses": 0
                    }
                cluster_stats[cid]["total"] += 1
                if e["aura_hit"]:
                    cluster_stats[cid]["hits"] += 1
                else:
                    cluster_stats[cid]["misses"] += 1

                baseline_hit = e.get("baseline_hit", False)
                if baseline_hit:
                    cluster_stats[cid]["baseline_hits"] += 1
                else:
                    cluster_stats[cid]["baseline_misses"] += 1

                if e["category"]:
                   cid_stats = cluster_stats[cid]
                   if "categories" not in cid_stats:
                       cid_stats["categories"] = CounterCollection()
                   cid_stats["categories"][e["category"]] += 1

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

        cluster_stats_with_ratios = {}
        for cid, stats in cluster_stats.items():
            total = stats["total"]
            hits = stats["hits"]
            misses = stats["misses"]
            hit_ratio = hits / total if total > 0 else 0.0

            baseline_hits = stats.get("baseline_hits", 0)
            baseline_misses = stats.get("baseline_misses", 0)
            baseline_total = baseline_hits + baseline_misses
            baseline_hit_ratio = baseline_hits / baseline_total if baseline_total > 0 else 0.0

            hr_gap = hit_ratio - baseline_hit_ratio

            top_cat = "Mixed"
            if "categories" in stats and stats["categories"]:
                top_cat = stats["categories"].most_common(1)[0][0]

            cluster_stats_with_ratios[cid] = {
                "hits": hits,
                "misses": misses,
                "total": total,
                "hit_ratio": hit_ratio,
                "baseline_hit_ratio": baseline_hit_ratio,
                "hr_gap": hr_gap,
                "semantic_label": top_cat,
                "delta_requests": 0,
                "delta_ratio": 0.0
            }

        for cid, stats in cluster_stats_with_ratios.items():
            if cid in self.previous_cluster_stats:
                prev = self.previous_cluster_stats[cid]
                stats["delta_requests"] = stats["total"] - prev.get("total", 0)
                stats["delta_ratio"] = stats["hit_ratio"] - prev.get("hit_ratio", 0.0)
            else:
                stats["delta_requests"] = stats["total"]
                stats["delta_ratio"] = 0.0

        self.previous_cluster_stats = {}
        for cid, stats in cluster_stats_with_ratios.items():
             self.previous_cluster_stats[cid] = {
                 "total": stats["total"],
                 "hit_ratio": stats["hit_ratio"]
             }

        workload_volatility = 0.0
        if self.previous_snapshot and "cluster_stats" in self.previous_snapshot:
            prev_clusters = self.previous_snapshot["cluster_stats"]
            prev_total = sum(s.get("total", 0) for s in prev_clusters.values())
            curr_total = sum(s.get("total", 0) for s in cluster_stats_with_ratios.values())

            if prev_total > 0 and curr_total > 0:
                prev_dist = {cid: s.get("total", 0) / prev_total for cid, s in prev_clusters.items()}
                curr_dist = {cid: s.get("total", 0) / curr_total for cid, s in cluster_stats_with_ratios.items()}

                all_cids = set(prev_dist.keys()) | set(curr_dist.keys())
                volatility = sum(abs(curr_dist.get(cid, 0) - prev_dist.get(cid, 0)) for cid in all_cids)
                workload_volatility = volatility / 2.0

        snapshot = {
            "total_events": len(self.events),
            "hit_ratio": aura_ratio,
            "aura_hit_ratio": aura_ratio,
            "top_items": items_meta,
            "top_categories": [{"category": c, "count": n} for c, n in top_categories],
            "miss_categories": [{"category": c, "count": n} for c, n in category_misses.most_common(5)],
            "sample_sessions": sessions_data,
            "cluster_stats": cluster_stats_with_ratios,
            "timestamp": now_ts or time.time(),
            "recent_access_sequence": [e.get("cluster_id") for e in list(self.events)[-100:] if e.get("cluster_id")],
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

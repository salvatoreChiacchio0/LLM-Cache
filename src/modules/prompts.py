import time
from ..core.config import TTL_MAP

def _is_small_model(model_name: str) -> bool:
    if not model_name:
        return False
    lowered = model_name.lower()
    for token in ["0.5b", "1b", "1.5b", "2b"]:
        if token in lowered:
            return True
    return False

def build_global_prompt_small(snapshot, previous_policy=None, metrics_feedback=None):
    lines = []

    lines.append("ROLE:")
    lines.append("You are a TinyLFU META-CONTROLLER.")
    lines.append("You can ONLY adjust TinyLFU parameters.")
    lines.append("You MUST NOT evict keys, prefetch keys, or touch Redis directly.")
    lines.append("")

    lines.append("CURRENT CACHE STATE:")
    lines.append(f"- Hit Ratio: {snapshot.get('hit_ratio', 0.0):.3f}")
    lines.append(f"- Total Events: {snapshot.get('total_events', 0)}")
    lines.append(f"- Workload Volatility: {snapshot.get('workload_volatility', 0.0):.3f}")
    lines.append("")

    opportunity_loss = 0.0
    if metrics_feedback and "adaptation_context" in metrics_feedback:
        opportunity_loss = metrics_feedback["adaptation_context"].get("opportunity_loss", 0.0)

    lines.append("BASELINE COMPARISON:")
    lines.append(f"- Opportunity Loss vs Baseline: {opportunity_loss:.6f}")
    lines.append("")

    cluster_stats = snapshot.get("cluster_stats", {})
    rising_clusters = []
    if cluster_stats:
        for cid, c in cluster_stats.items():
            if c.get("delta_requests", 0) > 50 and c.get("hr_gap", 0) < 0:
                rising_clusters.append(c.get("semantic_label", cid))

    if rising_clusters:
        lines.append("RISING CLUSTERS LOSING VS BASELINE:")
        for c in rising_clusters[:3]:
            lines.append(f"- {c}")
        lines.append("")
    else:
        lines.append("NO CRITICAL RISING CLUSTERS DETECTED")
        lines.append("")

    lines.append("CURRENT TINYLFU PARAMETERS:")
    if previous_policy and previous_policy.get("tinylfu_control"):
        t = previous_policy["tinylfu_control"]
        lines.append(f"- decay_factor: {t.get('decay_factor')}")
        lines.append(f"- reset_interval: {t.get('reset_interval')}")
        lines.append(f"- doorkeeper_bias: {'set' if t.get('doorkeeper_bias') else 'null'}")
    else:
        lines.append("- decay_factor: null")
        lines.append("- reset_interval: null")
        lines.append("- doorkeeper_bias: null")
    lines.append("")

    lines.append("DECISION RULES (FOLLOW STRICTLY):")
    lines.append("1) If opportunity_loss > 0:")
    lines.append("   → You MUST change decay_factor to a value < 0.90")
    lines.append("")
    lines.append("2) Else if workload_volatility > 0.35:")
    lines.append("   → You MUST change decay_factor to 0.75–0.85")
    lines.append("")
    lines.append("3) Else if there are RISING clusters losing vs baseline:")
    lines.append("   → You MUST set doorkeeper_bias ONLY for those categories (+3 or +4)")
    lines.append("")
    lines.append("4) Else:")
    lines.append("   → Do NOT change anything (all fields = null)")
    lines.append("")
    lines.append("IMPORTANT:")
    lines.append("- Change ONLY ONE parameter type")
    lines.append("- If you change decay_factor, reset_interval MUST be null")
    lines.append("- If evidence is weak, do NOTHING")
    lines.append("")

    lines.append("OUTPUT FORMAT (STRICT JSON ONLY):")
    lines.append("{")
    lines.append('  "reasoning": "Short technical explanation (max 2 sentences)",')
    lines.append('  "tinylfu_control": {')
    lines.append('    "decay_factor": null | 0.85,')
    lines.append('    "reset_interval": null,')
    lines.append('    "doorkeeper_bias": null | { "Category": 3 }')
    lines.append('  }')
    lines.append("}")
    lines.append("")
    lines.append("RULES:")
    lines.append("- ALWAYS return valid JSON")
    lines.append("- NEVER add extra fields")
    lines.append("- NEVER explain outside JSON")

    return "\n".join(lines)

def build_global_prompt(snapshot=None, cache_full=None, previous_policy=None, metrics_feedback=None, snapshot_history=None, current_adaptation_state=None, best_strategy=None):
    lines = []
    
    lines.append("TASK: You are a TinyLFU META-CONTROLLER. Your role is to adapt TinyLFU admission policy parameters based on workload dynamics.")
    lines.append("")
    lines.append("CRITICAL CONSTRAINTS:")
    lines.append("- You MUST NOT evict individual cache keys")
    lines.append("- You MUST NOT prefetch individual keys")
    lines.append("- You MUST NOT set TTLs on individual entries")
    lines.append("- You MUST NOT interact directly with Redis keys")
    lines.append("- You can ONLY adjust TinyLFU internal parameters to influence admission decisions")
    lines.append("")
    lines.append("GLOBAL STATE:")
    lines.append(f"- Cache Usage: {cache_full:.1f}%")
    lines.append(f"- Total Events (Window): {snapshot['total_events']}")
    lines.append(f"- Global Hit Ratio: {snapshot['hit_ratio']:.3f}")
    
    delta = snapshot.get("delta")
    change = 0.0
    if delta is not None:
        change = delta.get('delta_hit_ratio', delta.get('hit_ratio_change', 0.0))
        abs_change = abs(change)
        if abs_change < 0.005:
            signal_type = "NOISE (ignore)"
        elif abs_change < 0.010:
            signal_type = "WEAK SIGNAL (be cautious)"
        else:
            signal_type = "MEANINGFUL SIGNAL"
        lines.append(f"- Hit Ratio Change: {change:+.3f} ({signal_type})")

    lines.append("")
    lines.append("PREVIOUS POLICY EVALUATION:")
    if previous_policy:
        lines.append(f"Last Policy Applied At: {time.ctime(previous_policy.get('timestamp', 0)) if previous_policy.get('timestamp') else 'Unknown'}")
        
        tlfu_control = previous_policy.get("tinylfu_control", {})
        if tlfu_control:
            decay = tlfu_control.get("decay_factor")
            reset = tlfu_control.get("reset_interval")
            bias = tlfu_control.get("doorkeeper_bias")
            lines.append(f"  - Previous TinyLFU Parameters:")
            if decay is not None:
                lines.append(f"    * decay_factor: {decay}")
            if reset is not None:
                lines.append(f"    * reset_interval: {reset}")
            if bias:
                lines.append(f"    * doorkeeper_bias: {len(bias)} categories")
        
        abs_change = abs(change)
        if abs_change < 0.005:
            lines.append(f"  - Hit Ratio Change: {change:+.3f} (NOISE - too small to interpret)")
        elif abs_change < 0.010:
            lines.append(f"  - Hit Ratio Change: {change:+.3f} (WEAK SIGNAL - interpret cautiously)")
        else:
            direction = "increase" if change > 0 else "decrease"
            lines.append(f"  - Hit Ratio Change: {change:+.3f} ({direction})")
        
        if metrics_feedback:
            eff = metrics_feedback.get("eviction_efficiency", 1.0) * 100
            lines.append(f"  - Eviction Efficiency: {eff:.1f}% (Keys evicted & not re-requested)")
            
            regret = snapshot.get("eviction_regret", 0.0)
            if regret > 0:
                lines.append(f"  - Eviction Regret: {regret:.1%} (Evictions followed by re-access)")
            
            baseline_vs = metrics_feedback.get("baseline_vs_aura")
            if baseline_vs:
                baseline_hr = baseline_vs.get("baseline", 0.0) * 100
                aura_hr = baseline_vs.get("aura", 0.0) * 100
                improvement = baseline_vs.get("improvement", 0.0) * 100
                lines.append(f"  - Baseline TinyLFU Hit Ratio: {baseline_hr:.1f}%")
                lines.append(f"  - Aura (TinyLFU+LLM) Hit Ratio: {aura_hr:.1f}%")
                lines.append(f"  - Relative Performance: {improvement:+.1f}% vs baseline")
                lines.append("")
                lines.append("🎯 STRATEGIC GOAL (STEP 3 & 4: Proactive Anti-Stagnation + Killer Instinct):")
                if abs(improvement) < 2.0:
                    lines.append("  ⚠️ You are just matching the baseline (improvement < 2%). This is a FAILURE.")
                    lines.append("  → Your goal is NOT to match the baseline. Matching is a failure.")
                    lines.append("  → You MUST exceed the baseline by at least 2% to be considered successful")
                    lines.append("  → If Opportunity Loss is 0, you are just mimicking a basic LRU")
                    lines.append("  → To WIN, you must EXPERIMENT with doorkeeper_bias for RISING clusters")
                    lines.append("  → Use decay_factor aggressively (0.80-0.90) to break out of LRU-like behavior")
                elif improvement < 0:
                    lines.append("  🚨 You are LOSING to baseline. Immediate aggressive action required:")
                    lines.append("  → Apply decay_factor 0.85-0.90 to flush stale sketch data")
                    lines.append("  → Consider doorkeeper_bias for RISING clusters to capture trends early")
                else:
                    lines.append(f"  ✓ You are beating baseline by {improvement:.1f}%. STEP 4: KILLER INSTINCT ACTIVATED.")
                    lines.append("  → When Aura is winning (Improvement > 0), do NOT revert to default parameters.")
                    lines.append("  → Optimize further. Your target is a 10% lead. If you are at 4%, double down on doorkeeper_bias for high-volume clusters.")
                    lines.append("  → Continue experimenting with doorkeeper_bias for RISING clusters")
                    lines.append("  → Monitor for opportunity loss - if it appears, be proactive with decay_factor")
    else:
        lines.append("  (No previous policy data available - establish baseline)")

    if snapshot_history and len(snapshot_history) > 0:
        lines.append("")
        lines.append("HISTORICAL PERFORMANCE (STEP 3: Last 7 Decisions - Long-term Memory Bank):")
        lines.append("This shows the results of your previous parameter adjustments:")
        lines.append("")
        
        for i, hist_entry in enumerate(reversed(snapshot_history[-7:]), 1):
            params = hist_entry.get("parameters_applied", {})
            decay = params.get("decay_factor")
            reset = params.get("reset_interval")
            bias = params.get("doorkeeper_bias")
            
            param_strs = []
            if decay is not None:
                param_strs.append(f"Decay={decay:.3f}")
            if reset is not None:
                param_strs.append(f"Reset={reset}")
            if bias is not None and isinstance(bias, dict):
                param_strs.append(f"Bias={len(bias)}cats")
            if not param_strs:
                param_strs.append("No changes")
            
            params_display = ", ".join(param_strs)
            hr = hist_entry.get("hit_ratio_aura", 0.0)
            improvement = hist_entry.get("improvement_vs_baseline", 0.0)
            state = hist_entry.get("adaptation_state", "UNKNOWN")
            
            result_str = f"HR={hr:.3f}, Improvement={improvement:+.3f}"
            
            lines.append(f"  T-{i} [{state}]: {params_display} → {result_str}")
        
        lines.append("")
        lines.append("Use this history to reason about what worked and what didn't.")
        lines.append("Look for patterns: did decay_factor changes help? Did reset_interval adjustments improve performance?")
        
        if best_strategy:
            bs_params = best_strategy.get("parameters_applied", {})
            bs_improvement = best_strategy.get("improvement_vs_baseline", 0.0)
            bs_volatility = best_strategy.get("workload_volatility", 0.0)
            bs_hr = best_strategy.get("hit_ratio_aura", 0.0)
            lines.append("")
            lines.append("🏆 STEP 3: LAST PEAK PERFORMANCE STRATEGY (Best in Last Hour):")
            lines.append(f"  - Improvement: {bs_improvement:+.4f} (highest in last hour)")
            lines.append(f"  - Hit Ratio: {bs_hr:.4f}")
            lines.append(f"  - Volatility Level: {bs_volatility:.3f}")
            if bs_params:
                decay = bs_params.get("decay_factor")
                reset = bs_params.get("reset_interval")
                bias = bs_params.get("doorkeeper_bias")
                param_strs = []
                if decay is not None:
                    param_strs.append(f"decay_factor={decay:.3f}")
                if reset is not None:
                    param_strs.append(f"reset_interval={reset}")
                if bias is not None:
                    param_strs.append(f"doorkeeper_bias={len(bias)} categories")
                if param_strs:
                    lines.append(f"  - Parameters: {', '.join(param_strs)}")
            lines.append("  → Consider replicating this strategy if current volatility is similar")
    else:
        lines.append("")
        lines.append("HISTORICAL PERFORMANCE:")
        lines.append("  (No history available yet - establishing baseline)")

    if current_adaptation_state and current_adaptation_state.value == "STABLE_BUT_INEFFECTIVE":
        opp_loss = 0.0
        if metrics_feedback and "adaptation_context" in metrics_feedback:
            adaptation_ctx = metrics_feedback["adaptation_context"]
            opp_loss = adaptation_ctx.get('opportunity_loss', 0.0)
        
        lines.append("")
        lines.append("🚨🚨🚨 URGENT: ADAPTATION STATE: STABLE_BUT_INEFFECTIVE 🚨🚨🚨")
        lines.append(f"- Current State: {current_adaptation_state.value}")
        lines.append(f"- Opportunity Loss: {opp_loss:.6f} (Baseline is outperforming Aura - even 0.1% gap is unacceptable)")
        lines.append("")
        lines.append("⚠️ CRITICAL ALERT: Aura is underperforming. Small tweaks are PROHIBITED.")
        lines.append("")
        lines.append("IMMEDIATE DISRUPTIVE ACTION REQUIRED:")
        lines.append("  → You MUST use 'Disruptive Parameters': decay_factor < 0.90 AND reset_interval < 50000")
        lines.append("  → The TinyLFU frequency sketch is saturated with stale heavy hitters")
        lines.append("  → Incremental changes (0.01-0.02) will NOT be sufficient")
        lines.append("  → You need a DISRUPTIVE change (0.10-0.15 reduction) to force adaptation")
        lines.append("")
        lines.append("MANDATORY PARAMETERS:")
        lines.append("  - decay_factor: MUST be < 0.90 (recommended: 0.80-0.85 for aggressive forgetting)")
        lines.append("  - reset_interval: MUST be < 50000 (recommended: 40000-50000 for frequent resets)")
        lines.append("")
        lines.append("DO NOT use small adjustments. The baseline is beating you - you need to SHOCK the system.")
    elif metrics_feedback and "adaptation_context" in metrics_feedback:
        adaptation_ctx = metrics_feedback["adaptation_context"]
        opp_loss = adaptation_ctx.get('opportunity_loss', 0.0)
        if opp_loss > 0.001:
            lines.append("")
            lines.append("⚠️ WARNING: Opportunity Loss detected ({opp_loss:.6f})")
            lines.append("  → Baseline is outperforming Aura. Consider proactive decay_factor adjustment.")
    
    workload_volatility = snapshot.get("workload_volatility", 0.0)
    lines.append("")
    lines.append("WORKLOAD CHARACTERISTICS:")
    lines.append(f"- Workload Volatility: {workload_volatility:.3f} (0=stable, 1=high churn)")
    if workload_volatility > 0.35:
        lines.append("")
        lines.append("⚡ STEP 2: VOLATILITY SHOCK - ATTACK MODE ACTIVATED (volatility > 0.35)")
        lines.append("  → High Volatility detected. Coordinate Decay (0.75-0.80) and Reset (40k) to purge stale frequencies.")
        lines.append("  → DO NOT wait for hit ratio to drop. Assume the frequency sketch is ALREADY stale.")
        lines.append("  → Set decay_factor to 0.75-0.80 IMMEDIATELY (more aggressive than 0.85)")
        lines.append("  → Set reset_interval to 40000 for more frequent resets")
        lines.append("  → Proactive coordinated shock is required - don't wait for performance degradation")
    elif workload_volatility > 0.3:
        lines.append("")
        lines.append("🚨 PROACTIVE STRATEGY REQUIRED: High volatility detected (>0.3)")
        lines.append("  → DO NOT wait for hit ratio to drop. Assume the frequency sketch is ALREADY stale.")
        lines.append("  → Set decay_factor to 0.85 IMMEDIATELY to flush stale patterns")
        lines.append("  → Proactive action is required - don't wait for performance degradation")
    elif workload_volatility < 0.1:
        lines.append("  → Stable workload: can use longer reset intervals")
    
    cluster_stats = snapshot.get("cluster_stats", {})
    if cluster_stats:
        lines.append("")
        lines.append("CLUSTER TRENDS (Trend-Aware Analysis):")
        lines.append("Analyze volume changes to understand workload shifts:")
        lines.append("")
        
        clusters = []
        for cid, stats in cluster_stats.items():
            stats['id'] = cid
            clusters.append(stats)
        
        top_volume = sorted(clusters, key=lambda x: x['total'], reverse=True)[:10]

        def categorize_trend(delta_requests):
            if delta_requests > 50:
                return "RISING"
            elif delta_requests < -50:
                return "DROPPING"
            else:
                return "STABLE"
        
        def print_cluster_with_trend(c):
            delta = c.get('delta_requests', 0)
            trend_label = categorize_trend(delta)
            trend_symbol = "📈" if trend_label == "RISING" else "📉" if trend_label == "DROPPING" else "➡️"
            
            return (f"  {trend_symbol} ID {c['id']} [{c.get('semantic_label', 'Unknown')}] "
                    f"[{trend_label}]: Vol={c['total']} (Δ{delta:+d}), "
                    f"HR={c.get('hit_ratio', 0.0):.2f}")
        
        all_clusters_with_gap = [c for c in clusters if c.get('hr_gap') is not None]
        losing_clusters = sorted(
            [c for c in all_clusters_with_gap if c.get('hr_gap', 0) < 0],
            key=lambda x: (x.get('hr_gap', 0), -x.get('total', 0))
        )[:5]
        
        if losing_clusters:
            lines.append("")
            lines.append("🚨 STEP 1: TOP 5 CLUSTERS LOSING AGAINST BASELINE:")
            lines.append("These clusters have lower HR than Baseline. Apply aggressive bias immediately:")
            for c in losing_clusters:
                cid = c.get('id', 'Unknown')
                aura_hr = c.get('hit_ratio', 0.0)
                baseline_hr = c.get('baseline_hit_ratio', 0.0)
                gap = c.get('hr_gap', 0.0)
                total = c.get('total', 0)
                trend = categorize_trend(c.get('delta_requests', 0))
                category = c.get('semantic_label', 'Unknown')
                lines.append(f"  ⚠️ Cluster {cid} [{category}] [{trend}]: Aura HR={aura_hr:.3f}, Baseline HR={baseline_hr:.3f}, Gap={gap:+.3f}, Vol={total}")
                if trend == "RISING" and gap < 0:
                    lines.append(f"    → ACTION: Apply doorkeeper_bias +4 or +5 for '{category}' to front-run LRU")
            lines.append("")
        
        rising_clusters = [c for c in top_volume if categorize_trend(c.get('delta_requests', 0)) == "RISING"]
        dropping_clusters = [c for c in top_volume if categorize_trend(c.get('delta_requests', 0)) == "DROPPING"]
        stable_clusters = [c for c in top_volume if categorize_trend(c.get('delta_requests', 0)) == "STABLE"]
        
        if rising_clusters:
            lines.append("RISING Clusters (Increasing Volume):")
            for c in rising_clusters[:5]:
                lines.append(print_cluster_with_trend(c))
            lines.append("")
        
        if dropping_clusters:
            lines.append("DROPPING Clusters (Decreasing Volume):")
            for c in dropping_clusters[:5]:
                lines.append(print_cluster_with_trend(c))
            lines.append("")
        
        if stable_clusters:
            lines.append("STABLE Clusters (Steady Volume):")
            for c in stable_clusters[:3]:
                lines.append(print_cluster_with_trend(c))
            lines.append("")
        
        lines.append("REASONING GUIDANCE (STEP 1 & 3: Aggressive Cluster Bias Strategy):")
        lines.append("- STEP 1: RISING clusters with lower HR than Baseline: Apply HIGH-POWER bias (+4 or +5) immediately")
        lines.append("  → You have high-power bias (up to +5). If a RISING cluster has lower HR than Baseline, apply bias +4 immediately to front-run the LRU")
        lines.append("- RISING clusters: Apply positive doorkeeper_bias PROACTIVELY to admit their items BEFORE baseline does")
        lines.append("  → Identify RISING clusters and set doorkeeper_bias > 0 for their categories")
        lines.append("  → This gives Aura an advantage by admitting trending items earlier than baseline")
        lines.append("- DROPPING clusters suggest workload shift - consider adjusting decay_factor for faster adaptation")
        lines.append("- STABLE clusters indicate consistent patterns - preserve with longer reset_interval")
    else:
        lines.append("")
        lines.append("CLUSTER TRENDS:")
        lines.append("  (NO CLUSTER DATA AVAILABLE YET)")

    lines.append("")
    lines.append("CURRENT TINYLFU PARAMETERS:")
    if previous_policy and previous_policy.get("tinylfu_control"):
        tlfu_control = previous_policy.get("tinylfu_control", {})
        current_decay = tlfu_control.get("decay_factor")
        current_reset = tlfu_control.get("reset_interval")
        current_bias = tlfu_control.get("doorkeeper_bias")
        
        if current_decay is not None:
            lines.append(f"  - decay_factor: {current_decay} (CURRENT - applied in last policy)")
        else:
            lines.append(f"  - decay_factor: null (CURRENT - no decay applied, using default)")
        
        if current_reset is not None:
            lines.append(f"  - reset_interval: {current_reset} (CURRENT - applied in last policy)")
        else:
            lines.append(f"  - reset_interval: null (CURRENT - using default 100000)")
        
        if current_bias:
            lines.append(f"  - doorkeeper_bias: {len(current_bias)} categories configured (CURRENT)")
        else:
            lines.append(f"  - doorkeeper_bias: null (CURRENT - no category bias)")
    else:
        lines.append("  - decay_factor: null (CURRENT - no decay applied, using default)")
        lines.append("  - reset_interval: null (CURRENT - using default 100000)")
        lines.append("  - doorkeeper_bias: null (CURRENT - no category bias)")
    lines.append("")
    lines.append("⚠️ IMPORTANT: The values above are the CURRENT parameters. When making changes:")
    lines.append("   - If you want to keep a parameter unchanged, set it to null")
    lines.append("   - If you want to change a parameter, provide a NEW value (not the current one)")
    lines.append("   - Example: If current decay_factor=0.85 and you want to keep it, use null (not 0.85)")
    lines.append("   - Example: If current decay_factor=0.85 and you want to change it, use a different value (e.g., 0.90)")
    lines.append("")
    lines.append("TINYLFU ARCHITECTURE:")
    lines.append("TinyLFU uses three components for admission decisions:")
    lines.append("1. Count-Min Sketch: Approximate frequency estimation (tracks how often items are accessed)")
    lines.append("2. Doorkeeper: Admission filter (first access gets free pass, subsequent accesses checked)")
    lines.append("3. Periodic Reset: Prevents stale frequency data (resets counters after N accesses)")
    lines.append("")
    lines.append("TINYLFU PARAMETER ADAPTATION:")
    lines.append("You can adjust these parameters to optimize admission decisions:")
    lines.append("")
    lines.append("⚠️ CRITICAL: TinyLFU is SENSITIVE. To see a REAL change, parameters must move significantly:")
    lines.append("   - decay_factor needs to move by at least 0.05 (not 0.01) to have measurable impact")
    lines.append("   - Small tweaks (0.97 → 0.98) are often lost in noise")
    lines.append("   - If opportunity_loss is positive, your goal is to be DISRUPTIVE")
    lines.append("   - Focus on decay_factor as your PRIMARY weapon for aggressive adaptation")
    lines.append("")
    lines.append("1. decay_factor (float, 0.0-1.0, default: null = no decay):")
    lines.append("   - Scales down all Count-Min Sketch counters by this factor")
    lines.append("   - Lower values (e.g., 0.85-0.90) = aggressive forgetting of old patterns")
    lines.append("   - Use when workload is changing rapidly (high volatility) OR when losing to baseline")
    lines.append("   - Example: 0.85 = reduce all frequencies by 15% (strong favor for recent accesses)")
    lines.append("   - MINIMUM CHANGE: 0.05 difference from current value to see real impact")
    lines.append("")
    lines.append("2. reset_interval (int, 50000-500000, default: null = keep current):")
    lines.append("   - Number of accesses before resetting frequency data")
    lines.append("   - Lower values = more adaptive to changes, but lose long-term patterns")
    lines.append("   - Higher values = preserve long-term patterns, but slower adaptation")
    lines.append("   - Adjust based on workload volatility and hit ratio trends")
    lines.append("")
    lines.append("3. doorkeeper_bias (dict, category -> int, default: null = no bias):")
    lines.append("   - Adjusts admission bias per category/cluster")
    lines.append("   - Positive values = more likely to admit items from this category")
    lines.append("   - Negative values = less likely to admit items from this category")
    lines.append("   - Use when certain categories have different access patterns")
    lines.append("   - Example: {\"Electronics\": 2, \"Books\": -1}")
    lines.append("")
    lines.append("DECISION GUIDELINES:")
    lines.append("- Only change parameters if you have STRONG evidence (hit ratio change >= 0.01 or consistent trend)")
    lines.append("- If |hit_ratio_change| < 0.005: treat as noise, do NOT react")
    lines.append("- If |hit_ratio_change| < 0.010: interpret cautiously, prefer stability")
    lines.append("- Change AT MOST one parameter type per window (decay OR reset_interval OR doorkeeper_bias)")
    lines.append("- Prefer small, incremental adjustments (e.g., decay_factor: 0.95-0.99, reset_interval: ±20%)")
    lines.append("- If evidence is weak or ambiguous, set all fields to null (no change)")
    lines.append("")
    lines.append("REASONING REQUIREMENT (Chain-of-Thought):")
    lines.append("Before providing parameters, you MUST analyze the workload and explain your reasoning.")
    lines.append("Consider:")
    lines.append("  - What patterns do you see in the historical performance?")
    lines.append("  - How are cluster trends affecting cache behavior?")
    lines.append("  - What is the relationship between volatility and current hit ratio?")
    lines.append("  - Based on previous decisions, what adjustments are likely to help?")
    lines.append("")
    lines.append("OUTPUT FORMAT (STRICT JSON ONLY):")
    lines.append("{")
    lines.append('  "reasoning": "Your technical analysis explaining why these parameter changes are needed",')
    lines.append('  "tinylfu_control": {')
    lines.append('    "decay_factor": 0.95 | null,  // Optional: scale down frequencies')
    lines.append('    "reset_interval": 80000 | null,  // Optional: adjust reset frequency')
    lines.append('    "doorkeeper_bias": {  // Optional: category-specific admission bias')
    lines.append('      "category_name": 1,  // Positive = more likely to admit')
    lines.append('      "another_category": -1  // Negative = less likely to admit')
    lines.append('    } | null')
    lines.append('  }')
    lines.append("}")
    lines.append("")
    lines.append("FIELD REQUIREMENTS:")
    lines.append("- reasoning: REQUIRED - Brief technical analysis (2-4 sentences) explaining your decision")
    lines.append("- tinylfu_control: REQUIRED (but all fields inside may be null)")
    lines.append("- decay_factor: null if no change, otherwise float in [0.0, 1.0]")
    lines.append("- reset_interval: null if no change, otherwise int in [50000, 500000]")
    lines.append("- doorkeeper_bias: null if no change, otherwise dict with category->int mappings")
    lines.append("- NO other fields are allowed")
    lines.append("- NO cluster admission decisions")
    lines.append("- NO evict/prefetch/TTL operations")
    lines.append("")
    lines.append("EXAMPLE OUTPUTS:")
    lines.append("")
    lines.append("No change needed:")
    lines.append('{"reasoning": "Hit ratio is stable and improving. Historical data shows previous decay_factor adjustment (0.98) resulted in +0.002 improvement. Current volatility is low (0.08), so maintaining current parameters.", "tinylfu_control": {"decay_factor": null, "reset_interval": null, "doorkeeper_bias": null}}')
    lines.append("")
    lines.append("Adjust decay for volatile workload:")
    lines.append('{"reasoning": "Workload volatility increased to 0.52. Historical T-1 shows decay_factor=0.98 helped during similar volatility (+0.001). Cluster trends show 3 RISING clusters, indicating shift. Applying decay_factor=0.95 to favor recent accesses.", "tinylfu_control": {"decay_factor": 0.95, "reset_interval": null, "doorkeeper_bias": null}}')
    lines.append("")
    lines.append("Adjust reset interval for better adaptation:")
    lines.append('{"reasoning": "Hit ratio decreased by -0.008. History shows reset_interval changes were effective. STABLE clusters dominate, suggesting need to preserve long-term patterns. Reducing reset_interval to 80000 for faster adaptation while maintaining pattern recognition.", "tinylfu_control": {"decay_factor": null, "reset_interval": 80000, "doorkeeper_bias": null}}')
    lines.append("")
    lines.append("Category-specific bias:")
    lines.append('{"reasoning": "Cluster analysis reveals Electronics cluster is RISING (+120 requests) with low hit ratio (0.45). Books cluster is STABLE but high volume. Applying positive bias to Electronics to improve admission for trending items.", "tinylfu_control": {"decay_factor": null, "reset_interval": null, "doorkeeper_bias": {"Electronics": 2, "Books": -1}}}')
    
    return "\n".join(lines)

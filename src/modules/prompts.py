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

    lines.append("GOAL: Make Aura hit ratio HIGHER than Baseline. Improvement must be POSITIVE.")
    lines.append("")

    # Current metrics - concise
    baseline_hr = 0.0
    aura_hr = 0.0
    improvement = 0.0
    if metrics_feedback and "baseline_vs_aura" in metrics_feedback:
        baseline_data = metrics_feedback["baseline_vs_aura"]
        baseline_hr = baseline_data.get("baseline", 0.0)
        aura_hr = baseline_data.get("aura", 0.0)
        improvement = baseline_data.get("improvement", 0.0)

    volatility = snapshot.get('workload_volatility', 0.0)
    
    lines.append(f"Baseline HR: {baseline_hr:.4f} | Aura HR: {aura_hr:.4f} | Improvement: {improvement:+.4f}")
    lines.append(f"Volatility: {volatility:.4f} | Events: {snapshot.get('total_events', 0)}")
    lines.append("")

    # Previous parameters and explicit feedback
    prev_bias = None
    prev_decay = None
    prev_reset = None
    if previous_policy and previous_policy.get("tinylfu_control"):
        t = previous_policy["tinylfu_control"]
        prev_bias = t.get('admission_bias')
        prev_decay = t.get('decay_factor')
        prev_reset = t.get('reset_interval')
    
    lines.append("LAST DECISION:")
    lines.append(f"  admission_bias={prev_bias}, decay_factor={prev_decay}, reset_interval={prev_reset}")
    
    # Explicit anti-repetition feedback
    if improvement <= 0:
        lines.append(f"  RESULT: FAILED (improvement={improvement:+.4f})")
        lines.append(f"  ACTION REQUIRED: Your last parameters did NOT work. You MUST choose DIFFERENT values.")
        if prev_bias is not None:
            if prev_bias >= 0:
                lines.append(f"  SUGGESTION: Try negative admission_bias (e.g. {max(-5, prev_bias - 2)}) to preserve popular items")
            else:
                lines.append(f"  SUGGESTION: Try positive admission_bias (e.g. {min(5, prev_bias + 2)}) to admit more items")
        else:
            lines.append(f"  SUGGESTION: Try admission_bias=2 to start adapting")
    elif improvement > 0:
        lines.append(f"  RESULT: SUCCESS (improvement={improvement:+.4f})")
        lines.append(f"  Keep similar parameters or fine-tune slightly")
    lines.append("")

    # Parameters reference - ultra compact
    lines.append("PARAMETERS (set null to keep unchanged):")
    lines.append("- admission_bias [-5..+5]: KEY PARAM. +bias=admit more, -bias=keep popular. 0=standard")
    lines.append("- decay_factor [0.75..1.0]: Scale down frequencies. Lower=forget faster")
    lines.append("- reset_interval [50000..500000]: Accesses before halving data")
    lines.append("- reset_sketch [true|null]: Emergency reset (avoid)")
    lines.append("")
    
    # Decision framework
    if volatility > 0.5:
        lines.append("WORKLOAD: VOLATILE - try positive admission_bias (+1 to +3)")
    elif volatility < 0.2:
        lines.append("WORKLOAD: STABLE - try negative admission_bias (-1 to -2)")
    else:
        lines.append("WORKLOAD: MODERATE - experiment with admission_bias (-2 to +2)")
    lines.append("")

    lines.append('Respond with JSON: {"reasoning":"...", "tinylfu_control":{"admission_bias":N, "decay_factor":N, "reset_interval":N, "reset_sketch":null}}')

    return "\n".join(lines)

def build_global_prompt(snapshot=None, cache_full=None, previous_policy=None, metrics_feedback=None, snapshot_history=None, current_adaptation_state=None, best_strategy=None):
    lines = []
    
    lines.append("GOAL: Tune TinyLFU parameters so Aura cache BEATS the Baseline cache.")
    lines.append("SUCCESS = positive improvement (Aura HR > Baseline HR).")
    lines.append("")

    # Current metrics
    baseline_hr = 0.0
    aura_hr = 0.0
    improvement = 0.0
    if metrics_feedback and "baseline_vs_aura" in metrics_feedback:
        baseline_data = metrics_feedback["baseline_vs_aura"]
        baseline_hr = baseline_data.get("baseline", 0.0)
        aura_hr = baseline_data.get("aura", 0.0)
        improvement = baseline_data.get("improvement", 0.0)

    volatility = snapshot.get('workload_volatility', 0.0) if snapshot else 0.0
    total_events = snapshot.get('total_events', 0) if snapshot else 0
    cache_usage = cache_full if cache_full is not None else 0.0

    lines.append("=== CURRENT METRICS ===")
    lines.append(f"Baseline HR: {baseline_hr:.4f} | Aura HR: {aura_hr:.4f} | Improvement: {improvement:+.4f}")
    lines.append(f"Volatility: {volatility:.4f} | Cache Usage: {cache_usage:.1f}% | Events: {total_events}")
    
    if snapshot and "baseline_comparison" in snapshot:
        bc = snapshot["baseline_comparison"]
        delta_b = bc.get("delta_baseline_hit_ratio")
        delta_a = bc.get("delta_aura_hit_ratio")
        if delta_b is not None and delta_a is not None:
            lines.append(f"Trend: Baseline delta={delta_b:+.4f}, Aura delta={delta_a:+.4f}")
    lines.append("")

    # TinyLFU stats
    if snapshot and "tinylfu_stats" in snapshot:
        ts = snapshot.get("tinylfu_stats", {})
        lines.append(f"Evictions: {ts.get('eviction_count', 0)} | Ghost Hits: {ts.get('ghost_hits', 0)} | Efficiency: {ts.get('efficiency', 1.0):.4f}")
        lines.append("")

    # Previous decision + explicit feedback loop
    prev_bias = None
    prev_decay = None
    prev_reset = None
    prev_reset_sketch = None
    if previous_policy and previous_policy.get("tinylfu_control"):
        t = previous_policy["tinylfu_control"]
        prev_bias = t.get('admission_bias')
        prev_decay = t.get('decay_factor')
        prev_reset = t.get('reset_interval')
        prev_reset_sketch = t.get('reset_sketch')

    lines.append("=== YOUR LAST DECISION ===")
    lines.append(f"admission_bias={prev_bias}, decay_factor={prev_decay}, reset_interval={prev_reset}, reset_sketch={prev_reset_sketch}")
    
    if improvement < -0.001:
        lines.append(f"VERDICT: BAD - Aura is LOSING to baseline by {abs(improvement):.4f}")
        lines.append("You MUST change your strategy. Try different admission_bias values.")
    elif improvement < 0:
        lines.append(f"VERDICT: SLIGHTLY NEGATIVE ({improvement:+.4f}) - adjust parameters")
    elif improvement == 0:
        lines.append(f"VERDICT: NO EFFECT - your parameters are not making a difference")
        lines.append("You MUST try a different admission_bias value to create divergence.")
    else:
        lines.append(f"VERDICT: GOOD ({improvement:+.4f}) - keep or fine-tune")
    lines.append("")

    # History with admission_bias
    if snapshot_history and len(snapshot_history) > 0:
        lines.append("=== DECISION HISTORY (newest first) ===")
        for i, hist_entry in enumerate(reversed(snapshot_history[-5:]), 1):
            params = hist_entry.get("parameters_applied", {})
            bias = params.get("admission_bias")
            decay = params.get("decay_factor")
            reset = params.get("reset_interval")
            hr = hist_entry.get("hit_ratio_aura", 0.0)
            imp = hist_entry.get("improvement_vs_baseline", 0.0)
            vol = hist_entry.get("workload_volatility", 0.0)
            
            status = "OK" if imp > 0 else "FAIL" if imp < 0 else "NEUTRAL"
            lines.append(f"  T-{i}: bias={bias}, decay={decay}, reset={reset} → imp={imp:+.4f} [{status}]")
        
        # Anti-repetition check
        recent_params = [h.get("parameters_applied", {}) for h in snapshot_history[-3:]]
        if len(recent_params) >= 3:
            biases = [p.get("admission_bias") for p in recent_params]
            if len(set(str(b) for b in biases)) == 1:
                lines.append(f"  WARNING: You used admission_bias={biases[0]} for 3+ times. CHANGE IT NOW.")
        lines.append("")

    # Best strategy
    if best_strategy and best_strategy.get("improvement_vs_baseline", 0) > 0:
        bs = best_strategy
        lines.append(f"=== BEST RESULT SO FAR ===")
        bs_params = bs.get("parameters_applied", {})
        lines.append(f"admission_bias={bs_params.get('admission_bias')}, decay={bs_params.get('decay_factor')} → improvement={bs.get('improvement_vs_baseline', 0):+.4f}")
        lines.append("")

    # Parameters - concise
    lines.append("=== PARAMETERS ===")
    lines.append("admission_bias (int, -5 to +5): MOST IMPACTFUL")
    lines.append("  +bias → admit more new items (good for volatile/changing workloads)")
    lines.append("  -bias → keep popular items longer (good for stable workloads)")
    lines.append("  Formula: admit if (new_freq + bias) > victim_freq")
    lines.append("")
    lines.append("decay_factor (float, 0.75-1.0): Scale down all frequencies")
    lines.append("reset_interval (int, 50000-500000): Accesses before halving frequencies")
    lines.append("reset_sketch (true|null): Emergency full reset (max 1 per 5 calls)")
    lines.append("")

    # Decision guidance based on current state
    lines.append("=== WHAT TO DO NOW ===")
    if improvement <= 0 and prev_bias is not None:
        opposite = -prev_bias if prev_bias != 0 else 1
        lines.append(f"Step 1: Your bias={prev_bias} didn't work. Try bias={opposite}")
        lines.append(f"Step 2: Adjust decay_factor (try {0.95 if prev_decay and prev_decay < 0.9 else 0.80})")
        lines.append(f"Step 3: Write reasoning explaining WHY you chose different values")
    elif improvement <= 0:
        if volatility > 0.5:
            lines.append("Step 1: High volatility → try admission_bias=+2 (admit new patterns faster)")
        else:
            lines.append("Step 1: Low volatility → try admission_bias=-1 (preserve popular items)")
        lines.append("Step 2: Set decay_factor=0.90, reset_interval=100000")
        lines.append("Step 3: Write reasoning explaining your analysis")
    else:
        lines.append(f"Current strategy works! Fine-tune admission_bias around {prev_bias}")
        lines.append(f"Try bias={prev_bias+1 if prev_bias and prev_bias < 5 else prev_bias} or bias={prev_bias-1 if prev_bias and prev_bias > -5 else prev_bias}")
    lines.append("")

    lines.append('OUTPUT: {"reasoning":"...", "tinylfu_control":{"admission_bias":N, "decay_factor":N, "reset_interval":N, "reset_sketch":null}}')
    lines.append("")
    lines.append("CONSTRAINTS: admission_bias∈[-5,5], decay_factor∈[0.75,1.0], reset_interval∈[50000,500000]")
    
    return "\n".join(lines)

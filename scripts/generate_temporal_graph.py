"""
Script per generare grafici temporali delle performance delle cache.
Legge i dati salvati durante le chiamate LLM e genera un grafico che mostra
l'andamento delle hit rate delle due cache nel tempo, insieme ai parametri TinyLFU applicati.
"""
import json
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for Docker environments
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
import sys
import time

def load_temporal_metrics(metrics_file="data/temporal_metrics/llm_calls_temporal.json"):
    """Carica le metriche temporali dal file JSON."""
    metrics_path = Path(metrics_file)
    if not metrics_path.exists():
        print(f"ERROR: Metrics file not found: {metrics_file}")
        return None
    
    try:
        with open(metrics_path, 'r') as f:
            metrics = json.load(f)
        return metrics
    except Exception as e:
        print(f"ERROR: Failed to load metrics: {e}")
        return None

def generate_graph(metrics, output_path=None, test_name=None, llm_model=None):
    """Genera grafici separati per ogni metrica temporale."""
    if not metrics or len(metrics) == 0:
        print("ERROR: No metrics data available")
        return
    
    try:
        timestamps = []
        baseline_hr = []
        aura_hr = []
        improvement = []
        decay_factors = []
        reset_intervals = []
        reset_sketch_applied = []
        total_events_list = []
        volatility_list = []
        adaptation_states = []
        
        start_time = metrics[0]["timestamp"] if metrics else time.time()
        
        cumulative_baseline_hits = 0
        cumulative_baseline_misses = 0
        cumulative_aura_hits = 0
        cumulative_aura_misses = 0
        
        for i, metric in enumerate(metrics):
            timestamp = metric.get("timestamp", 0)
            relative_time = timestamp - start_time
            
            baseline_hits = metric.get("baseline_hits", 0)
            baseline_misses = metric.get("baseline_misses", 0)
            aura_hits = metric.get("aura_hits", 0)
            aura_misses = metric.get("aura_misses", 0)
            
            cumulative_baseline_hits += baseline_hits
            cumulative_baseline_misses += baseline_misses
            cumulative_aura_hits += aura_hits
            cumulative_aura_misses += aura_misses
            
            baseline_hr_window = metric.get("baseline_hit_ratio", 0.0)
            aura_hr_window = metric.get("aura_hit_ratio", 0.0)
            
            baseline_hr_cumulative = cumulative_baseline_hits / (cumulative_baseline_hits + cumulative_baseline_misses) if (cumulative_baseline_hits + cumulative_baseline_misses) > 0 else 0.0
            aura_hr_cumulative = cumulative_aura_hits / (cumulative_aura_hits + cumulative_aura_misses) if (cumulative_aura_hits + cumulative_aura_misses) > 0 else 0.0
            
            timestamps.append(relative_time)
            baseline_hr.append(baseline_hr_cumulative)
            aura_hr.append(aura_hr_cumulative)
            improvement.append(aura_hr_cumulative - baseline_hr_cumulative)
            
            decay_factors.append(metric.get("decay_factor"))
            reset_intervals.append(metric.get("reset_interval"))
            reset_sketch_applied.append(metric.get("reset_sketch", False))
            total_events_list.append(metric.get("total_events", 0))
            volatility_list.append(metric.get("workload_volatility", 0.0))
            adaptation_states.append(metric.get("adaptation_state", "UNKNOWN"))
        
        if output_path:
            output_dir = Path(output_path)
            if output_dir.suffix:  # È un file, usa la directory
                output_dir = output_dir.parent
            output_dir.mkdir(parents=True, exist_ok=True)
        else:
            output_dir = Path("data/temporal_graphs")
            if test_name:
                output_dir = output_dir / test_name
            output_dir.mkdir(parents=True, exist_ok=True)
        
        title_suffix = ""
        if test_name:
            title_suffix += f" - {test_name}"
        if llm_model:
            title_suffix += f" ({llm_model})"
        
        generated_files = []
        
        # 1. Hit Rate Cumulative (con evidenziazione reset)
        reset_sketch_times = [timestamps[i] for i, rs in enumerate(reset_sketch_applied) if rs is True]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(timestamps, baseline_hr, label="Baseline (TinyLFU) - Cumulative", marker='o', markersize=4, linewidth=2, color='#1f77b4', alpha=0.8)
        ax.plot(timestamps, aura_hr, label="AURA (TinyLFU + LLM) - Cumulative", marker='s', markersize=4, linewidth=2, color='#ff7f0e', alpha=0.8)
        
        # Evidenzia zone dopo i reset
        if reset_sketch_times:
            for i, rs_time in enumerate(reset_sketch_times):
                # Trova il prossimo reset o la fine
                next_reset = reset_sketch_times[i + 1] if i + 1 < len(reset_sketch_times) else timestamps[-1] if timestamps else rs_time
                # Ombreggia la zona per 30 secondi dopo il reset
                ax.axvspan(rs_time, min(rs_time + 30, next_reset), alpha=0.15, color='red', label='Post-Reset Zone' if i == 0 else '')
                # Linea verticale al momento del reset
                ax.axvline(x=rs_time, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Sketch Reset' if i == 0 else '')
        
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel("Cumulative Hit Rate", fontsize=12)
        title = f"Cache Hit Rate Over Time (Cumulative){title_suffix}"
        if reset_sketch_times:
            title += f" ({len(reset_sketch_times)} reset(s))"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_ylim([0, 1])
        output_file = output_dir / "hit_rate_cumulative.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 2. Improvement Over Baseline (con evidenziazione reset)
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(timestamps, improvement, label="Improvement (AURA - Baseline)", marker='^', markersize=4, linewidth=2, color='#2ca02c', alpha=0.8)
        ax.axhline(y=0, color='r', linestyle='--', alpha=0.5, linewidth=1)
        ax.fill_between(timestamps, improvement, 0, where=[i >= 0 for i in improvement], alpha=0.3, color='green', label='Positive improvement')
        ax.fill_between(timestamps, improvement, 0, where=[i < 0 for i in improvement], alpha=0.3, color='red', label='Negative improvement')
        
        # Evidenzia zone dopo i reset
        if reset_sketch_times:
            for i, rs_time in enumerate(reset_sketch_times):
                next_reset = reset_sketch_times[i + 1] if i + 1 < len(reset_sketch_times) else timestamps[-1] if timestamps else rs_time
                ax.axvspan(rs_time, min(rs_time + 30, next_reset), alpha=0.15, color='orange', label='Post-Reset Zone' if i == 0 else '')
                ax.axvline(x=rs_time, color='red', linestyle='--', linewidth=2, alpha=0.7, label='Sketch Reset' if i == 0 else '')
        
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel("Improvement", fontsize=12)
        title = f"Improvement Over Baseline (Cumulative){title_suffix}"
        if reset_sketch_times:
            title += f" ({len(reset_sketch_times)} reset(s))"
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=11)
        ax.grid(True, alpha=0.3)
        output_file = output_dir / "improvement.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 3. Sketch Reset Events (nuovo grafico dedicato)
        reset_sketch_times = [timestamps[i] for i, rs in enumerate(reset_sketch_applied) if rs is True]
        reset_sketch_indices = [i for i, rs in enumerate(reset_sketch_applied) if rs is True]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        if reset_sketch_times:
            # Mostra i reset come eventi temporali
            ax.scatter(reset_sketch_times, [1] * len(reset_sketch_times), marker='X', s=200, color='red', 
                      linewidths=2, edgecolors='darkred', zorder=5, label='Sketch Reset Events')
            
            # Mostra anche l'hit rate al momento del reset
            reset_hr_aura = [aura_hr[i] for i in reset_sketch_indices]
            reset_hr_baseline = [baseline_hr[i] for i in reset_sketch_indices]
            
            ax.scatter(reset_sketch_times, reset_hr_aura, marker='s', s=100, color='#ff7f0e', 
                      alpha=0.8, zorder=4, label='AURA HR at Reset')
            ax.scatter(reset_sketch_times, reset_hr_baseline, marker='o', s=100, color='#1f77b4', 
                      alpha=0.8, zorder=4, label='Baseline HR at Reset')
            
            # Linee che collegano i punti
            for i, rs_time in enumerate(reset_sketch_times):
                ax.plot([rs_time, rs_time], [reset_hr_baseline[i], reset_hr_aura[i]], 
                       'k--', alpha=0.3, linewidth=1)
            
            ax.set_ylabel("Hit Rate / Reset Event", fontsize=12)
            ax.set_title(f"Sketch Reset Events and Hit Rates{title_suffix} ({len(reset_sketch_times)} reset(s))", 
                        fontsize=14, fontweight='bold')
            ax.legend(loc='best', fontsize=11)
            ax.grid(True, alpha=0.3)
            ax.set_ylim([0, 1.1])
        else:
            ax.text(0.5, 0.5, 'No sketch resets occurred', ha='center', va='center', 
                   transform=ax.transAxes, fontsize=14)
            ax.set_title(f"Sketch Reset Events{title_suffix}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        output_file = output_dir / "sketch_resets.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 4. Decay Factor
        valid_decay = [d for d in decay_factors if d is not None]
        decay_times = [timestamps[i] for i, d in enumerate(decay_factors) if d is not None]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        if valid_decay:
            ax.plot(decay_times, valid_decay, marker='o', markersize=6, linewidth=2, color='#d62728', label='decay_factor')
            if reset_sketch_times:
                for rs_time in reset_sketch_times:
                    ax.axvline(x=rs_time, color='red', linestyle='--', alpha=0.5, linewidth=1, label='Sketch Reset' if rs_time == reset_sketch_times[0] else '')
            ax.set_ylabel("Decay Factor", fontsize=12)
            ax.set_title(f"TinyLFU Decay Factor Over Time{title_suffix}" + (" (red lines = sketch reset)" if reset_sketch_times else ""), fontsize=14, fontweight='bold')
            ax.legend(loc='best', fontsize=11)
            ax.grid(True, alpha=0.3)
            ax.set_ylim([0.7, 1.0])
        else:
            ax.text(0.5, 0.5, 'No decay_factor changes', ha='center', va='center', transform=ax.transAxes, fontsize=14)
            ax.set_title(f"TinyLFU Decay Factor Over Time{title_suffix}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        output_file = output_dir / "decay_factor.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 5. Reset Interval
        valid_reset = [r for r in reset_intervals if r is not None]
        reset_times = [timestamps[i] for i, r in enumerate(reset_intervals) if r is not None]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        if valid_reset:
            ax.plot(reset_times, valid_reset, marker='s', markersize=6, linewidth=2, color='#9467bd', label='reset_interval')
            if reset_sketch_times:
                for rs_time in reset_sketch_times:
                    ax.axvline(x=rs_time, color='red', linestyle='--', alpha=0.5, linewidth=1, label='Sketch Reset' if rs_time == reset_sketch_times[0] else '')
            ax.set_ylabel("Reset Interval", fontsize=12)
            ax.set_title(f"TinyLFU Reset Interval Over Time{title_suffix}" + (" (red lines = sketch reset)" if reset_sketch_times else ""), fontsize=14, fontweight='bold')
            ax.legend(loc='best', fontsize=11)
            ax.grid(True, alpha=0.3)
        else:
            ax.text(0.5, 0.5, 'No reset_interval changes', ha='center', va='center', transform=ax.transAxes, fontsize=14)
            ax.set_title(f"TinyLFU Reset Interval Over Time{title_suffix}", fontsize=14, fontweight='bold')
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        output_file = output_dir / "reset_interval.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 6. Workload Volatility
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(timestamps, volatility_list, label="Workload Volatility", marker='s', markersize=4, linewidth=2, color='#e377c2', alpha=0.8)
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel("Workload Volatility", fontsize=12)
        ax.set_title(f"Workload Volatility Over Time{title_suffix}", fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=11)
        ax.grid(True, alpha=0.3)
        ax.set_ylim([0, 1])
        output_file = output_dir / "workload_volatility.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        # 7. Total Events
        fig, ax = plt.subplots(figsize=(12, 6))
        ax.plot(timestamps, total_events_list, label="Total Events", marker='o', markersize=4, linewidth=2, color='#8c564b', alpha=0.8)
        ax.set_xlabel("Time (seconds from start)", fontsize=12)
        ax.set_ylabel("Total Events", fontsize=12)
        ax.set_title(f"Total Events Processed Over Time{title_suffix}", fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=11)
        ax.grid(True, alpha=0.3)
        output_file = output_dir / "total_events.png"
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        plt.close()
        generated_files.append(output_file)
        print(f"Graph saved: {output_file}")
        
        print(f"\nGenerated {len(generated_files)} graphs in: {output_dir}")
        
    except Exception as e:
        print(f"ERROR: Failed to generate graphs: {e}")
        import traceback
        traceback.print_exc()
        if 'plt' in locals():
            try:
                plt.close('all')
            except:
                pass
        raise

def generate_summary_stats(metrics):
    """Genera statistiche riassuntive dalle metriche temporali."""
    if not metrics or len(metrics) == 0:
        return None
    
    cumulative_baseline_hits = 0
    cumulative_baseline_misses = 0
    cumulative_aura_hits = 0
    cumulative_aura_misses = 0
    
    baseline_hrs_window = []
    aura_hrs_window = []
    improvements_window = []
    decay_factors = []
    reset_intervals = []
    
    for m in metrics:
        baseline_hits = m.get("baseline_hits", 0)
        baseline_misses = m.get("baseline_misses", 0)
        aura_hits = m.get("aura_hits", 0)
        aura_misses = m.get("aura_misses", 0)
        
        cumulative_baseline_hits += baseline_hits
        cumulative_baseline_misses += baseline_misses
        cumulative_aura_hits += aura_hits
        cumulative_aura_misses += aura_misses
        
        baseline_hr_window = m.get("baseline_hit_ratio", 0.0)
        aura_hr_window = m.get("aura_hit_ratio", 0.0)
        
        baseline_hrs_window.append(baseline_hr_window)
        aura_hrs_window.append(aura_hr_window)
        improvements_window.append(aura_hr_window - baseline_hr_window)
        
        if m.get("decay_factor") is not None:
            decay_factors.append(m.get("decay_factor"))
        if m.get("reset_interval") is not None:
            reset_intervals.append(m.get("reset_interval"))
    
    baseline_hr_cumulative = cumulative_baseline_hits / (cumulative_baseline_hits + cumulative_baseline_misses) if (cumulative_baseline_hits + cumulative_baseline_misses) > 0 else 0.0
    aura_hr_cumulative = cumulative_aura_hits / (cumulative_aura_hits + cumulative_aura_misses) if (cumulative_aura_hits + cumulative_aura_misses) > 0 else 0.0
    
    stats = {
        "total_llm_calls": len(metrics),
        "baseline_hr_cumulative": baseline_hr_cumulative,
        "aura_hr_cumulative": aura_hr_cumulative,
        "improvement_cumulative": aura_hr_cumulative - baseline_hr_cumulative,
        "baseline_hr_window": {
            "mean": sum(baseline_hrs_window) / len(baseline_hrs_window) if baseline_hrs_window else 0.0,
            "min": min(baseline_hrs_window) if baseline_hrs_window else 0.0,
            "max": max(baseline_hrs_window) if baseline_hrs_window else 0.0,
            "final": baseline_hrs_window[-1] if baseline_hrs_window else 0.0
        },
        "aura_hr_window": {
            "mean": sum(aura_hrs_window) / len(aura_hrs_window) if aura_hrs_window else 0.0,
            "min": min(aura_hrs_window) if aura_hrs_window else 0.0,
            "max": max(aura_hrs_window) if aura_hrs_window else 0.0,
            "final": aura_hrs_window[-1] if aura_hrs_window else 0.0
        },
        "improvement_window": {
            "mean": sum(improvements_window) / len(improvements_window) if improvements_window else 0.0,
            "min": min(improvements_window) if improvements_window else 0.0,
            "max": max(improvements_window) if improvements_window else 0.0,
            "final": improvements_window[-1] if improvements_window else 0.0
        },
        "decay_factor": {
            "values": decay_factors,
            "count": len(decay_factors),
            "mean": sum(decay_factors) / len(decay_factors) if decay_factors else None,
            "min": min(decay_factors) if decay_factors else None,
            "max": max(decay_factors) if decay_factors else None
        },
        "reset_interval": {
            "values": reset_intervals,
            "count": len(reset_intervals),
            "mean": sum(reset_intervals) / len(reset_intervals) if reset_intervals else None,
            "min": min(reset_intervals) if reset_intervals else None,
            "max": max(reset_intervals) if reset_intervals else None
        }
    }
    
    return stats

def get_short_filename(test_name):
    """Genera un nome file breve e descrittivo basato sul nome del test."""
    if not test_name:
        return "cache_performance"
    
    name_mapping = {
        "normal_dataset": "normal",
        "burst_cooldown": "burst",
        "hot_cold_shift": "hotcold",
        "daily_pattern": "daily",
        "adversarial": "adversarial"
    }
    
    short_name = name_mapping.get(test_name, test_name.replace("_", ""))
    return f"{short_name}_performance"

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate temporal graphs for cache performance")
    parser.add_argument("--input", "-i", default="data/temporal_metrics/llm_calls_temporal.json",
                        help="Input metrics file path")
    parser.add_argument("--output", "-o", default=None,
                        help="Output graph file path")
    parser.add_argument("--test-name", "-t", default=None,
                        help="Test name to include in graph title")
    parser.add_argument("--llm-model", "-m", default=None,
                        help="LLM model name for folder organization")
    parser.add_argument("--stats", "-s", action="store_true",
                        help="Print summary statistics")
    
    args = parser.parse_args()
    
    metrics = load_temporal_metrics(args.input)
    if not metrics:
        sys.exit(1)
    
    if args.stats:
        stats = generate_summary_stats(metrics)
        if stats:
            print("\n=== Summary Statistics ===")
            print(f"Total LLM calls: {stats['total_llm_calls']}")
            print(f"\nCumulative Hit Rates:")
            print(f"  Baseline: {stats['baseline_hr_cumulative']:.4f}")
            print(f"  AURA: {stats['aura_hr_cumulative']:.4f}")
            print(f"  Improvement: {stats['improvement_cumulative']:+.4f}")
            print(f"\nWindow Hit Rates (mean):")
            print(f"  Baseline: {stats['baseline_hr_window']['mean']:.4f}")
            print(f"  AURA: {stats['aura_hr_window']['mean']:.4f}")
            print(f"  Improvement: {stats['improvement_window']['mean']:+.4f}")
            if stats['decay_factor']['count'] > 0:
                print(f"\nDecay Factor:")
                print(f"  Changes: {stats['decay_factor']['count']}")
                print(f"  Mean: {stats['decay_factor']['mean']:.4f}")
                print(f"  Range: [{stats['decay_factor']['min']:.4f}, {stats['decay_factor']['max']:.4f}]")
            if stats['reset_interval']['count'] > 0:
                print(f"\nReset Interval:")
                print(f"  Changes: {stats['reset_interval']['count']}")
                print(f"  Mean: {stats['reset_interval']['mean']:.0f}")
                print(f"  Range: [{stats['reset_interval']['min']:.0f}, {stats['reset_interval']['max']:.0f}]")
    
    output_path = args.output
    if output_path is None:
        base_dir = Path("data/temporal_graphs")
        if args.llm_model:
            llm_model_safe = args.llm_model.replace(":", "_").replace("/", "_")
            base_dir = base_dir / llm_model_safe
        if args.test_name:
            base_dir = base_dir / args.test_name
        output_path = base_dir
    else:
        output_path = Path(output_path)
    
    generate_graph(metrics, output_path, args.test_name, args.llm_model)

if __name__ == "__main__":
    main()

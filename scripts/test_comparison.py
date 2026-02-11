"""
Script per test comparativi tra TinyLFU baseline e TinyLFU+LLM (AURA).
Esegue test su:
1. Dataset normale (primi 100k eventi)
2. Ogni scenario generato (100k eventi per scenario)

Salva risultati in formato JSON per analisi successive.
"""
import os
import sys
import json
import time
import argparse
from pathlib import Path
from typing import Dict, List, Optional

# Aggiungi il path del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.modules.scenario_loader import ScenarioLoader
from src.modules.limited_dataset_loader import LimitedDatasetLoader
from src.modules.preprocessed_loader import PreprocessedLoader
from src.core.db import get_redis_aura, get_redis_lru
import redis
from src.modules.tinylfu import TinyLFU
from src.core.config import (
    TINYLFU_SKETCH_WIDTH, TINYLFU_SKETCH_DEPTH, TINYLFU_DOORKEEPER_SIZE,
    TINYLFU_RESET_INTERVAL, TINYLFU_SAMPLE_SIZE, FLUSH_ON_STARTUP
)


class ComparisonTester:
    """
    Esegue test comparativi tra TinyLFU baseline e TinyLFU+LLM.
    """
    
    def __init__(self, max_events: int = 1000000):
        """
        Args:
            max_events: Numero massimo di eventi da processare per test
        """
        self.max_events = max_events
        self.results_dir = project_root / "data" / "test_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        
        # Inizializza Redis (prova connessione Docker, altrimenti localhost)
        try:
            self.r_aura = get_redis_aura()
            self.r_baseline = get_redis_lru()
            # Test connessione
            self.r_aura.ping()
            self.r_baseline.ping()
            print("[COMPARISON_TESTER] Connected to Redis (Docker)")
        except Exception:
            # Fallback a localhost per test locali
            print("[COMPARISON_TESTER] Docker Redis not available, using localhost")
            self.r_aura = redis.Redis(host="localhost", port=6379, encoding="utf-8", decode_responses=True)
            self.r_baseline = redis.Redis(host="localhost", port=6380, encoding="utf-8", decode_responses=True)
            try:
                self.r_aura.ping()
                self.r_baseline.ping()
                print("[COMPARISON_TESTER] Connected to Redis (localhost)")
            except Exception as e:
                print(f"[ERROR] Cannot connect to Redis: {e}")
                raise
        
        print(f"[COMPARISON_TESTER] Initialized with max_events={max_events}")
        print(f"[COMPARISON_TESTER] Results will be saved to: {self.results_dir}")
    
    def run_test(self, test_name: str, event_generator, metadata: Optional[Dict] = None) -> Dict:
        """
        Esegue un singolo test comparativo.
        
        Args:
            test_name: Nome identificativo del test
            event_generator: Generatore di eventi (item_id, user, action, timestamp)
            metadata: Metadata aggiuntive da includere nei risultati
        
        Returns:
            Dizionario con i risultati del test
        """
        print(f"\n{'='*60}")
        print(f"[TEST] Starting: {test_name}")
        print(f"{'='*60}")
        
        # Reset Redis
        if FLUSH_ON_STARTUP:
            self.r_aura.flushdb()
            self.r_baseline.flushdb()
            print("[TEST] Flushed Redis databases")
        
        # Inizializza TinyLFU instances
        tinylfu_baseline = TinyLFU(
            redis_client=self.r_baseline,
            sketch_width=TINYLFU_SKETCH_WIDTH,
            sketch_depth=TINYLFU_SKETCH_DEPTH,
            doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
            reset_interval=TINYLFU_RESET_INTERVAL,
            sample_size=TINYLFU_SAMPLE_SIZE
        )
        
        tinylfu_aura = TinyLFU(
            redis_client=self.r_aura,
            sketch_width=TINYLFU_SKETCH_WIDTH,
            sketch_depth=TINYLFU_SKETCH_DEPTH,
            doorkeeper_size=TINYLFU_DOORKEEPER_SIZE,
            reset_interval=TINYLFU_RESET_INTERVAL,
            sample_size=TINYLFU_SAMPLE_SIZE
        )
        
        # Metriche
        baseline_hits = 0
        baseline_misses = 0
        aura_hits = 0
        aura_misses = 0
        
        events_processed = 0
        start_time = time.time()
        
        # Processa eventi
        print(f"[TEST] Processing events...")
        for item_id, user, action, timestamp in event_generator:
            if events_processed >= self.max_events:
                break
            
            item_key = f"item:{item_id}"
            
            # Test TinyLFU baseline
            try:
                baseline_hit = tinylfu_baseline.exists(item_key)
                if baseline_hit:
                    baseline_hits += 1
                else:
                    baseline_misses += 1
                    # Inserisci in cache con TTL fisso
                    tinylfu_baseline.set(item_key, "1", ttl=30)
            except Exception as e:
                baseline_misses += 1
                print(f"[WARNING] Baseline error: {e}")
            
            # Test TinyLFU AURA (stesso comportamento per ora, senza LLM)
            try:
                aura_hit = tinylfu_aura.exists(item_key)
                if aura_hit:
                    aura_hits += 1
                else:
                    aura_misses += 1
                    # Inserisci in cache con TTL fisso
                    tinylfu_aura.set(item_key, "1", ttl=30)
            except Exception as e:
                aura_misses += 1
                print(f"[WARNING] AURA error: {e}")
            
            events_processed += 1
            
            if events_processed % 10000 == 0:
                print(f"[TEST] Processed {events_processed}/{self.max_events} events...")
        
        elapsed_time = time.time() - start_time
        
        # Calcola metriche
        baseline_total = baseline_hits + baseline_misses
        aura_total = aura_hits + aura_misses
        
        baseline_hit_rate = baseline_hits / baseline_total if baseline_total > 0 else 0.0
        aura_hit_rate = aura_hits / aura_total if aura_total > 0 else 0.0
        improvement = aura_hit_rate - baseline_hit_rate
        improvement_percent = (improvement / baseline_hit_rate * 100) if baseline_hit_rate > 0 else 0.0
        
        results = {
            "test_name": test_name,
            "timestamp": time.time(),
            "events_processed": events_processed,
            "elapsed_time_seconds": elapsed_time,
            "events_per_second": events_processed / elapsed_time if elapsed_time > 0 else 0,
            "baseline": {
                "hits": baseline_hits,
                "misses": baseline_misses,
                "total": baseline_total,
                "hit_rate": baseline_hit_rate
            },
            "aura": {
                "hits": aura_hits,
                "misses": aura_misses,
                "total": aura_total,
                "hit_rate": aura_hit_rate
            },
            "comparison": {
                "improvement": improvement,
                "improvement_percent": improvement_percent
            }
        }
        
        if metadata:
            results["metadata"] = metadata
        
        print(f"\n[TEST] Results for {test_name}:")
        print(f"  Events processed: {events_processed}")
        print(f"  Time elapsed: {elapsed_time:.2f}s ({events_processed/elapsed_time:.0f} events/s)")
        print(f"  Baseline hit rate: {baseline_hit_rate:.4f} ({baseline_hits}/{baseline_total})")
        print(f"  AURA hit rate: {aura_hit_rate:.4f} ({aura_hits}/{aura_total})")
        print(f"  Improvement: {improvement:+.4f} ({improvement_percent:+.2f}%)")
        
        return results
    
    def test_normal_dataset(self, use_preprocessed: bool = True) -> Dict:
        """Test sul dataset normale (primi 100k eventi)."""
        # Prova a usare file pre-processato se disponibile
        if use_preprocessed:
            preprocessed_path = project_root / "data" / "test_data" / "normal_dataset_100k.json"
            if preprocessed_path.exists():
                print(f"[TEST] Using preprocessed file: {preprocessed_path.name}")
                loader = PreprocessedLoader(str(preprocessed_path))
                metadata = loader.load_metadata()
                metadata["dataset_type"] = "normal"
                metadata["max_events"] = self.max_events
                return self.run_test(
                    "normal_dataset",
                    loader.generate_events(),
                    metadata=metadata
                )
        
        # Fallback a loader normale
        print(f"[TEST] Using live dataset loader (max_events={self.max_events})")
        loader = LimitedDatasetLoader(max_events=self.max_events)
        return self.run_test(
            "normal_dataset",
            loader.generate_events(),
            metadata={"dataset_type": "normal", "max_events": self.max_events}
        )
    
    def test_scenario(self, scenario_path: str, use_preprocessed: bool = True) -> Dict:
        """Test su uno scenario specifico."""
        scenario_name = Path(scenario_path).stem
        
        # Prova a usare file pre-processato se disponibile
        if use_preprocessed:
            preprocessed_path = project_root / "data" / "test_data" / f"{scenario_name}_100k.json"
            if preprocessed_path.exists():
                print(f"[TEST] Using preprocessed file: {preprocessed_path.name}")
                loader = PreprocessedLoader(str(preprocessed_path))
                metadata = loader.load_metadata()
                metadata["max_events"] = self.max_events
                return self.run_test(
                    f"scenario_{scenario_name}",
                    loader.generate_events(),
                    metadata=metadata
                )
        
        # Fallback a loader normale
        print(f"[TEST] Using live scenario loader (max_events={self.max_events})")
        loader = ScenarioLoader(scenario_path, max_events=self.max_events)
        metadata = loader.load_metadata()
        metadata["max_events"] = self.max_events
        
        return self.run_test(
            f"scenario_{scenario_name}",
            loader.generate_events(),
            metadata=metadata
        )
    
    def run_all_tests(self) -> List[Dict]:
        """
        Esegue tutti i test: dataset normale + tutti gli scenari.
        
        Returns:
            Lista di risultati per ogni test
        """
        all_results = []
        
        # Test dataset normale
        print("\n" + "="*60)
        print("TEST 1: Normal Dataset")
        print("="*60)
        try:
            result = self.test_normal_dataset()
            all_results.append(result)
        except Exception as e:
            print(f"[ERROR] Failed to test normal dataset: {e}")
            import traceback
            traceback.print_exc()
        
        # Test scenari
        scenarios_dir = project_root / "data" / "scenarios" / "final"
        if scenarios_dir.exists():
            scenario_files = sorted(scenarios_dir.glob("*.json"))
            print(f"\n[COMPARISON_TESTER] Found {len(scenario_files)} scenarios to test")
            
            for i, scenario_file in enumerate(scenario_files, start=2):
                print("\n" + "="*60)
                print(f"TEST {i}: Scenario {scenario_file.name}")
                print("="*60)
                try:
                    result = self.test_scenario(str(scenario_file))
                    all_results.append(result)
                except Exception as e:
                    print(f"[ERROR] Failed to test scenario {scenario_file.name}: {e}")
                    import traceback
                    traceback.print_exc()
        else:
            print(f"[WARNING] Scenarios directory not found: {scenarios_dir}")
        
        return all_results
    
    def save_results(self, results: List[Dict], filename: Optional[str] = None):
        """
        Salva i risultati in un file JSON.
        
        Args:
            results: Lista di risultati da salvare
            filename: Nome del file (default: timestamp-based)
        """
        if filename is None:
            timestamp = int(time.time())
            filename = f"comparison_results_{timestamp}.json"
        
        output_path = self.results_dir / filename
        
        output_data = {
            "test_run": {
                "timestamp": time.time(),
                "max_events_per_test": self.max_events,
                "total_tests": len(results)
            },
            "results": results
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n[COMPARISON_TESTER] Results saved to: {output_path}")
        return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Esegue test comparativi tra TinyLFU baseline e TinyLFU+LLM"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=1000000,
        help="Numero massimo di eventi da processare per test (default: 1000000)"
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Testa solo uno scenario specifico (path al file JSON)"
    )
    parser.add_argument(
        "--normal-only",
        action="store_true",
        help="Testa solo il dataset normale"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Nome del file di output (default: auto-generato)"
    )
    
    args = parser.parse_args()
    
    tester = ComparisonTester(max_events=args.max_events)
    
    if args.scenario:
        # Test singolo scenario
        result = tester.test_scenario(args.scenario)
        tester.save_results([result], filename=args.output)
    elif args.normal_only:
        # Solo dataset normale
        result = tester.test_normal_dataset()
        tester.save_results([result], filename=args.output)
    else:
        # Tutti i test
        results = tester.run_all_tests()
        tester.save_results(results, filename=args.output)
    
    print("\n[COMPARISON_TESTER] All tests completed!")


if __name__ == "__main__":
    main()

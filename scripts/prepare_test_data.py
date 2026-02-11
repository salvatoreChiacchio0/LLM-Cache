"""
Script per preparare file di test da 100k eventi.
Estrae 100k eventi da:
1. Dataset normale
2. Ogni scenario generato

Salva i file in data/test_data/ per essere committati su Git.
"""
import os
import sys
import json
import csv
import argparse
import time
from pathlib import Path
from typing import Iterator, Tuple

# Aggiungi il path del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.modules.scenario_loader import ScenarioLoader
from src.modules.limited_dataset_loader import LimitedDatasetLoader
from src.core.config import LOG_FILE, LOG_DELIMITER, FILE_ENCODING


def save_events_to_csv(events: Iterator[Tuple[int, str, str, str]], output_path: Path, max_events: int):
    """
    Salva eventi in formato CSV compatibile con il sistema.
    
    Args:
        events: Generatore di eventi (item_id, user, action, timestamp)
        output_path: Path del file CSV di output
        max_events: Numero massimo di eventi da salvare
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    events_saved = 0
    with open(output_path, 'w', encoding=FILE_ENCODING, newline='') as f:
        writer = csv.writer(f, delimiter=LOG_DELIMITER)
        
        for item_id, user, action, timestamp in events:
            if events_saved >= max_events:
                break
            
            writer.writerow([str(item_id), user, action, timestamp])
            events_saved += 1
            
            if events_saved % 10000 == 0:
                print(f"  Saved {events_saved}/{max_events} events...")
    
    print(f"  [OK] Saved {events_saved} events to {output_path}")
    return events_saved


def save_events_to_json(events: Iterator[Tuple[int, str, str, str]], output_path: Path, max_events: int, metadata: dict = None):
    """
    Salva eventi in formato JSON compatibile con gli scenari.
    
    Args:
        events: Generatore di eventi (item_id, user, action, timestamp)
        output_path: Path del file JSON di output
        max_events: Numero massimo di eventi da salvare
        metadata: Metadata da includere nel file JSON
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    events_list = []
    events_saved = 0
    
    for item_id, user, action, timestamp in events:
        if events_saved >= max_events:
            break
        
        events_list.append({
            "item_id": str(item_id),
            "user_id": user,
            "action": action,
            "timestamp": timestamp
        })
        events_saved += 1
        
        if events_saved % 10000 == 0:
            print(f"  Saved {events_saved}/{max_events} events...")
    
    output_data = {
        "metadata": metadata or {},
        "events": events_list
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    print(f"  [OK] Saved {events_saved} events to {output_path}")
    return events_saved


def prepare_normal_dataset(max_events: int = 1000000, output_dir: Path = None):
    """
    Prepara file di test dal dataset normale.
    
    Args:
        max_events: Numero di eventi da estrarre
        output_dir: Directory di output (default: data/test_data/)
    """
    if output_dir is None:
        output_dir = project_root / "data" / "test_data"
    
    print(f"\n{'='*60}")
    print(f"Preparing normal dataset ({max_events} events)")
    print(f"{'='*60}")
    
    loader = LimitedDatasetLoader(max_events=max_events)
    output_path = output_dir / "normal_dataset_100k.csv"
    
    events_saved = save_events_to_csv(
        loader.generate_events(),
        output_path,
        max_events
    )
    
    # Crea anche un file JSON per coerenza
    loader = LimitedDatasetLoader(max_events=max_events)
    json_output_path = output_dir / "normal_dataset_100k.json"
    save_events_to_json(
        loader.generate_events(),
        json_output_path,
        max_events,
        metadata={
            "dataset_type": "normal",
            "source": LOG_FILE,
            "events_count": events_saved,
            "max_events": max_events
        }
    )
    
    return output_path, json_output_path


def prepare_scenario(scenario_path: Path, max_events: int = 1000000, output_dir: Path = None):
    """
    Prepara file di test da uno scenario.
    
    Args:
        scenario_path: Path al file JSON dello scenario
        max_events: Numero di eventi da estrarre
        output_dir: Directory di output (default: data/test_data/)
    
    Returns:
        Tuple di (csv_path, json_path)
    """
    if output_dir is None:
        output_dir = project_root / "data" / "test_data"
    
    scenario_name = scenario_path.stem
    print(f"\n{'='*60}")
    print(f"Preparing scenario: {scenario_name} ({max_events} events)")
    print(f"{'='*60}")
    
    loader = ScenarioLoader(str(scenario_path), max_events=max_events)
    metadata = loader.load_metadata()
    metadata["events_count"] = max_events
    metadata["max_events"] = max_events
    
    # Salva in formato JSON (mantiene la struttura originale)
    json_output_path = output_dir / f"{scenario_name}_100k.json"
    events_saved = save_events_to_json(
        loader.generate_events(),
        json_output_path,
        max_events,
        metadata=metadata
    )
    
    # Salva anche in formato CSV per compatibilità
    loader = ScenarioLoader(str(scenario_path), max_events=max_events)
    csv_output_path = output_dir / f"{scenario_name}_100k.csv"
    save_events_to_csv(
        loader.generate_events(),
        csv_output_path,
        max_events
    )
    
    return csv_output_path, json_output_path


def prepare_all(max_events: int = 1000000, output_dir: Path = None):
    """
    Prepara tutti i file di test: dataset normale + tutti gli scenari.
    
    Args:
        max_events: Numero di eventi da estrarre per ogni dataset
        output_dir: Directory di output (default: data/test_data/)
    """
    if output_dir is None:
        output_dir = project_root / "data" / "test_data"
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"\n{'='*60}")
    print(f"PREPARING TEST DATA ({max_events} events per dataset)")
    print(f"{'='*60}")
    
    prepared_files = []
    
    # 1. Dataset normale
    try:
        csv_path, json_path = prepare_normal_dataset(max_events, output_dir)
        prepared_files.append(("normal_dataset", csv_path, json_path))
    except Exception as e:
        print(f"[ERROR] Failed to prepare normal dataset: {e}")
        import traceback
        traceback.print_exc()
    
    # 2. Scenari
    scenarios_dir = project_root / "data" / "scenarios" / "final"
    if scenarios_dir.exists():
        scenario_files = sorted(scenarios_dir.glob("*.json"))
        print(f"\n[PREPARE] Found {len(scenario_files)} scenarios to prepare")
        
        for scenario_file in scenario_files:
            try:
                csv_path, json_path = prepare_scenario(scenario_file, max_events, output_dir)
                prepared_files.append((scenario_file.stem, csv_path, json_path))
            except Exception as e:
                print(f"[ERROR] Failed to prepare scenario {scenario_file.name}: {e}")
                import traceback
                traceback.print_exc()
    else:
        print(f"[WARNING] Scenarios directory not found: {scenarios_dir}")
    
    # Riepilogo
    print(f"\n{'='*60}")
    print("PREPARATION SUMMARY")
    print(f"{'='*60}")
    print(f"Total datasets prepared: {len(prepared_files)}")
    print(f"Output directory: {output_dir}")
    print("\nFiles created:")
    for name, csv_path, json_path in prepared_files:
        print(f"  - {name}:")
        print(f"      CSV:  {csv_path}")
        print(f"      JSON: {json_path}")
    
    # Crea file index
    index_path = output_dir / "index.json"
    index_data = {
        "max_events": max_events,
        "prepared_at": int(time.time()),
        "datasets": [
            {
                "name": name,
                "csv_path": str(csv_path.relative_to(project_root)),
                "json_path": str(json_path.relative_to(project_root))
            }
            for name, csv_path, json_path in prepared_files
        ]
    }
    
    with open(index_path, 'w', encoding='utf-8') as f:
        json.dump(index_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n[OK] Index saved to: {index_path}")
    print(f"\n[OK] All test data prepared! Files are ready to be committed to Git.")
    
    return prepared_files


def main():
    import time
    
    parser = argparse.ArgumentParser(
        description="Prepara file di test da 100k eventi per ogni dataset"
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=1000000,
        help="Numero di eventi da estrarre per dataset (default: 1000000)"
    )
    parser.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Prepara solo uno scenario specifico (path al file JSON)"
    )
    parser.add_argument(
        "--normal-only",
        action="store_true",
        help="Prepara solo il dataset normale"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Directory di output (default: data/test_data/)"
    )
    
    args = parser.parse_args()
    
    output_dir = Path(args.output_dir) if args.output_dir else None
    
    if args.scenario:
        # Prepara solo uno scenario
        scenario_path = Path(args.scenario)
        if not scenario_path.exists():
            print(f"[ERROR] Scenario file not found: {scenario_path}")
            return
        
        prepare_scenario(scenario_path, args.max_events, output_dir)
    elif args.normal_only:
        # Solo dataset normale
        prepare_normal_dataset(args.max_events, output_dir)
    else:
        # Tutti i dataset
        prepare_all(args.max_events, output_dir)


if __name__ == "__main__":
    main()

"""
Script per creare i 4 file di test da 100k eventi:
1. normal_dataset_100k (da log_15M_subset.txt)
2. 02_burst_cooldown_100k
3. 04_daily_pattern_100k
4. 01_hot_cold_shift_100k
"""
import os
import sys
import json
import csv
from pathlib import Path

# Aggiungi il path del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.modules.scenario_loader import ScenarioLoader
from src.modules.limited_dataset_loader import LimitedDatasetLoader
from src.core.config import LOG_FILE, LOG_DELIMITER, FILE_ENCODING


def create_test_file(dataset_name, source_path, max_events=1000000):
    """
    Crea un file di test da 100k eventi.
    
    Args:
        dataset_name: Nome del dataset (es: "normal_dataset", "02_burst_cooldown")
        source_path: Path al file sorgente
        max_events: Numero di eventi da estrarre (default: 1000000)
    
    Returns:
        Path al file JSON creato
    """
    test_data_dir = project_root / "data" / "test_data"
    test_data_dir.mkdir(parents=True, exist_ok=True)
    
    output_file = test_data_dir / f"{dataset_name}_100k.json"
    
    print(f"\n{'='*60}")
    print(f"Creating: {dataset_name}_100k.json")
    print(f"Source: {source_path}")
    print(f"{'='*60}")
    
    events_list = []
    
    # Carica eventi dal sorgente
    if source_path.suffix == '.json':
        # Scenario JSON
        loader = ScenarioLoader(str(source_path), max_events=max_events)
        metadata = loader.load_metadata()
        metadata["events_count"] = max_events
        metadata["max_events"] = max_events
        metadata["source_file"] = str(source_path)
        
        print(f"Loading events from scenario JSON...")
        for i, event in enumerate(loader.generate_events()):
            if i >= max_events:
                break
            item_id, user_id, action, timestamp = event
            events_list.append({
                "item_id": str(item_id),
                "user_id": user_id,
                "action": action,
                "timestamp": timestamp
            })
            if (i + 1) % 10000 == 0:
                print(f"  Loaded {i+1}/{max_events} events...")
    else:
        # CSV file (dataset normale)
        loader = LimitedDatasetLoader(log_file=str(source_path), max_events=max_events)
        metadata = {
            "dataset_type": "normal",
            "source": str(source_path),
            "events_count": max_events,
            "max_events": max_events
        }
        
        print(f"Loading events from CSV...")
        for i, event in enumerate(loader.generate_events()):
            if i >= max_events:
                break
            item_id, user_id, action, timestamp = event
            events_list.append({
                "item_id": str(item_id),
                "user_id": user_id,
                "action": action,
                "timestamp": timestamp
            })
            if (i + 1) % 10000 == 0:
                print(f"  Loaded {i+1}/{max_events} events...")
    
    # Salva in JSON
    output_data = {
        "metadata": metadata,
        "events": events_list
    }
    
    print(f"Saving {len(events_list)} events to {output_file.name}...")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)
    
    file_size = output_file.stat().st_size / (1024 * 1024)  # MB
    print(f"[OK] Created {output_file.name} ({len(events_list)} events, {file_size:.2f} MB)")
    
    return output_file


def main():
    """Crea i 4 file di test richiesti."""
    max_events = 1000000
    
    print("="*60)
    print("CREATING 4 TEST FILES (100k events each)")
    print("="*60)
    
    # Definisci i 4 file da creare
    test_files = [
        {
            "name": "normal_dataset",
            "source": project_root / "data" / "log_15M_subset.txt"
        },
        {
            "name": "02_burst_cooldown",
            "source": project_root / "data" / "scenarios" / "final" / "02_burst_cooldown.json"
        },
        {
            "name": "04_daily_pattern",
            "source": project_root / "data" / "scenarios" / "final" / "04_daily_pattern.json"
        },
        {
            "name": "01_hot_cold_shift",
            "source": project_root / "data" / "scenarios" / "final" / "01_hot_cold_shift.json"
        }
    ]
    
    created_files = []
    
    for test_config in test_files:
        source_path = test_config["source"]
        
        if not source_path.exists():
            print(f"\n[ERROR] Source file not found: {source_path}")
            print(f"        Skipping {test_config['name']}")
            continue
        
        try:
            output_file = create_test_file(
                test_config["name"],
                source_path,
                max_events=max_events
            )
            created_files.append((test_config["name"], output_file))
        except Exception as e:
            print(f"\n[ERROR] Failed to create {test_config['name']}: {e}")
            import traceback
            traceback.print_exc()
    
    # Riepilogo
    print(f"\n{'='*60}")
    print("SUMMARY")
    print(f"{'='*60}")
    print(f"Files created: {len(created_files)}/4")
    print("\nCreated files:")
    for name, file_path in created_files:
        file_size = file_path.stat().st_size / (1024 * 1024)
        print(f"  [OK] {file_path.name} ({file_size:.2f} MB)")
    
    if len(created_files) == 4:
        print(f"\n[SUCCESS] All 4 test files created successfully!")
        print(f"Location: {project_root / 'data' / 'test_data'}")
    else:
        print(f"\n[WARNING] Only {len(created_files)}/4 files created. Check errors above.")


if __name__ == "__main__":
    main()

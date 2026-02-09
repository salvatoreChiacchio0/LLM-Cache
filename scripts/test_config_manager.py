"""
Test Config Manager: Gestisce la configurazione del test corrente.
Crea/modifica un file JSON che streamer legge per sapere quale dataset usare.
"""
import json
import os
from pathlib import Path
from typing import Optional, Dict


class TestConfigManager:
    """Gestisce la configurazione del test corrente."""
    
    def __init__(self, config_file: str = "data/current_test.json"):
        self.config_file = Path(config_file)
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
    
    def set_current_test(self, test_name: str, dataset_path: str, max_events: int, metadata: Optional[Dict] = None):
        """Imposta il test corrente."""
        config = {
            "test_name": test_name,
            "dataset_path": dataset_path,
            "max_events": max_events,
            "metadata": metadata or {}
        }
        
        with open(self.config_file, 'w') as f:
            json.dump(config, f, indent=2)
        
        print(f"[CONFIG] Set current test: {test_name} -> {dataset_path}")
    
    def get_current_test(self) -> Optional[Dict]:
        """Ottiene il test corrente."""
        if not self.config_file.exists():
            return None
        
        with open(self.config_file, 'r') as f:
            return json.load(f)
    
    def clear_current_test(self):
        """Rimuove il test corrente."""
        if self.config_file.exists():
            self.config_file.unlink()
        print("[CONFIG] Cleared current test")

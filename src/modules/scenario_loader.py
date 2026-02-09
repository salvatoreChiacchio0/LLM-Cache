"""
Loader per scenari JSON generati dal sistema di scenario generation.
Legge gli scenari da data/scenarios/final/ e genera eventi nel formato
compatibile con il sistema di cache.
"""
import json
import os
from typing import Iterator, Tuple, Optional
from pathlib import Path


class ScenarioLoader:
    """
    Carica scenari JSON e genera eventi nel formato (item_id, user, action, timestamp).
    Supporta limitazione del numero di eventi (es. 100k).
    """
    
    def __init__(self, scenario_path: str, max_events: Optional[int] = None):
        """
        Args:
            scenario_path: Path al file JSON dello scenario
            max_events: Numero massimo di eventi da generare (None = tutti)
        """
        self.scenario_path = scenario_path
        self.max_events = max_events
        self._check_file()
    
    def _check_file(self):
        """Verifica che il file esista."""
        if not os.path.exists(self.scenario_path):
            raise FileNotFoundError(f"Scenario file not found: {self.scenario_path}")
    
    def load_metadata(self) -> dict:
        """Carica i metadata dello scenario."""
        with open(self.scenario_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('metadata', {})
    
    def generate_events(self, shutdown_event=None) -> Iterator[Tuple[int, str, str, str]]:
        """
        Genera eventi dallo scenario JSON.
        
        Yields:
            Tuple di (item_id, user_id, action, timestamp)
        """
        with open(self.scenario_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            events = data.get('events', [])
            
            if self.max_events:
                events = events[:self.max_events]
            
            print(f"[SCENARIO_LOADER] Loading {len(events)} events from {os.path.basename(self.scenario_path)}")
            
            for event in events:
                if shutdown_event and shutdown_event.is_set():
                    return
                
                item_id = event.get('item_id', '')
                user_id = event.get('user_id', '')
                action = event.get('action', '')
                timestamp = event.get('timestamp', '')
                
                try:
                    # Converti item_id a int se possibile
                    item_id_int = int(item_id) if item_id else 0
                    # Converti timestamp a stringa
                    timestamp_str = str(timestamp)
                    yield item_id_int, user_id, action, timestamp_str
                except (ValueError, TypeError) as e:
                    continue
    
    @staticmethod
    def list_available_scenarios(scenarios_dir: str = "data/scenarios/final") -> list:
        """
        Lista tutti gli scenari disponibili nella directory.
        
        Returns:
            Lista di path completi ai file JSON degli scenari
        """
        scenarios_path = Path(scenarios_dir)
        if not scenarios_path.exists():
            return []
        
        scenario_files = sorted(scenarios_path.glob("*.json"))
        return [str(f) for f in scenario_files]

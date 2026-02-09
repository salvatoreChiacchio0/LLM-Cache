"""
Loader per file di test pre-processati (100k eventi).
Legge file da data/test_data/ che sono stati preparati in anticipo.
"""
import json
import csv
import os
from typing import Iterator, Tuple, Optional
from pathlib import Path


class PreprocessedLoader:
    """
    Carica file di test pre-processati (CSV o JSON).
    Supporta sia formato CSV che JSON.
    """
    
    def __init__(self, file_path: str):
        """
        Args:
            file_path: Path al file pre-processato (CSV o JSON)
        """
        self.file_path = Path(file_path)
        self._check_file()
    
    def _check_file(self):
        """Verifica che il file esista."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Preprocessed file not found: {self.file_path}")
    
    def load_metadata(self) -> dict:
        """Carica metadata se il file è JSON."""
        if self.file_path.suffix.lower() == '.json':
            with open(self.file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('metadata', {})
        return {}
    
    def generate_events(self, shutdown_event=None) -> Iterator[Tuple[int, str, str, str]]:
        """
        Genera eventi dal file pre-processato.
        
        Yields:
            Tuple di (item_id, user_id, action, timestamp)
        """
        if self.file_path.suffix.lower() == '.json':
            yield from self._generate_from_json(shutdown_event)
        else:
            yield from self._generate_from_csv(shutdown_event)
    
    def _generate_from_json(self, shutdown_event=None) -> Iterator[Tuple[int, str, str, str]]:
        """Genera eventi da file JSON."""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            events = data.get('events', [])
            
            print(f"[PREPROCESSED_LOADER] Loading {len(events)} events from {self.file_path.name}")
            
            for event in events:
                if shutdown_event and shutdown_event.is_set():
                    return
                
                item_id = event.get('item_id', '')
                user_id = event.get('user_id', '')
                action = event.get('action', '')
                timestamp = event.get('timestamp', '')
                
                try:
                    item_id_int = int(item_id) if item_id else 0
                    timestamp_str = str(timestamp)
                    yield item_id_int, user_id, action, timestamp_str
                except (ValueError, TypeError):
                    continue
    
    def _generate_from_csv(self, shutdown_event=None) -> Iterator[Tuple[int, str, str, str]]:
        """Genera eventi da file CSV."""
        from src.core.config import LOG_DELIMITER, FILE_ENCODING
        
        with open(self.file_path, 'r', encoding=FILE_ENCODING, errors='ignore') as f:
            reader = csv.reader(f, delimiter=LOG_DELIMITER)
            
            # Conta righe per logging
            lines = list(reader)
            print(f"[PREPROCESSED_LOADER] Loading {len(lines)} events from {self.file_path.name}")
            
            for row in lines:
                if shutdown_event and shutdown_event.is_set():
                    return
                
                if len(row) < 4:
                    continue
                
                item, user, action = row[0], row[1], row[2]
                timestamp = row[3]
                
                try:
                    item_id = int(item)
                    yield item_id, user, action, timestamp
                except ValueError:
                    continue
    
    @staticmethod
    def list_available_datasets(test_data_dir: str = "data/test_data") -> list:
        """
        Lista tutti i dataset pre-processati disponibili.
        
        Returns:
            Lista di tuple (name, csv_path, json_path)
        """
        test_data_path = Path(test_data_dir)
        if not test_data_path.exists():
            return []
        
        # Leggi index.json se esiste
        index_path = test_data_path / "index.json"
        if index_path.exists():
            with open(index_path, 'r', encoding='utf-8') as f:
                index_data = json.load(f)
                return [
                    (ds["name"], ds["csv_path"], ds["json_path"])
                    for ds in index_data.get("datasets", [])
                ]
        
        # Fallback: cerca file manualmente
        datasets = []
        json_files = sorted(test_data_path.glob("*_100k.json"))
        for json_file in json_files:
            name = json_file.stem.replace("_100k", "")
            csv_file = test_data_path / f"{name}_100k.csv"
            if csv_file.exists():
                datasets.append((name, str(csv_file), str(json_file)))
        
        return datasets

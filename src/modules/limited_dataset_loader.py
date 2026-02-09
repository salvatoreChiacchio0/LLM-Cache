"""
Loader per dataset normale con limitazione del numero di eventi.
Legge solo i primi N eventi dal file CSV originale.
"""
import csv
import os
from typing import Iterator, Tuple, Optional
from ..core.config import LOG_FILE, LOG_DELIMITER, FILE_ENCODING


class LimitedDatasetLoader:
    """
    Carica un numero limitato di eventi dal dataset normale.
    Utile per test comparativi senza processare tutto il dataset.
    """
    
    def __init__(self, log_file: str = LOG_FILE, max_events: int = 100000):
        """
        Args:
            log_file: Path al file CSV del dataset
            max_events: Numero massimo di eventi da leggere
        """
        self.log_file = log_file
        self.max_events = max_events
        self._check_file()
    
    def _check_file(self):
        """Verifica che il file esista."""
        if not os.path.exists(self.log_file):
            raise FileNotFoundError(f"Log file not found: {self.log_file}")
    
    def generate_events(self, shutdown_event=None) -> Iterator[Tuple[int, str, str, str]]:
        """
        Genera eventi dal dataset CSV limitato.
        
        Yields:
            Tuple di (item_id, user_id, action, timestamp)
        """
        events_read = 0
        
        with open(self.log_file, "r", encoding=FILE_ENCODING, errors="ignore") as f:
            print(f"[LIMITED_DATASET_LOADER] Loading up to {self.max_events} events from {os.path.basename(self.log_file)}")
            reader = csv.reader(f, delimiter=LOG_DELIMITER)
            
            # Skip header se presente
            try:
                first_line = next(reader)
                # Verifica se è un header (non inizia con un numero)
                try:
                    int(first_line[0])
                    # È un dato, riposiziona
                    f.seek(0)
                    reader = csv.reader(f, delimiter=LOG_DELIMITER)
                except (ValueError, IndexError):
                    # È un header, continua
                    pass
            except (StopIteration, EOFError):
                pass
            
            for row in reader:
                if shutdown_event and shutdown_event.is_set():
                    return
                
                if events_read >= self.max_events:
                    print(f"[LIMITED_DATASET_LOADER] Reached limit of {self.max_events} events")
                    return
                
                if len(row) < 4:
                    continue
                
                item, user, action = row[0], row[1], row[2]
                timestamp = row[3]
                
                try:
                    item_id = int(item)
                    events_read += 1
                    yield item_id, user, action, timestamp
                except ValueError:
                    continue

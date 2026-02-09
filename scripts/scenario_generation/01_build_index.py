#!/usr/bin/env python3
"""
01_build_index.py - Tianchi Dataset Indexer

Crea un indice leggero del dataset per analisi rapide.
Usa chunked reading per gestire 15M righe senza overflow RAM.

Usage:
    python 01_build_index.py --input data/tianchi_dataset.csv --output data/tianchi_index.json

Author: Scenario Generation System
Date: 2025-02-02
"""

import pandas as pd
import json
from collections import defaultdict, Counter
from datetime import datetime
from tqdm import tqdm
import argparse
import sys
from pathlib import Path
import logging

# Import utilities
from utils.chunked_reader import ChunkedReader
from utils.stats_calculator import calculate_zipf_alpha, calculate_gini
from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class DatasetIndexer:
    """
    Indexer per il dataset Tianchi che crea un indice giornaliero leggero.
    
    L'indice contiene:
    - Top-100 item per giorno
    - Statistiche aggregate (total_events, action_distribution)
    - Metriche di volatilità giornaliera (Zipf, Gini)
    
    Memory Safety:
    - Usa chunked reading (default 500K righe per chunk)
    - Peak RAM: ~50MB per chunk + overhead strutture dati
    - Total peak: ~100-150MB (ben sotto il limite di 500MB)
    """
    
    def __init__(self, dataset_path, chunksize=None):
        """
        Inizializza l'indexer.
        
        Args:
            dataset_path: Path al file CSV del dataset
            chunksize: Dimensione del chunk (default da Config)
        """
        self.dataset_path = Path(dataset_path)
        self.chunksize = chunksize or Config.CHUNK_SIZE
        
        # Struttura dati per l'indice giornaliero
        # Usa defaultdict per evitare controlli espliciti
        self.daily_index = defaultdict(lambda: {
            'top_items': Counter(),
            'total_events': 0,
            'action_dist': Counter(),
            'unique_items': set(),
            'hourly_dist': Counter()
        })
        
        # Validate file exists
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {self.dataset_path}")
        
        logger.info(f"Initialized DatasetIndexer for {self.dataset_path}")
        logger.info(f"Chunk size: {self.chunksize:,} rows")
    
    def build_index(self, output_path):
        """
        Main indexing pipeline con progress tracking.
        
        Args:
            output_path: Path dove salvare l'indice JSON
            
        Returns:
            dict: Indice completo del dataset
        """
        logger.info(f"Starting indexing: {self.dataset_path}")
        logger.info(f"Chunk size: {self.chunksize:,} rows")
        
        # Step 1: Count total lines (for progress bar)
        logger.info("Counting total rows...")
        total_lines = self._count_lines()
        logger.info(f"Total rows: {total_lines:,}")
        
        if total_lines == 0:
            raise ValueError(f"Dataset file {self.dataset_path} appears to be empty")
        
        # Step 2: Process chunks
        reader = ChunkedReader(
            self.dataset_path,
            chunksize=self.chunksize,
            usecols=Config.DATASET_COLUMNS,
            encoding=Config.DATASET_ENCODING,
            delimiter=Config.CSV_DELIMITER,
            skip_bad_lines=True
        )
        
        chunk_count = 0
        total_processed = 0
        
        with tqdm(total=total_lines, desc="Indexing", unit="rows", unit_scale=True) as pbar:
            for chunk in reader:
                chunk_count += 1
                rows_in_chunk = len(chunk)
                
                # Process chunk
                self._process_chunk(chunk)
                
                # Update progress
                total_processed += rows_in_chunk
                pbar.update(rows_in_chunk)
                
                # Log memory usage ogni N chunks
                if chunk_count % Config.PROGRESS_UPDATE_INTERVAL == 0:
                    self._log_memory_usage()
                    logger.debug(f"Processed {chunk_count} chunks, {total_processed:,} rows")
        
        logger.info(f"Processed {chunk_count} chunks, {total_processed:,} total rows")
        
        # Step 3: Post-process and serialize
        logger.info("Finalizing index...")
        index_light = self._finalize_index()
        
        # Step 4: Save
        self._save_index(index_light, output_path)
        
        # Step 5: Validate
        self._validate_index(index_light)
        
        return index_light
    
    def _count_lines(self):
        """
        Conta il numero totale di righe nel file (escluso header).
        
        Returns:
            int: Numero totale di righe
            
        Note:
            Questo metodo legge il file una volta per contare le righe.
            È necessario per avere una progress bar accurata.
            Memory-safe: legge una riga alla volta.
        """
        try:
            reader = ChunkedReader(
                self.dataset_path,
                chunksize=100000,  # Smaller chunks for counting
                encoding=Config.DATASET_ENCODING,
                delimiter=Config.CSV_DELIMITER
            )
            count = 0
            for chunk in reader:
                count += len(chunk)
            return count
        except Exception as e:
            logger.warning(f"Failed to count lines using ChunkedReader: {e}")
            # Fallback: manual count
            count = 0
            encodings_to_try = [Config.DATASET_ENCODING, 'latin-1', 'utf-8']
            for encoding in encodings_to_try:
                try:
                    with open(self.dataset_path, 'r', encoding=encoding, errors='ignore') as f:
                        # Skip header
                        try:
                            next(f)
                        except StopIteration:
                            return 0
                        # Count lines
                        for _ in f:
                            count += 1
                    return count
                except Exception:
                    continue
            return 0
    
    def _process_chunk(self, chunk):
        """
        Processa un singolo chunk e aggiorna l'indice.
        
        Args:
            chunk (pd.DataFrame): Chunk da processare con colonne: Item_id, User_id, Action, Vtime
            
        Note:
            Memory-safe: il chunk viene processato e poi rilasciato.
            Aggrega dati per giorno senza mantenere tutto in memoria.
        """
        # Validate required columns
        required_cols = Config.DATASET_COLUMNS
        missing_cols = [col for col in required_cols if col not in chunk.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Parse timestamps
        try:
            chunk['Vtime'] = pd.to_datetime(
                chunk['Vtime'],
                format=Config.TIMESTAMP_FORMAT,
                errors='coerce'
            )
        except Exception as e:
            logger.warning(f"Error parsing timestamps: {e}, trying alternative format")
            chunk['Vtime'] = pd.to_datetime(chunk['Vtime'], errors='coerce')
        
        # Drop rows with invalid timestamps
        chunk = chunk.dropna(subset=['Vtime'])
        
        if len(chunk) == 0:
            logger.warning("Chunk has no valid timestamps, skipping")
            return
        
        # Extract date and hour
        chunk['date'] = chunk['Vtime'].dt.date
        chunk['hour'] = chunk['Vtime'].dt.hour
        
        # Aggrega per giorno
        for date, group in chunk.groupby('date'):
            date_str = str(date)
            
            # Update counters
            # Top items: usa value_counts per efficienza
            item_counts = group['Item_id'].value_counts()
            self.daily_index[date_str]['top_items'].update(item_counts.to_dict())
            
            # Total events
            self.daily_index[date_str]['total_events'] += len(group)
            
            # Action distribution
            self.daily_index[date_str]['action_dist'].update(group['Action'].value_counts().to_dict())
            
            # Unique items (usa set per efficienza)
            self.daily_index[date_str]['unique_items'].update(group['Item_id'].unique())
            
            # Hourly distribution
            self.daily_index[date_str]['hourly_dist'].update(group['hour'].value_counts().to_dict())
    
    def _finalize_index(self):
        """
        Converte l'indice grezzo in formato serializzabile.
        Mantiene solo top-100 item per giorno per risparmiare spazio.
        
        Returns:
            dict: Indice finalizzato e serializzabile
            
        Note:
            Memory-safe: converte set in int (count) e mantiene solo top-K.
            Riduce significativamente la dimensione dell'indice.
        """
        index_light = {}
        
        logger.info(f"Finalizing index for {len(self.daily_index)} days...")
        
        for date, data in tqdm(self.daily_index.items(), desc="Finalizing", unit="day"):
            # Top-100 items con conteggi
            top_100 = data['top_items'].most_common(Config.INDEX_TOP_ITEMS)
            
            # Calcola statistiche se abilitate
            statistics = {}
            
            if Config.INDEX_STATISTICS.get('zipf_alpha', False):
                counts = [c for _, c in top_100]
                if len(counts) >= 10:
                    zipf_alpha = calculate_zipf_alpha(counts)
                    statistics['zipf_alpha'] = zipf_alpha
            
            if Config.INDEX_STATISTICS.get('gini_coefficient', False):
                counts = [c for _, c in top_100]
                if len(counts) >= 10:
                    gini = calculate_gini(counts)
                    statistics['gini_coefficient'] = gini
            
            if Config.INDEX_STATISTICS.get('repeat_rate', False):
                repeat_rate = self._calculate_repeat_rate(data['top_items'])
                statistics['repeat_rate'] = repeat_rate
            
            # Build final entry
            entry = {
                'top_100': [(str(item_id), count) for item_id, count in top_100],
                'total_events': data['total_events'],
                'unique_items': len(data['unique_items']),
            }
            
            if Config.INDEX_STATISTICS.get('action_distribution', False):
                entry['action_dist'] = dict(data['action_dist'])
            
            if Config.INDEX_STATISTICS.get('hourly_distribution', False):
                entry['hourly_dist'] = dict(data['hourly_dist'])
            
            entry['statistics'] = statistics
            
            index_light[date] = entry
        
        return index_light
    
    def _calculate_repeat_rate(self, counter):
        """
        Calcola la percentuale di item visti più di una volta.
        
        Args:
            counter: Counter con conteggi degli item
            
        Returns:
            float: Percentuale tra 0 e 1
        """
        if len(counter) == 0:
            return 0.0
        repeated = sum(1 for c in counter.values() if c > 1)
        return repeated / len(counter)
    
    def _save_index(self, index, output_path):
        """
        Salva indice in JSON con metadata.
        
        Args:
            index: Indice da salvare
            output_path: Path dove salvare il file JSON
        """
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        
        # Aggiungi metadata
        dates = sorted(index.keys())
        full_output = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'source_file': str(self.dataset_path),
                'total_days': len(index),
                'date_range': {
                    'start': dates[0] if dates else None,
                    'end': dates[-1] if dates else None
                },
                'version': '1.0',
                'chunk_size': self.chunksize,
                'top_items_per_day': Config.INDEX_TOP_ITEMS,
                'statistics_enabled': Config.INDEX_STATISTICS
            },
            'index': index
        }
        
        # Save with indentation for readability
        with open(output, 'w', encoding='utf-8') as f:
            json.dump(full_output, f, indent=2, ensure_ascii=False)
        
        file_size_mb = output.stat().st_size / 1024 / 1024
        logger.info(f"Index saved: {output} ({file_size_mb:.2f} MB)")
    
    def _validate_index(self, index):
        """
        Valida che l'indice sia completo e coerente.
        
        Args:
            index: Indice da validare
            
        Note:
            Esegue controlli di qualità e logga warning se necessario.
        """
        logger.info("Validating index...")
        
        if len(index) == 0:
            logger.warning("Index is empty!")
            return
        
        # Check 1: Date range
        dates = sorted(index.keys())
        if len(dates) == 0:
            logger.warning("No dates in index")
            return
        
        try:
            start_date = pd.to_datetime(dates[0])
            end_date = pd.to_datetime(dates[-1])
            expected_days = (end_date - start_date).days + 1
            actual_days = len(dates)
            
            if actual_days < expected_days:
                missing = expected_days - actual_days
                missing_ratio = missing / expected_days
                logger.warning(
                    f"Missing {missing} days in index ({missing_ratio:.1%} of expected). "
                    f"Expected: {expected_days}, Actual: {actual_days}"
                )
                
                if missing_ratio > Config.MAX_MISSING_DAYS_RATIO:
                    logger.error(
                        f"Too many missing days ({missing_ratio:.1%} > {Config.MAX_MISSING_DAYS_RATIO:.1%}). "
                        f"Index may be incomplete."
                    )
        except Exception as e:
            logger.warning(f"Could not validate date range: {e}")
        
        # Check 2: Reasonable event counts
        total_events = sum(d['total_events'] for d in index.values())
        logger.info(f"Total events indexed: {total_events:,}")
        
        if total_events == 0:
            logger.error("No events indexed!")
            return
        
        # Check 3: All days have top items
        empty_days = [d for d, data in index.items() if len(data.get('top_100', [])) == 0]
        if empty_days:
            logger.warning(f"{len(empty_days)} days have no top items: {empty_days[:10]}...")
        
        # Check 4: Statistics quality
        days_with_stats = sum(
            1 for d in index.values()
            if d.get('statistics', {}).get('zipf_alpha') is not None
        )
        logger.info(f"Days with Zipf statistics: {days_with_stats}/{len(index)}")
        
        logger.info("Index validation complete")
    
    def _log_memory_usage(self):
        """
        Log current RAM usage.
        
        Note:
            Usa psutil se disponibile, altrimenti salta il logging.
        """
        try:
            import psutil
            import os
            process = psutil.Process(os.getpid())
            mem_mb = process.memory_info().rss / 1024 / 1024
            logger.info(f"Current RAM usage: {mem_mb:.2f} MB (limit: {Config.MAX_RAM_MB} MB)")
            
            if mem_mb > Config.MAX_RAM_MB:
                logger.warning(
                    f"RAM usage ({mem_mb:.2f} MB) exceeds limit ({Config.MAX_RAM_MB} MB)! "
                    f"Consider reducing chunk size."
                )
        except ImportError:
            logger.debug("psutil not available, skipping memory logging")
        except Exception as e:
            logger.debug(f"Could not log memory usage: {e}")


def main():
    """
    Entry point per lo script di indexing.
    
    Parsa gli argomenti da command line e esegue l'indexing.
    """
    parser = argparse.ArgumentParser(
        description='Build Tianchi dataset index',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 01_build_index.py --input data/tianchi_dataset.csv
  python 01_build_index.py --input data/tianchi_dataset.csv --output data/custom_index.json
  python 01_build_index.py --input data/tianchi_dataset.csv --chunksize 1000000
        """
    )
    parser.add_argument(
        '--input',
        type=str,
        required=True,
        help='Path to tianchi_dataset.csv'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output path for index (default: data/tianchi_index.json)'
    )
    parser.add_argument(
        '--chunksize',
        type=int,
        default=None,
        help=f'Chunk size for reading (default: {Config.CHUNK_SIZE:,})'
    )
    
    args = parser.parse_args()
    
    # Validate input exists
    input_path = Path(args.input)
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)
    
    # Set output path
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Config.TIANCHI_INDEX
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Run indexing
    try:
        indexer = DatasetIndexer(
            input_path,
            chunksize=args.chunksize
        )
        index = indexer.build_index(output_path)
        
        logger.info("=" * 60)
        logger.info("Indexing complete!")
        logger.info(f"Indexed {len(index)} days")
        logger.info(f"Output: {output_path}")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.warning("Indexing interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Indexing failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()

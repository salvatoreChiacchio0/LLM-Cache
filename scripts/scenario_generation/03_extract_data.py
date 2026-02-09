#!/usr/bin/env python3
"""
03_extract_data.py - Data Extractor

Estrae i dati effettivi dal dataset originale per ogni scenario trovato.
Salva i dati estratti in CSV nella directory data/scenarios/raw/.

Usage:
    python 03_extract_data.py --scenarios data/scenarios_found.json --output data/scenarios/raw/

Author: Scenario Generation System
Date: 2025-02-02
"""

import json
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional

from utils.chunked_reader import ChunkedReader
from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """
    Estrae dati dal dataset originale per ogni scenario trovato.
    
    Per ogni scenario, filtra il dataset per le date/periodi specificati
    e salva i dati estratti in file CSV separati.
    """
    
    def __init__(self, dataset_path, scenarios_path):
        """
        Inizializza l'estrattore.
        
        Args:
            dataset_path: Path al dataset originale CSV
            scenarios_path: Path al file scenarios_found.json
        """
        self.dataset_path = Path(dataset_path)
        self.scenarios_path = Path(scenarios_path)
        
        if not self.dataset_path.exists():
            raise FileNotFoundError(f"Dataset file not found: {self.dataset_path}")
        if not self.scenarios_path.exists():
            raise FileNotFoundError(f"Scenarios file not found: {self.scenarios_path}")
        
        # Carica scenari
        with open(self.scenarios_path, 'r', encoding='utf-8') as f:
            scenarios_data = json.load(f)
        
        self.scenarios = scenarios_data.get('scenarios', {})
        self.metadata = scenarios_data.get('metadata', {})
        
        logger.info(f"Loaded {len(self.scenarios)} scenarios from {self.scenarios_path}")
    
    def extract_all_scenarios(self, output_dir: Path) -> Dict[str, Path]:
        """
        Estrae dati per tutti gli scenari trovati.
        
        Args:
            output_dir: Directory dove salvare i CSV estratti
            
        Returns:
            dict: Mapping scenario_name -> output_file_path
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        extracted_files = {}
        
        logger.info("\n" + "="*60)
        logger.info("Starting data extraction...")
        logger.info("="*60)
        
        # Extract each scenario
        scenario_names = {
            'hot_cold_shift': 'hot_cold_shift',
            'burst_cooldown': 'burst_cooldown',
            'scan_attack': 'scan_attack',
            'daily_pattern': 'daily_pattern',
            'multi_modal': 'multi_modal'
        }
        
        for scenario_key, scenario_name in scenario_names.items():
            scenario_data = self.scenarios.get(scenario_key)
            
            if scenario_data is None:
                logger.warning(f"[SKIP] Scenario '{scenario_key}' not found in scenarios file")
                continue
            
            logger.info(f"\n[{scenario_key}] Extracting data...")
            
            try:
                output_file = self._extract_scenario(
                    scenario_key,
                    scenario_data,
                    output_dir
                )
                
                if output_file:
                    extracted_files[scenario_key] = output_file
                    logger.info(f"[SUCCESS] Extracted to: {output_file}")
                else:
                    logger.warning(f"[WARNING] No data extracted for {scenario_key}")
                    
            except Exception as e:
                logger.error(f"[ERROR] Failed to extract {scenario_key}: {e}", exc_info=True)
        
        logger.info("\n" + "="*60)
        logger.info(f"Extraction complete! Extracted {len(extracted_files)} scenarios")
        logger.info("="*60)
        
        return extracted_files
    
    def _extract_scenario(self, scenario_key: str, scenario_data: Dict, output_dir: Path) -> Optional[Path]:
        """
        Estrae dati per un singolo scenario.
        
        Args:
            scenario_key: Chiave dello scenario
            scenario_data: Dati dello scenario da scenarios_found.json
            output_dir: Directory di output
            
        Returns:
            Path: Path al file CSV estratto, o None se nessun dato estratto
        """
        # Determina le date da estrarre in base al tipo di scenario
        target_dates = self._get_target_dates(scenario_key, scenario_data)
        
        if not target_dates:
            logger.warning(f"No target dates found for {scenario_key}")
            return None
        
        logger.info(f"Target dates: {min(target_dates)} to {max(target_dates)} ({len(target_dates)} days)")
        
        # Estrai dati
        extracted_rows = []
        total_processed = 0
        
        reader = ChunkedReader(
            self.dataset_path,
            chunksize=Config.CHUNK_SIZE,
            usecols=Config.DATASET_COLUMNS,
            encoding=Config.DATASET_ENCODING,
            delimiter=Config.CSV_DELIMITER,
            skip_bad_lines=True
        )
        
        # Count total rows for progress
        total_rows = reader.get_total_rows()
        
        with tqdm(total=total_rows, desc=f"Extracting {scenario_key}", unit="rows") as pbar:
            for chunk in reader:
                # Parse timestamps
                chunk['Vtime'] = pd.to_datetime(
                    chunk['Vtime'],
                    format=Config.TIMESTAMP_FORMAT,
                    errors='coerce'
                )
                
                # Drop invalid timestamps
                chunk = chunk.dropna(subset=['Vtime'])
                
                if len(chunk) == 0:
                    pbar.update(Config.CHUNK_SIZE)
                    continue
                
                # Extract date
                chunk['date'] = chunk['Vtime'].dt.date
                
                # Filter by target dates
                chunk_filtered = chunk[chunk['date'].isin(target_dates)]
                
                if len(chunk_filtered) > 0:
                    # Remove temporary 'date' column before saving
                    chunk_to_save = chunk_filtered.drop(columns=['date'])
                    extracted_rows.append(chunk_to_save)
                
                total_processed += len(chunk)
                pbar.update(len(chunk))
        
        if not extracted_rows:
            logger.warning(f"No data extracted for {scenario_key}")
            return None
        
        # Combine all chunks
        logger.info(f"Combining {len(extracted_rows)} chunks...")
        extracted_df = pd.concat(extracted_rows, ignore_index=True)
        
        # Sort by timestamp
        extracted_df = extracted_df.sort_values('Vtime').reset_index(drop=True)
        
        # Save to CSV
        output_file = output_dir / f"{scenario_key}.csv"
        extracted_df.to_csv(
            output_file,
            index=False,
            encoding='utf-8'
        )
        
        file_size_mb = output_file.stat().st_size / 1024 / 1024
        logger.info(f"Extracted {len(extracted_df):,} rows ({file_size_mb:.2f} MB)")
        
        return output_file
    
    def _get_target_dates(self, scenario_key: str, scenario_data: Dict) -> List:
        """
        Determina le date target per l'estrazione in base al tipo di scenario.
        
        Args:
            scenario_key: Chiave dello scenario
            scenario_data: Dati dello scenario
            
        Returns:
            list: Lista di date (datetime.date) da estrarre
        """
        from datetime import timedelta
        
        target_dates = []
        
        if scenario_key == 'hot_cold_shift':
            # Estrai date1 e date2
            date1_str = scenario_data.get('date1')
            date2_str = scenario_data.get('date2')
            
            if date1_str:
                date1 = pd.to_datetime(date1_str).date()
                target_dates.append(date1)
            if date2_str:
                date2 = pd.to_datetime(date2_str).date()
                target_dates.append(date2)
        
        elif scenario_key == 'burst_cooldown':
            # Estrai periodo del burst (start_date, end_date)
            start_date_str = scenario_data.get('start_date')
            end_date_str = scenario_data.get('end_date')
            
            if start_date_str and end_date_str:
                start_date = pd.to_datetime(start_date_str).date()
                end_date = pd.to_datetime(end_date_str).date()
                
                # Include tutti i giorni nel range
                current_date = start_date
                while current_date <= end_date:
                    target_dates.append(current_date)
                    current_date += timedelta(days=1)
        
        elif scenario_key == 'scan_attack':
            # Estrai date_range
            date_range = scenario_data.get('date_range', [])
            
            if len(date_range) >= 2:
                start_date = pd.to_datetime(date_range[0]).date()
                end_date = pd.to_datetime(date_range[1]).date()
                
                # Include tutti i giorni nel range
                current_date = start_date
                while current_date <= end_date:
                    target_dates.append(current_date)
                    current_date += timedelta(days=1)
        
        elif scenario_key == 'daily_pattern':
            # Estrai cycle_dates
            cycle_dates = scenario_data.get('cycle_dates', [])
            period_days = scenario_data.get('period_days', 7)
            
            if cycle_dates:
                # Estrai tutti i giorni per ogni ciclo
                for cycle_date_str in cycle_dates:
                    cycle_start = pd.to_datetime(cycle_date_str).date()
                    
                    # Include period_days giorni per ogni ciclo
                    for day_offset in range(period_days):
                        target_date = cycle_start + timedelta(days=day_offset)
                        if target_date not in target_dates:
                            target_dates.append(target_date)
        
        elif scenario_key == 'multi_modal':
            # Estrai date_range
            date_range = scenario_data.get('date_range', [])
            
            if len(date_range) >= 2:
                start_date = pd.to_datetime(date_range[0]).date()
                end_date = pd.to_datetime(date_range[1]).date()
                
                # Include tutti i giorni nel range
                current_date = start_date
                while current_date <= end_date:
                    target_dates.append(current_date)
                    current_date += timedelta(days=1)
        
        # Remove duplicates and sort
        target_dates = sorted(list(set(target_dates)))
        
        return target_dates


def main():
    """
    Entry point per lo script di estrazione dati.
    """
    parser = argparse.ArgumentParser(
        description='Extract scenario data from Tianchi dataset',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 03_extract_data.py --scenarios data/scenarios_found.json
  python 03_extract_data.py --scenarios data/scenarios_found.json --output custom_output/
        """
    )
    parser.add_argument(
        '--scenarios',
        type=str,
        default=None,
        help='Path to scenarios_found.json (default: data/scenarios_found.json)'
    )
    parser.add_argument(
        '--dataset',
        type=str,
        default=None,
        help='Path to tianchi_dataset.csv (default: from Config)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output directory for extracted CSV files (default: data/scenarios/raw/)'
    )
    
    args = parser.parse_args()
    
    # Set default paths
    if args.scenarios is None:
        scenarios_path = Config.DATA_DIR / 'scenarios_found.json'
    else:
        scenarios_path = Path(args.scenarios)
    
    if args.dataset is None:
        dataset_path = Config.get_dataset_path()
    else:
        dataset_path = Path(args.dataset)
    
    if args.output is None:
        output_dir = Config.RAW_SCENARIOS_DIR
    else:
        output_dir = Path(args.output)
    
    # Validate inputs
    if not scenarios_path.exists():
        logger.error(f"Scenarios file not found: {scenarios_path}")
        logger.error("Please run 02_find_scenarios.py first to create scenarios_found.json")
        return 1
    
    if not dataset_path.exists():
        logger.error(f"Dataset file not found: {dataset_path}")
        logger.error("Please ensure tianchi_dataset.csv exists")
        return 1
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run extraction
    try:
        extractor = DataExtractor(dataset_path, scenarios_path)
        extracted_files = extractor.extract_all_scenarios(output_dir)
        
        logger.info("\n" + "="*60)
        logger.info("Extraction Summary:")
        logger.info("="*60)
        for scenario_key, file_path in extracted_files.items():
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            logger.info(f"  {scenario_key}: {file_path} ({file_size_mb:.2f} MB)")
        
        logger.info(f"\n[SUCCESS] All extractions complete!")
        logger.info(f"Output directory: {output_dir}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error extracting data: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

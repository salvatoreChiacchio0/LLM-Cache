#!/usr/bin/env python3
"""
04_generate_workload.py - Workload Generator

Converte i CSV estratti in formato JSON strutturato per il testing.
Salva i workload JSON nella directory data/scenarios/final/.

Usage:
    python 04_generate_workload.py --input data/scenarios/raw/ --output data/scenarios/final/

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

from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class WorkloadGenerator:
    """
    Genera workload JSON dai CSV estratti.
    
    Converte i dati CSV in formato JSON strutturato adatto per il testing
    del sistema di cache, con metadata e eventi ordinati per timestamp.
    """
    
    # Mapping scenario names to output file numbers
    SCENARIO_MAPPING = {
        'hot_cold_shift': '01',
        'burst_cooldown': '02',
        'scan_attack': '03',
        'daily_pattern': '04',
        'multi_modal': '05'
    }
    
    def __init__(self, scenarios_found_path: Optional[Path] = None):
        """
        Inizializza il generatore.
        
        Args:
            scenarios_found_path: Path al file scenarios_found.json (opzionale)
        """
        if scenarios_found_path is None:
            scenarios_found_path = Config.DATA_DIR / 'scenarios_found.json'
        
        self.scenarios_found_path = Path(scenarios_found_path)
        
        # Carica metadata degli scenari se disponibile
        self.scenarios_metadata = {}
        if self.scenarios_found_path.exists():
            try:
                with open(self.scenarios_found_path, 'r', encoding='utf-8') as f:
                    scenarios_data = json.load(f)
                    self.scenarios_metadata = scenarios_data.get('scenarios', {})
                    logger.info(f"Loaded metadata from {self.scenarios_found_path}")
            except Exception as e:
                logger.warning(f"Could not load scenarios metadata: {e}")
    
    def generate_all_workloads(self, input_dir: Path, output_dir: Path) -> Dict[str, Path]:
        """
        Genera workload JSON per tutti i CSV trovati.
        
        Args:
            input_dir: Directory contenente i CSV estratti
            output_dir: Directory dove salvare i JSON
            
        Returns:
            dict: Mapping scenario_name -> output_file_path
        """
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generated_files = {}
        
        logger.info("\n" + "="*60)
        logger.info("Starting workload generation...")
        logger.info("="*60)
        
        # Find all CSV files in input directory
        csv_files = list(input_dir.glob('*.csv'))
        
        if not csv_files:
            logger.warning(f"No CSV files found in {input_dir}")
            return generated_files
        
        logger.info(f"Found {len(csv_files)} CSV files to process")
        
        for csv_file in csv_files:
            scenario_name = csv_file.stem  # filename without extension
            
            logger.info(f"\n[{scenario_name}] Processing {csv_file.name}...")
            
            try:
                output_file = self._generate_workload(
                    csv_file,
                    scenario_name,
                    output_dir
                )
                
                if output_file:
                    generated_files[scenario_name] = output_file
                    logger.info(f"[SUCCESS] Generated: {output_file}")
                else:
                    logger.warning(f"[WARNING] Failed to generate workload for {scenario_name}")
                    
            except Exception as e:
                logger.error(f"[ERROR] Failed to process {scenario_name}: {e}", exc_info=True)
        
        logger.info("\n" + "="*60)
        logger.info(f"Generation complete! Generated {len(generated_files)} workloads")
        logger.info("="*60)
        
        return generated_files
    
    def _generate_workload(self, csv_file: Path, scenario_name: str, output_dir: Path) -> Optional[Path]:
        """
        Genera workload JSON da un singolo CSV.
        
        Args:
            csv_file: Path al file CSV
            scenario_name: Nome dello scenario
            output_dir: Directory di output
            
        Returns:
            Path: Path al file JSON generato, o None se fallisce
        """
        # Read CSV in chunks for memory safety
        logger.info(f"Reading CSV file: {csv_file}")
        
        chunks = []
        total_rows = 0
        
        # Read in chunks
        chunk_size = Config.CHUNK_SIZE
        for chunk in pd.read_csv(
            csv_file,
            chunksize=chunk_size,
            usecols=Config.DATASET_COLUMNS,
            encoding='utf-8',
            low_memory=False
        ):
            # Parse timestamps
            chunk['Vtime'] = pd.to_datetime(
                chunk['Vtime'],
                format=Config.TIMESTAMP_FORMAT,
                errors='coerce'
            )
            
            # Drop invalid timestamps
            chunk = chunk.dropna(subset=['Vtime'])
            
            if len(chunk) > 0:
                chunks.append(chunk)
                total_rows += len(chunk)
        
        if not chunks:
            logger.warning(f"No valid data in {csv_file}")
            return None
        
        # Combine chunks
        logger.info(f"Combining {len(chunks)} chunks ({total_rows:,} rows)...")
        df = pd.concat(chunks, ignore_index=True)
        
        # Sort by timestamp
        df = df.sort_values('Vtime').reset_index(drop=True)
        
        # Extract date range
        min_date = df['Vtime'].min()
        max_date = df['Vtime'].max()
        
        # Convert to events list
        logger.info("Converting to JSON format...")
        events = []
        
        for _, row in tqdm(df.iterrows(), total=len(df), desc="Converting events"):
            # Convert timestamp to Unix timestamp (float)
            timestamp = row['Vtime'].timestamp()
            
            event = {
                'timestamp': float(timestamp),
                'item_id': str(row['Item_id']),
                'user_id': str(row['User_id']),
                'action': str(row['Action'])
            }
            events.append(event)
        
        # Get scenario metadata if available
        scenario_meta = self.scenarios_metadata.get(scenario_name, {})
        
        # Build output structure
        output_data = {
            'metadata': {
                'scenario_name': scenario_name,
                'scenario_type': scenario_name,
                'source_file': str(csv_file.name),
                'generated_at': datetime.now().isoformat(),
                'total_events': len(events),
                'date_range': {
                    'start': min_date.strftime(Config.TIMESTAMP_FORMAT),
                    'end': max_date.strftime(Config.TIMESTAMP_FORMAT)
                },
                'duration_seconds': float((max_date - min_date).total_seconds()),
                'events_per_second': len(events) / max(1.0, (max_date - min_date).total_seconds())
            },
            'events': events
        }
        
        # Add scenario-specific metadata if available
        if scenario_meta:
            if 'score' in scenario_meta:
                output_data['metadata']['quality_score'] = scenario_meta['score']
            if 'selection_reason' in scenario_meta:
                output_data['metadata']['selection_reason'] = scenario_meta['selection_reason']
            
            # Add scenario-specific fields
            if scenario_name == 'hot_cold_shift':
                if 'date1' in scenario_meta and 'date2' in scenario_meta:
                    output_data['metadata']['hot_date'] = scenario_meta['date1']
                    output_data['metadata']['cold_date'] = scenario_meta['date2']
                    output_data['metadata']['overlap'] = scenario_meta.get('overlap')
                    output_data['metadata']['days_apart'] = scenario_meta.get('days_apart')
            
            elif scenario_name == 'burst_cooldown':
                if 'item_id' in scenario_meta:
                    output_data['metadata']['burst_item_id'] = scenario_meta['item_id']
                    output_data['metadata']['burst_date'] = scenario_meta.get('burst_date')
                    output_data['metadata']['intensity'] = scenario_meta.get('intensity')
                    output_data['metadata']['cv'] = scenario_meta.get('cv')
            
            elif scenario_name == 'scan_attack':
                if 'date_range' in scenario_meta:
                    output_data['metadata']['attack_window'] = scenario_meta['date_range']
                    output_data['metadata']['unique_rate'] = scenario_meta.get('unique_rate')
                    output_data['metadata']['repeat_rate'] = scenario_meta.get('repeat_rate')
            
            elif scenario_name == 'daily_pattern':
                if 'cycle_dates' in scenario_meta:
                    output_data['metadata']['cycle_dates'] = scenario_meta['cycle_dates']
                    output_data['metadata']['period_days'] = scenario_meta.get('period_days')
                    output_data['metadata']['autocorrelation'] = scenario_meta.get('autocorrelation')
            
            elif scenario_name == 'multi_modal':
                if 'date_range' in scenario_meta:
                    output_data['metadata']['modal_window'] = scenario_meta['date_range']
                    output_data['metadata']['num_clusters'] = scenario_meta.get('num_clusters')
                    output_data['metadata']['silhouette_score'] = scenario_meta.get('silhouette_score')
        
        # Determine output filename
        file_number = self.SCENARIO_MAPPING.get(scenario_name, '00')
        output_filename = f"{file_number}_{scenario_name}.json"
        output_file = output_dir / output_filename
        
        # Save JSON
        logger.info(f"Saving to {output_file}...")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        file_size_mb = output_file.stat().st_size / 1024 / 1024
        logger.info(f"Generated {len(events):,} events ({file_size_mb:.2f} MB)")
        
        return output_file


def main():
    """
    Entry point per lo script di generazione workload.
    """
    parser = argparse.ArgumentParser(
        description='Generate workload JSON files from extracted CSV data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 04_generate_workload.py --input data/scenarios/raw/
  python 04_generate_workload.py --input data/scenarios/raw/ --output custom_output/
        """
    )
    parser.add_argument(
        '--input',
        type=str,
        default=None,
        help='Input directory with CSV files (default: data/scenarios/raw/)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output directory for JSON files (default: data/scenarios/final/)'
    )
    parser.add_argument(
        '--scenarios',
        type=str,
        default=None,
        help='Path to scenarios_found.json for metadata (default: data/scenarios_found.json)'
    )
    
    args = parser.parse_args()
    
    # Set default paths
    if args.input is None:
        input_dir = Config.RAW_SCENARIOS_DIR
    else:
        input_dir = Path(args.input)
    
    if args.output is None:
        output_dir = Config.FINAL_SCENARIOS_DIR
    else:
        output_dir = Path(args.output)
    
    if args.scenarios is None:
        scenarios_path = Config.DATA_DIR / 'scenarios_found.json'
    else:
        scenarios_path = Path(args.scenarios)
    
    # Validate inputs
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        logger.error("Please run 03_extract_data.py first to create CSV files")
        return 1
    
    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Run generation
    try:
        generator = WorkloadGenerator(scenarios_path if scenarios_path.exists() else None)
        generated_files = generator.generate_all_workloads(input_dir, output_dir)
        
        logger.info("\n" + "="*60)
        logger.info("Generation Summary:")
        logger.info("="*60)
        for scenario_name, file_path in generated_files.items():
            file_size_mb = file_path.stat().st_size / 1024 / 1024
            
            # Load and show event count
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    event_count = data.get('metadata', {}).get('total_events', 0)
                    logger.info(f"  {scenario_name}: {file_path.name} ({event_count:,} events, {file_size_mb:.2f} MB)")
            except Exception:
                logger.info(f"  {scenario_name}: {file_path.name} ({file_size_mb:.2f} MB)")
        
        logger.info(f"\n[SUCCESS] All workloads generated!")
        logger.info(f"Output directory: {output_dir}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error generating workloads: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

#!/usr/bin/env python3
"""
validate_scenarios.py - Scenario Validator

Valida gli scenari generati verificando:
- Validità JSON
- Numero eventi e date range
- Coerenza dei pattern
- Qualità degli scenari

Usage:
    python validate_scenarios.py --scenarios-dir data/scenarios/final/

Author: Scenario Generation System
Date: 2025-02-02
"""

import json
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from collections import Counter

from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class ScenarioValidator:
    """
    Valida gli scenari generati verificando struttura, coerenza e qualità.
    """
    
    def __init__(self):
        """Inizializza il validatore."""
        self.validation_results = {}
        self.errors = []
        self.warnings = []
    
    def validate_all_scenarios(self, scenarios_dir: Path) -> Dict:
        """
        Valida tutti gli scenari nella directory.
        
        Args:
            scenarios_dir: Directory contenente i file JSON degli scenari
            
        Returns:
            dict: Risultati della validazione
        """
        logger.info("\n" + "="*60)
        logger.info("Starting scenario validation...")
        logger.info("="*60)
        
        json_files = sorted(scenarios_dir.glob("*.json"))
        
        if not json_files:
            logger.error(f"No JSON files found in {scenarios_dir}")
            return {'valid': False, 'errors': ['No scenarios found']}
        
        logger.info(f"Found {len(json_files)} scenario files to validate")
        
        for json_file in json_files:
            logger.info(f"\n[{json_file.name}] Validating...")
            result = self._validate_scenario(json_file)
            self.validation_results[json_file.name] = result
        
        # Summary
        self._print_summary()
        
        return {
            'valid': len(self.errors) == 0,
            'total_scenarios': len(json_files),
            'valid_scenarios': sum(1 for r in self.validation_results.values() if r['valid']),
            'errors': self.errors,
            'warnings': self.warnings,
            'results': self.validation_results
        }
    
    def _validate_scenario(self, json_file: Path) -> Dict:
        """
        Valida un singolo scenario.
        
        Args:
            json_file: Path al file JSON dello scenario
            
        Returns:
            dict: Risultati validazione per questo scenario
        """
        result = {
            'file': str(json_file),
            'valid': True,
            'errors': [],
            'warnings': [],
            'checks': {}
        }
        
        # Check 1: File exists and is readable
        if not json_file.exists():
            error = f"File not found: {json_file}"
            result['errors'].append(error)
            result['valid'] = False
            self.errors.append(f"{json_file.name}: {error}")
            return result
        
        # Check 2: Valid JSON
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            error = f"Invalid JSON: {e}"
            result['errors'].append(error)
            result['valid'] = False
            self.errors.append(f"{json_file.name}: {error}")
            return result
        except Exception as e:
            error = f"Error reading file: {e}"
            result['errors'].append(error)
            result['valid'] = False
            self.errors.append(f"{json_file.name}: {error}")
            return result
        
        # Check 3: Required structure
        if 'metadata' not in data:
            error = "Missing 'metadata' key"
            result['errors'].append(error)
            result['valid'] = False
            self.errors.append(f"{json_file.name}: {error}")
        
        if 'events' not in data:
            error = "Missing 'events' key"
            result['errors'].append(error)
            result['valid'] = False
            self.errors.append(f"{json_file.name}: {error}")
            return result
        
        metadata = data.get('metadata', {})
        events = data.get('events', [])
        
        # Check 4: Metadata validation
        result['checks']['metadata'] = self._validate_metadata(metadata, json_file.name)
        if not result['checks']['metadata']['valid']:
            result['errors'].extend(result['checks']['metadata']['errors'])
            result['warnings'].extend(result['checks']['metadata']['warnings'])
            result['valid'] = False
        
        # Check 5: Events validation
        result['checks']['events'] = self._validate_events(events, metadata)
        if not result['checks']['events']['valid']:
            result['errors'].extend(result['checks']['events']['errors'])
            result['warnings'].extend(result['checks']['events']['warnings'])
            result['valid'] = False
        
        # Check 6: Pattern coherence
        result['checks']['pattern'] = self._validate_pattern_coherence(metadata, events, json_file.name)
        if not result['checks']['pattern']['valid']:
            result['warnings'].extend(result['checks']['pattern']['warnings'])
        
        # Collect errors and warnings
        if result['errors']:
            self.errors.extend([f"{json_file.name}: {e}" for e in result['errors']])
        if result['warnings']:
            self.warnings.extend([f"{json_file.name}: {w}" for w in result['warnings']])
        
        # Print results
        if result['valid']:
            logger.info(f"[SUCCESS] Scenario is valid")
        else:
            logger.error(f"[FAILED] Scenario has {len(result['errors'])} errors")
        
        if result['warnings']:
            logger.warning(f"[WARNING] {len(result['warnings'])} warnings")
        
        return result
    
    def _validate_metadata(self, metadata: Dict, scenario_name: str) -> Dict:
        """
        Valida i metadata dello scenario.
        
        Args:
            metadata: Dizionario metadata
            scenario_name: Nome dello scenario
            
        Returns:
            dict: Risultati validazione metadata
        """
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        # Required fields
        required_fields = ['scenario_name', 'total_events']
        for field in required_fields:
            if field not in metadata:
                result['errors'].append(f"Missing required metadata field: {field}")
                result['valid'] = False
        
        # Check total_events
        total_events = metadata.get('total_events', 0)
        if total_events < Config.MIN_SCENARIO_EVENTS:
            result['errors'].append(
                f"Too few events: {total_events} < {Config.MIN_SCENARIO_EVENTS} (minimum)"
            )
            result['valid'] = False
        elif total_events > Config.MAX_SCENARIO_EVENTS:
            result['warnings'].append(
                f"Many events: {total_events} > {Config.MAX_SCENARIO_EVENTS} (maximum recommended)"
            )
        
        # Check date_range
        date_range = metadata.get('date_range', {})
        if 'start' in date_range and 'end' in date_range:
            try:
                if isinstance(date_range['start'], (int, float)):
                    start_ts = date_range['start']
                    end_ts = date_range['end']
                    duration = end_ts - start_ts
                else:
                    start_dt = datetime.fromisoformat(date_range['start'].replace('Z', '+00:00'))
                    end_dt = datetime.fromisoformat(date_range['end'].replace('Z', '+00:00'))
                    duration = (end_dt - start_dt).total_seconds()
                
                if duration < 0:
                    result['errors'].append("Invalid date_range: end < start")
                    result['valid'] = False
                elif duration == 0:
                    result['warnings'].append("Date range has zero duration")
            except Exception as e:
                result['warnings'].append(f"Could not parse date_range: {e}")
        else:
            result['warnings'].append("Missing or incomplete date_range")
        
        # Check quality_score or score
        if 'quality_score' not in metadata and 'score' not in metadata:
            result['warnings'].append("Missing quality score")
        
        # Check selection_reason
        if 'selection_reason' not in metadata:
            result['warnings'].append("Missing selection_reason")
        
        return result
    
    def _validate_events(self, events: List[Dict], metadata: Dict) -> Dict:
        """
        Valida la lista di eventi.
        
        Args:
            events: Lista di eventi
            metadata: Metadata dello scenario
            
        Returns:
            dict: Risultati validazione eventi
        """
        result = {'valid': True, 'errors': [], 'warnings': []}
        
        if not events:
            result['errors'].append("Empty events list")
            result['valid'] = False
            return result
        
        # Check event count matches metadata
        total_events_meta = metadata.get('total_events', 0)
        if len(events) != total_events_meta:
            result['warnings'].append(
                f"Event count mismatch: metadata says {total_events_meta}, "
                f"but events list has {len(events)}"
            )
        
        # Check event structure
        required_fields = ['timestamp', 'item_id', 'user_id', 'action']
        invalid_events = 0
        
        timestamps = []
        item_ids = set()
        user_ids = set()
        actions = Counter()
        
        for i, event in enumerate(events):
            # Check required fields
            missing_fields = [f for f in required_fields if f not in event]
            if missing_fields:
                invalid_events += 1
                if invalid_events <= 5:  # Log first 5 only
                    result['warnings'].append(
                        f"Event {i} missing fields: {missing_fields}"
                    )
                continue
            
            # Validate timestamp
            try:
                ts = float(event['timestamp'])
                timestamps.append(ts)
            except (ValueError, TypeError):
                invalid_events += 1
                if invalid_events <= 5:
                    result['warnings'].append(f"Event {i} has invalid timestamp: {event['timestamp']}")
                continue
            
            # Collect statistics
            item_ids.add(str(event['item_id']))
            user_ids.add(str(event['user_id']))
            actions[str(event['action'])] += 1
        
        if invalid_events > 0:
            result['warnings'].append(f"{invalid_events} events have invalid structure")
        
        # Check timestamp ordering
        if len(timestamps) > 1:
            is_sorted = all(timestamps[i] <= timestamps[i+1] for i in range(len(timestamps)-1))
            if not is_sorted:
                result['warnings'].append("Events are not sorted by timestamp")
        
        # Check date range consistency
        if timestamps:
            min_ts = min(timestamps)
            max_ts = max(timestamps)
            
            date_range = metadata.get('date_range', {})
            if 'start' in date_range:
                try:
                    if isinstance(date_range['start'], (int, float)):
                        meta_start = date_range['start']
                    else:
                        meta_start = datetime.fromisoformat(
                            date_range['start'].replace('Z', '+00:00')
                        ).timestamp()
                    
                    if abs(meta_start - min_ts) > 86400:  # More than 1 day difference
                        result['warnings'].append(
                            f"Date range start mismatch: metadata={date_range['start']}, "
                            f"actual first event={datetime.fromtimestamp(min_ts)}"
                        )
                except Exception:
                    pass
        
        # Statistics
        result['statistics'] = {
            'total_events': len(events),
            'unique_items': len(item_ids),
            'unique_users': len(user_ids),
            'action_distribution': dict(actions),
            'timestamp_range': {
                'start': min(timestamps) if timestamps else None,
                'end': max(timestamps) if timestamps else None
            }
        }
        
        return result
    
    def _validate_pattern_coherence(self, metadata: Dict, events: List[Dict], scenario_name: str) -> Dict:
        """
        Valida la coerenza del pattern per tipo di scenario.
        
        Args:
            metadata: Metadata dello scenario
            events: Lista di eventi
            scenario_name: Nome del file scenario
            
        Returns:
            dict: Risultati validazione pattern
        """
        result = {'valid': True, 'warnings': []}
        
        scenario_type = metadata.get('scenario_name', '').replace('.json', '')
        
        if scenario_type == 'hot_cold_shift':
            # Check that we have data from two distinct dates
            if 'hot_date' in metadata and 'cold_date' in metadata:
                result['valid'] = True
            else:
                result['warnings'].append("Missing hot_date or cold_date in metadata")
        
        elif scenario_type == 'burst_cooldown':
            # Check burst pattern characteristics
            if 'burst_item_id' in metadata:
                # Verify the burst item appears in events
                burst_item = metadata['burst_item_id']
                item_counts = Counter(e['item_id'] for e in events)
                if burst_item not in item_counts:
                    result['warnings'].append(
                        f"Burst item {burst_item} not found in events"
                    )
                else:
                    # Check if it's actually a burst (high frequency)
                    max_count = max(item_counts.values())
                    burst_count = item_counts.get(burst_item, 0)
                    if burst_count < max_count * 0.5:
                        result['warnings'].append(
                            f"Burst item {burst_item} doesn't appear to be the most frequent "
                            f"({burst_count} vs {max_count} max)"
                        )
        
        elif scenario_type == 'scan_attack':
            # Check unique rate
            unique_rate = metadata.get('unique_rate')
            if unique_rate is not None:
                if unique_rate < 0.7:
                    result['warnings'].append(
                        f"Low unique rate for scan attack: {unique_rate:.2%} (expected >80%)"
                    )
        
        elif scenario_type == 'daily_pattern':
            # Check autocorrelation
            autocorr = metadata.get('autocorrelation')
            if autocorr is not None:
                if autocorr < 0.5:
                    result['warnings'].append(
                        f"Low autocorrelation for daily pattern: {autocorr:.3f} (expected >0.6)"
                    )
        
        elif scenario_type == 'multi_modal':
            # Check silhouette score
            silhouette = metadata.get('silhouette_score')
            if silhouette is not None:
                if silhouette < 0.4:
                    result['warnings'].append(
                        f"Low silhouette score for multi-modal: {silhouette:.3f} (expected >0.5)"
                    )
        
        elif scenario_type == 'adversarial':
            # Check attack pattern
            attack_pattern = metadata.get('attack_pattern')
            if attack_pattern:
                result['valid'] = True
            # Check threshold factor
            threshold_factor = metadata.get('threshold_factor')
            if threshold_factor and threshold_factor < 1.0:
                result['warnings'].append(
                    f"Threshold factor < 1.0: {threshold_factor} (expected >1.0)"
                )
        
        return result
    
    def _print_summary(self):
        """Stampa riepilogo validazione."""
        logger.info("\n" + "="*60)
        logger.info("Validation Summary")
        logger.info("="*60)
        
        total = len(self.validation_results)
        valid = sum(1 for r in self.validation_results.values() if r['valid'])
        
        logger.info(f"Total scenarios: {total}")
        logger.info(f"Valid scenarios: {valid}")
        logger.info(f"Invalid scenarios: {total - valid}")
        logger.info(f"Total errors: {len(self.errors)}")
        logger.info(f"Total warnings: {len(self.warnings)}")
        
        if self.errors:
            logger.error("\nErrors:")
            for error in self.errors[:10]:  # Show first 10
                logger.error(f"  - {error}")
            if len(self.errors) > 10:
                logger.error(f"  ... and {len(self.errors) - 10} more errors")
        
        if self.warnings:
            logger.warning("\nWarnings:")
            for warning in self.warnings[:10]:  # Show first 10
                logger.warning(f"  - {warning}")
            if len(self.warnings) > 10:
                logger.warning(f"  ... and {len(self.warnings) - 10} more warnings")
        
        # Detailed results per scenario
        logger.info("\nDetailed Results:")
        for filename, result in self.validation_results.items():
            status = "✓ VALID" if result['valid'] else "✗ INVALID"
            logger.info(f"\n  {filename}: {status}")
            
            if 'checks' in result:
                for check_name, check_result in result['checks'].items():
                    check_status = "✓" if check_result.get('valid', True) else "✗"
                    logger.info(f"    {check_status} {check_name}")
                    
                    if 'statistics' in check_result:
                        stats = check_result['statistics']
                        logger.info(f"      Events: {stats.get('total_events', 'N/A'):,}")
                        logger.info(f"      Unique items: {stats.get('unique_items', 'N/A'):,}")
                        logger.info(f"      Unique users: {stats.get('unique_users', 'N/A'):,}")


def main():
    """
    Entry point per lo script di validazione.
    """
    parser = argparse.ArgumentParser(
        description='Validate generated scenario JSON files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validate_scenarios.py
  python validate_scenarios.py --scenarios-dir data/scenarios/final/
  python validate_scenarios.py --scenarios-dir data/scenarios/final/ --output validation_report.json
        """
    )
    parser.add_argument(
        '--scenarios-dir',
        type=str,
        default=None,
        help='Directory with scenario JSON files (default: data/scenarios/final/)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output path for validation report JSON (optional)'
    )
    
    args = parser.parse_args()
    
    # Set default path
    if args.scenarios_dir is None:
        scenarios_dir = Config.FINAL_SCENARIOS_DIR
    else:
        scenarios_dir = Path(args.scenarios_dir)
    
    if not scenarios_dir.exists():
        logger.error(f"Scenarios directory not found: {scenarios_dir}")
        return 1
    
    # Run validation
    try:
        validator = ScenarioValidator()
        results = validator.validate_all_scenarios(scenarios_dir)
        
        # Save report if requested
        if args.output:
            output_path = Path(args.output)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"\nValidation report saved to: {output_path}")
        
        # Return exit code
        return 0 if results['valid'] else 1
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error during validation: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

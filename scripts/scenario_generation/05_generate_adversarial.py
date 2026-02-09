#!/usr/bin/env python3
"""
05_generate_adversarial.py - Adversarial Scenario Generator

Genera uno scenario sintetico adversarial per testare il sistema di cache
in condizioni estreme. Lo scenario è completamente sintetico e non basato
su dati reali del dataset.

Usage:
    python 05_generate_adversarial.py --output data/scenarios/final/06_adversarial.json

Author: Scenario Generation System
Date: 2025-02-02
"""

import json
import numpy as np
from pathlib import Path
from tqdm import tqdm
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, List
import random

from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class AdversarialGenerator:
    """
    Genera scenario adversarial sintetico.
    
    Crea un workload sintetico progettato per stressare il sistema di cache
    con pattern di accesso che cercano di bypassare le ottimizzazioni.
    """
    
    def __init__(self, seed: int = None):
        """
        Inizializza il generatore.
        
        Args:
            seed: Seed per riproducibilità (default: Config.RANDOM_SEED)
        """
        self.seed = seed or Config.RANDOM_SEED
        np.random.seed(self.seed)
        random.seed(self.seed)
        
        logger.info(f"Initialized AdversarialGenerator with seed={self.seed}")
    
    def generate(self, output_path: Path) -> Path:
        """
        Genera lo scenario adversarial completo.
        
        Args:
            output_path: Path dove salvare il file JSON
            
        Returns:
            Path: Path al file generato
        """
        params = Config.SCENARIOS['adversarial']
        
        logger.info("\n" + "="*60)
        logger.info("Generating adversarial scenario...")
        logger.info("="*60)
        logger.info(f"Parameters:")
        logger.info(f"  - num_items: {params['num_items']}")
        logger.info(f"  - target_events: {params['target_events']}")
        logger.info(f"  - threshold_factor: {params['threshold_factor']}")
        logger.info(f"  - attack_pattern: {params['attack_pattern']}")
        
        # Generate item frequencies based on attack pattern
        item_frequencies = self._generate_item_frequencies(
            params['num_items'],
            params['target_events'],
            params['threshold_factor'],
            params['attack_pattern']
        )
        
        # Generate events
        logger.info("\nGenerating events...")
        events = self._generate_events(
            item_frequencies,
            params['target_events'],
            params['attack_pattern']
        )
        
        # Calculate statistics
        logger.info("Calculating statistics...")
        stats = self._calculate_statistics(events, item_frequencies)
        
        # Build output structure
        output_data = {
            'metadata': {
                'scenario_name': 'adversarial',
                'scenario_type': 'adversarial',
                'generated_at': datetime.now().isoformat(),
                'seed': self.seed,
                'total_events': len(events),
                'num_items': params['num_items'],
                'attack_pattern': params['attack_pattern'],
                'threshold_factor': params['threshold_factor'],
                'date_range': {
                    'start': events[0]['timestamp'] if events else None,
                    'end': events[-1]['timestamp'] if events else None
                },
                'duration_seconds': (
                    events[-1]['timestamp'] - events[0]['timestamp']
                    if len(events) > 1 else 0.0
                ),
                'statistics': stats,
                'selection_reason': (
                    f"Synthetic adversarial scenario with {params['attack_pattern']} pattern. "
                    f"Designed to stress-test cache system with {params['num_items']} items "
                    f"at threshold_factor={params['threshold_factor']}x. "
                    f"Total events: {len(events):,}."
                )
            },
            'events': events
        }
        
        # Save JSON
        output_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"\nSaving to {output_path}...")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        file_size_mb = output_path.stat().st_size / 1024 / 1024
        logger.info(f"Generated {len(events):,} events ({file_size_mb:.2f} MB)")
        
        logger.info("\n" + "="*60)
        logger.info("Adversarial scenario generation complete!")
        logger.info("="*60)
        
        return output_path
    
    def _generate_item_frequencies(
        self,
        num_items: int,
        target_events: int,
        threshold_factor: float,
        attack_pattern: str
    ) -> Dict[str, int]:
        """
        Genera frequenze per gli item basate sul pattern di attacco.
        
        Args:
            num_items: Numero di item da generare
            target_events: Numero totale di eventi target
            threshold_factor: Fattore moltiplicativo per la threshold
            attack_pattern: Pattern di attacco ('uniform', 'burst', 'gradual')
            
        Returns:
            dict: Mapping item_id -> frequenza
        """
        item_frequencies = {}
        
        # Calcola threshold base (frequenza media per item)
        base_threshold = target_events / num_items
        
        if attack_pattern == 'uniform':
            # Distribuzione uniforme: tutti gli item hanno frequenza simile
            # appena sopra la threshold
            frequency = int(base_threshold * threshold_factor)
            for i in range(num_items):
                item_id = f"adv_item_{i:06d}"
                # Aggiungi piccola variazione randomica
                variation = np.random.randint(-int(frequency * 0.1), int(frequency * 0.1))
                item_frequencies[item_id] = max(1, frequency + variation)
        
        elif attack_pattern == 'burst':
            # Pattern burst: alcuni item hanno frequenze molto alte,
            # altri molto basse, ma tutti sopra threshold
            for i in range(num_items):
                item_id = f"adv_item_{i:06d}"
                # 20% degli item hanno burst (frequenza alta)
                if i < num_items * 0.2:
                    # Burst items: 3-5x la threshold
                    multiplier = np.random.uniform(3.0, 5.0)
                    item_frequencies[item_id] = int(base_threshold * multiplier)
                else:
                    # Altri item: appena sopra threshold
                    item_frequencies[item_id] = int(base_threshold * threshold_factor)
        
        elif attack_pattern == 'gradual':
            # Pattern graduale: frequenze aumentano gradualmente
            # da threshold a 2x threshold
            for i in range(num_items):
                item_id = f"adv_item_{i:06d}"
                # Frequenza aumenta linearmente
                progress = i / num_items
                multiplier = threshold_factor + (2.0 - threshold_factor) * progress
                item_frequencies[item_id] = int(base_threshold * multiplier)
        
        else:
            # Default: uniform
            frequency = int(base_threshold * threshold_factor)
            for i in range(num_items):
                item_id = f"adv_item_{i:06d}"
                item_frequencies[item_id] = frequency
        
        # Normalizza per raggiungere esattamente target_events
        total_generated = sum(item_frequencies.values())
        if total_generated != target_events:
            # Aggiusta proporzionalmente
            scale_factor = target_events / total_generated
            for item_id in item_frequencies:
                item_frequencies[item_id] = max(1, int(item_frequencies[item_id] * scale_factor))
            
            # Aggiusta per raggiungere esattamente il target
            current_total = sum(item_frequencies.values())
            diff = target_events - current_total
            if diff != 0:
                # Distribuisci la differenza su item random
                items_list = list(item_frequencies.keys())
                for _ in range(abs(diff)):
                    item_id = random.choice(items_list)
                    if diff > 0:
                        item_frequencies[item_id] += 1
                    else:
                        item_frequencies[item_id] = max(1, item_frequencies[item_id] - 1)
        
        logger.info(f"Generated frequencies for {len(item_frequencies)} items")
        logger.info(f"  Min frequency: {min(item_frequencies.values())}")
        logger.info(f"  Max frequency: {max(item_frequencies.values())}")
        logger.info(f"  Mean frequency: {np.mean(list(item_frequencies.values())):.1f}")
        logger.info(f"  Total events: {sum(item_frequencies.values()):,}")
        
        return item_frequencies
    
    def _generate_events(
        self,
        item_frequencies: Dict[str, int],
        target_events: int,
        attack_pattern: str
    ) -> List[Dict]:
        """
        Genera lista di eventi basata sulle frequenze degli item.
        
        Args:
            item_frequencies: Mapping item_id -> frequenza
            target_events: Numero target di eventi
            attack_pattern: Pattern di attacco
            
        Returns:
            list: Lista di eventi ordinati per timestamp
        """
        events = []
        
        # Genera timestamp base (ultimi 7 giorni)
        start_time = datetime.now() - timedelta(days=7)
        end_time = datetime.now()
        duration_seconds = (end_time - start_time).total_seconds()
        
        # Genera eventi per ogni item
        for item_id, frequency in tqdm(item_frequencies.items(), desc="Generating events"):
            # Genera timestamp per questo item basato sul pattern
            if attack_pattern == 'uniform':
                # Timestamp distribuiti uniformemente
                timestamps = np.linspace(
                    start_time.timestamp(),
                    end_time.timestamp(),
                    frequency
                )
            elif attack_pattern == 'burst':
                # Burst: eventi concentrati in finestre temporali
                num_bursts = max(1, frequency // 100)
                timestamps = []
                for _ in range(num_bursts):
                    burst_start = np.random.uniform(
                        start_time.timestamp(),
                        end_time.timestamp() - duration_seconds * 0.1
                    )
                    burst_duration = duration_seconds * 0.1
                    burst_timestamps = np.random.uniform(
                        burst_start,
                        burst_start + burst_duration,
                        frequency // num_bursts
                    )
                    timestamps.extend(burst_timestamps)
                # Aggiungi eventi rimanenti
                remaining = frequency - len(timestamps)
                if remaining > 0:
                    timestamps.extend(
                        np.random.uniform(
                            start_time.timestamp(),
                            end_time.timestamp(),
                            remaining
                        )
                    )
            else:  # gradual
                # Graduale: eventi distribuiti con densità crescente
                timestamps = []
                for i in range(frequency):
                    progress = i / frequency
                    # Densità aumenta nel tempo
                    time_offset = duration_seconds * (progress ** 0.5)
                    timestamp = start_time.timestamp() + time_offset + np.random.uniform(-3600, 3600)
                    timestamps.append(timestamp)
            
            # Genera eventi per questo item
            for timestamp in timestamps:
                # Genera user_id sintetico
                user_id = f"adv_user_{np.random.randint(1, 1000):06d}"
                
                # Genera action (distribuzione simile al dataset reale)
                actions = ['pv', 'buy', 'cart', 'fav']
                weights = [0.7, 0.1, 0.15, 0.05]  # pv è più comune
                action = np.random.choice(actions, p=weights)
                
                event = {
                    'timestamp': float(timestamp),
                    'item_id': item_id,
                    'user_id': user_id,
                    'action': action
                }
                events.append(event)
        
        # Ordina per timestamp
        events.sort(key=lambda x: x['timestamp'])
        
        logger.info(f"Generated {len(events):,} events")
        logger.info(f"  Time range: {datetime.fromtimestamp(events[0]['timestamp'])} to {datetime.fromtimestamp(events[-1]['timestamp'])}")
        
        return events
    
    def _calculate_statistics(
        self,
        events: List[Dict],
        item_frequencies: Dict[str, int]
    ) -> Dict:
        """
        Calcola statistiche sullo scenario generato.
        
        Args:
            events: Lista di eventi
            item_frequencies: Frequenze degli item
            
        Returns:
            dict: Statistiche calcolate
        """
        # Statistiche base
        unique_items = len(item_frequencies)
        unique_users = len(set(e['user_id'] for e in events))
        
        # Distribuzione azioni
        action_counts = {}
        for event in events:
            action = event['action']
            action_counts[action] = action_counts.get(action, 0) + 1
        
        # Frequenze item
        freq_values = list(item_frequencies.values())
        
        stats = {
            'unique_items': unique_items,
            'unique_users': unique_users,
            'action_distribution': action_counts,
            'item_frequency_stats': {
                'min': int(min(freq_values)),
                'max': int(max(freq_values)),
                'mean': float(np.mean(freq_values)),
                'median': float(np.median(freq_values)),
                'std': float(np.std(freq_values))
            },
            'events_per_item_mean': float(np.mean(freq_values)),
            'events_per_item_std': float(np.std(freq_values))
        }
        
        return stats


def main():
    """
    Entry point per lo script di generazione adversarial.
    """
    parser = argparse.ArgumentParser(
        description='Generate synthetic adversarial scenario for cache testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 05_generate_adversarial.py
  python 05_generate_adversarial.py --output custom_path.json --seed 123
        """
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output path for JSON file (default: data/scenarios/final/06_adversarial.json)'
    )
    parser.add_argument(
        '--seed',
        type=int,
        default=None,
        help=f'Random seed (default: {Config.RANDOM_SEED})'
    )
    
    args = parser.parse_args()
    
    # Set default output path
    if args.output is None:
        output_path = Config.FINAL_SCENARIOS_DIR / '06_adversarial.json'
    else:
        output_path = Path(args.output)
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Run generation
    try:
        generator = AdversarialGenerator(seed=args.seed)
        generated_file = generator.generate(output_path)
        
        logger.info("\n" + "="*60)
        logger.info(f"[SUCCESS] Adversarial scenario generated!")
        logger.info(f"Output: {generated_file}")
        logger.info("="*60)
        
        # Show file info
        file_size_mb = generated_file.stat().st_size / 1024 / 1024
        with open(generated_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            event_count = data.get('metadata', {}).get('total_events', 0)
            logger.info(f"\nFile statistics:")
            logger.info(f"  Events: {event_count:,}")
            logger.info(f"  Size: {file_size_mb:.2f} MB")
            logger.info(f"  Pattern: {data.get('metadata', {}).get('attack_pattern', 'unknown')}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error generating adversarial scenario: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

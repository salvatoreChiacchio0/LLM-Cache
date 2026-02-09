#!/usr/bin/env python3
"""
02_find_scenarios.py - Scenario Finder

Analizza l'indice e identifica le migliori candidate windows per ogni scenario.

Usage:
    python 02_find_scenarios.py --index data/tianchi_index.json --output scenarios_found.json

Author: Scenario Generation System
Date: 2025-02-02
"""

import json
import numpy as np
from pathlib import Path
from tqdm import tqdm
import argparse
import logging
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, List, Optional

from utils.stats_calculator import (
    calculate_zipf_alpha,
    calculate_coefficient_of_variation,
    calculate_autocorrelation
)
from config import Config

# Setup logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format=Config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class ScenarioFinder:
    """
    Trova le migliori candidate windows per ogni tipo di scenario.
    
    Analizza l'indice giornaliero e identifica pattern che corrispondono
    ai criteri definiti in Config.SCENARIOS.
    """
    
    def __init__(self, index_path):
        """
        Inizializza il finder caricando l'indice.
        
        Args:
            index_path: Path al file JSON dell'indice
            
        Raises:
            FileNotFoundError: Se il file indice non esiste
            ValueError: Se il file indice è malformato
        """
        index_path = Path(index_path)
        if not index_path.exists():
            raise FileNotFoundError(f"Index file not found: {index_path}")
        
        try:
            with open(index_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in index file: {e}")
        except Exception as e:
            raise ValueError(f"Error reading index file: {e}")
        
        if 'index' not in data:
            raise ValueError("Index file missing 'index' key")
        if 'metadata' not in data:
            raise ValueError("Index file missing 'metadata' key")
        
        self.index = data['index']
        self.metadata = data['metadata']
        self.dates = sorted(self.index.keys())
        
        logger.info(f"Loaded index: {len(self.dates)} days from {self.dates[0]} to {self.dates[-1]}")
        logger.info(f"Index version: {self.metadata.get('version', 'unknown')}")
    
    def find_all_scenarios(self) -> Dict:
        """
        Trova tutti gli scenari e ritorna un dict con i risultati.
        
        Returns:
            dict: Dizionario con struttura: {'metadata': {...}, 'scenarios': {...}}
        """
        logger.info("\n" + "="*60)
        logger.info("Starting scenario discovery...")
        logger.info("="*60)
        
        scenarios = {}
        
        logger.info("\n[1/5] Finding Hot-Cold Shift...")
        scenarios['hot_cold_shift'] = self.find_hot_cold_shift()
        
        logger.info("\n[2/5] Finding Burst-Cooldown patterns...")
        scenarios['burst_cooldown'] = self.find_burst_patterns()
        
        logger.info("\n[3/5] Finding Scan Attack windows...")
        scenarios['scan_attack'] = self.find_scan_attack()
        
        logger.info("\n[4/5] Finding Daily Pattern cycles...")
        scenarios['daily_pattern'] = self.find_daily_pattern()
        
        logger.info("\n[5/5] Finding Multi-Modal distribution...")
        scenarios['multi_modal'] = self.find_multi_modal()
        
        # Count found scenarios
        found_count = sum(1 for v in scenarios.values() if v is not None)
        
        results = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'index_source': str(self.metadata.get('source_file', 'unknown')),
                'total_scenarios_found': found_count,
                'date_range': {
                    'start': self.dates[0],
                    'end': self.dates[-1]
                },
                'total_days': len(self.dates)
            },
            'scenarios': scenarios
        }
        
        logger.info("\n" + "="*60)
        logger.info("Scenario discovery complete!")
        logger.info(f"Found {found_count}/5 scenarios")
        logger.info("="*60)
        
        return results
    
    def find_hot_cold_shift(self) -> Optional[Dict]:
        """
        Trova la migliore coppia di date con Hot → Cold shift.
        
        Criteri:
        - Overlap top-20 < 30%
        - Distanza temporale >= 14 giorni
        - Entrambe le finestre hanno >1000 eventi
        - Massimizza "shift intensity"
        
        Returns:
            dict: Dizionario con informazioni sul shift trovato, o None se non trovato
        """
        params = Config.SCENARIOS['hot_cold_shift']
        
        best_shift = None
        best_score = -1.0
        
        # Top-K items da considerare (default 20, ma può essere configurato)
        top_k = min(20, params.get('top_k', 20))
        
        # Calcola numero totale di coppie da esaminare
        total_pairs = sum(
            len(self.dates) - i - params['min_shift_days']
            for i in range(len(self.dates) - params['min_shift_days'])
        )
        logger.info(f"Scanning {total_pairs:,} date pairs for Hot-Cold shift...")
        logger.info(f"Criteria: overlap < {params['max_overlap']:.0%}, gap >= {params['min_shift_days']} days")
        
        for i in tqdm(range(len(self.dates) - params['min_shift_days']), 
                     desc="Scanning shifts"):
            date1 = self.dates[i]
            
            # Salta giorni con pochi eventi
            if self.index[date1]['total_events'] < Config.MIN_SCENARIO_EVENTS:
                continue
            
            # Cerca date2 almeno N giorni dopo
            for j in range(i + params['min_shift_days'], len(self.dates)):
                date2 = self.dates[j]
                
                if self.index[date2]['total_events'] < Config.MIN_SCENARIO_EVENTS:
                    continue
                
                # Calcola overlap top-K
                top1_items = [item for item, _ in self.index[date1]['top_100'][:top_k]]
                top2_items = [item for item, _ in self.index[date2]['top_100'][:top_k]]
                
                top1_set = set(top1_items)
                top2_set = set(top2_items)
                
                overlap = len(top1_set & top2_set) / top_k
                
                # Verifica criterio overlap
                if overlap >= params['max_overlap']:
                    continue
                
                # Calcola score (considera anche separazione temporale)
                days_apart = j - i
                shift_intensity = 1.0 - overlap
                temporal_separation = min(days_apart / 30.0, 1.0)  # Normalizzato
                score = shift_intensity * 0.7 + temporal_separation * 0.3
                
                if score > best_score:
                    best_score = score
                    best_shift = {
                        'date1': date1,
                        'date2': date2,
                        'overlap': overlap,
                        'days_apart': days_apart,
                        'top1': top1_items,
                        'top2': top2_items,
                        'survivors': list(top1_set & top2_set),
                        'events_date1': self.index[date1]['total_events'],
                        'events_date2': self.index[date2]['total_events'],
                        'score': score,
                        'zipf_alpha_date1': self.index[date1]['statistics'].get('zipf_alpha'),
                        'zipf_alpha_date2': self.index[date2]['statistics'].get('zipf_alpha'),
                        'gini_date1': self.index[date1]['statistics'].get('gini_coefficient'),
                        'gini_date2': self.index[date2]['statistics'].get('gini_coefficient')
                    }
        
        if best_shift:
            # Normalizza score a [0, 1]
            best_shift['score'] = min(1.0, max(0.0, best_shift['score']))
            
            # Aggiungi selection reason
            overlap_pct = best_shift['overlap'] * 100
            best_shift['selection_reason'] = (
                f"Minimal overlap ({overlap_pct:.1f}%) with maximum temporal separation "
                f"({best_shift['days_apart']} days). Score: {best_shift['score']:.3f}"
            )
            
            logger.info(f"[SUCCESS] Found shift: {best_shift['date1']} → {best_shift['date2']}")
            logger.info(f"          Overlap: {best_shift['overlap']:.2%}, Days apart: {best_shift['days_apart']}")
            logger.info(f"          Score: {best_shift['score']:.3f}")
            logger.info(f"          Reason: {best_shift['selection_reason']}")
        else:
            logger.warning("[WARNING] No suitable Hot-Cold shift found")
        
        return best_shift
    
    def find_burst_patterns(self) -> Optional[Dict]:
        """
        Trova item con pattern burst-cooldown.
        
        Per ogni item, costruisce una timeline giornaliera e identifica
        burst con alta intensità e coefficiente di variazione.
        
        Returns:
            dict: Il miglior burst pattern con timeline completo, o None se non trovato
        """
        params = Config.SCENARIOS['burst_cooldown']
        
        # Costruisci timeline per ogni item (item_id -> lista di conteggi giornalieri)
        item_timelines = defaultdict(list)
        item_dates = defaultdict(list)
        
        logger.info("Building item timelines...")
        for date in tqdm(self.dates, desc="Processing days"):
            day_data = self.index[date]
            top_items_dict = {item: count for item, count in day_data['top_100']}
            
            # Aggiungi conteggi per ogni item (0 se non presente)
            for item_id in item_timelines.keys():
                item_timelines[item_id].append(top_items_dict.get(item_id, 0))
                item_dates[item_id].append(date)
            
            # Aggiungi nuovi item
            for item_id in top_items_dict:
                if item_id not in item_timelines:
                    # Inizializza con zeri per i giorni precedenti
                    prev_days = len([d for d in self.dates if d < date])
                    item_timelines[item_id] = [0] * prev_days
                    item_dates[item_id] = [d for d in self.dates if d <= date]
                    item_timelines[item_id].append(top_items_dict[item_id])
        
        # Trova burst per ogni item
        bursts = []
        
        logger.info("Identifying burst patterns...")
        for item_id, timeline in tqdm(item_timelines.items(), desc="Analyzing items"):
            if len(timeline) < params['context_days'] * 2 + 1:
                continue
            
            # Calcola CV globale
            cv = calculate_coefficient_of_variation(timeline)
            if cv is None or cv < params['min_cv']:
                continue
            
            # Trova picchi (burst)
            mean_baseline = np.mean(timeline)
            std_baseline = np.std(timeline)
            threshold = mean_baseline + 2 * std_baseline  # 2 sigma
            
            for i in range(params['context_days'], len(timeline) - params['context_days']):
                peak_value = timeline[i]
                
                # Verifica che sia un picco significativo
                if peak_value < threshold:
                    continue
                
                # Calcola baseline (media dei giorni prima e dopo, escluso il picco)
                baseline_before = np.mean(timeline[max(0, i - params['context_days']):i])
                baseline_after = np.mean(timeline[i + 1:i + 1 + params['context_days']])
                baseline = (baseline_before + baseline_after) / 2.0
                
                if baseline == 0:
                    continue
                
                intensity = peak_value / baseline
                
                if intensity < params['min_intensity']:
                    continue
                
                # Calcola finestra burst (include context)
                start_idx = max(0, i - params['context_days'])
                end_idx = min(len(timeline), i + 1 + params['context_days'])
                
                burst_dates = item_dates[item_id][start_idx:end_idx]
                burst_values = timeline[start_idx:end_idx]
                
                # Costruisci timeline completa
                timeline_data = [
                    {'date': date, 'count': int(count)}
                    for date, count in zip(burst_dates, burst_values)
                ]
                
                bursts.append({
                    'item_id': item_id,
                    'burst_date': item_dates[item_id][i],
                    'start_date': burst_dates[0],
                    'end_date': burst_dates[-1],
                    'peak_value': int(peak_value),
                    'baseline': float(baseline),
                    'intensity': float(intensity),
                    'cv': float(cv),
                    'total_events': int(sum(burst_values)),
                    'duration_days': len(burst_dates),
                    'timeline': timeline_data,
                    'score': float(intensity * cv / 100.0)  # Normalizza score a [0, 1]
                })
        
        # Ordina per score e prendi il migliore
        if bursts:
            bursts.sort(key=lambda x: x['score'], reverse=True)
            best_burst = bursts[0]
            
            # Normalizza score
            best_burst['score'] = min(1.0, max(0.0, best_burst['score']))
            
            # Aggiungi selection reason
            best_burst['selection_reason'] = (
                f"Highest intensity burst ({best_burst['intensity']:.1f}x baseline) "
                f"with high variability (CV={best_burst['cv']:.2f}). "
                f"Peak on {best_burst['burst_date']} with {best_burst['peak_value']} events."
            )
            
            logger.info(f"[SUCCESS] Found {len(bursts)} burst patterns, selected best")
            logger.info(f"          Item {best_burst['item_id']}: intensity={best_burst['intensity']:.1f}x, "
                      f"CV={best_burst['cv']:.2f}, date={best_burst['burst_date']}")
            logger.info(f"          Reason: {best_burst['selection_reason']}")
            
            return best_burst
        else:
            logger.warning("[WARNING] No suitable burst patterns found")
            return None
    
    def find_scan_attack(self) -> Optional[Dict]:
        """
        Trova finestre temporali con pattern di scan attack.
        
        Criteri:
        - Alta percentuale di item unici (>80%)
        - Bassa percentuale di item ripetuti (<15%)
        - Finestra temporale compatta (1-3 giorni)
        
        Returns:
            dict: La migliore candidate window per scan attack, o None se non trovata
        """
        params = Config.SCENARIOS['scan_attack']
        
        candidates = []
        
        # Analizza finestre di 1-3 giorni consecutivi
        window_sizes = [1, 2, 3]
        
        logger.info("Scanning for attack patterns...")
        for window_size in window_sizes:
            for i in tqdm(range(len(self.dates) - window_size + 1), 
                         desc=f"Window size {window_size}"):
                window_dates = self.dates[i:i + window_size]
                
                # Raccogli tutti gli item nella finestra
                all_items = []
                total_events = 0
                
                for date in window_dates:
                    day_data = self.index[date]
                    total_events += day_data['total_events']
                    
                    # Aggiungi tutti gli item (non solo top-100)
                    # Usa top-100 come proxy (l'indice non ha tutti gli item)
                    for item_id, count in day_data['top_100']:
                        all_items.extend([item_id] * count)
                
                if total_events < Config.MIN_SCENARIO_EVENTS:
                    continue
                
                # Calcola metriche
                item_counter = Counter(all_items)
                unique_items = len(item_counter)
                total_item_accesses = len(all_items)
                
                if total_item_accesses == 0:
                    continue
                
                unique_rate = unique_items / total_item_accesses if total_item_accesses > 0 else 0
                
                # Item visti più di una volta
                repeated_items = sum(1 for count in item_counter.values() if count > 1)
                repeat_rate = repeated_items / unique_items if unique_items > 0 else 0
                
                # Verifica criteri
                if unique_rate < params['min_unique_rate']:
                    continue
                if repeat_rate > params['max_repeat_rate']:
                    continue
                
                # Calcola score (massimizza unique_rate, minimizza repeat_rate)
                score = unique_rate * 0.7 + (1 - repeat_rate) * 0.3
                
                candidates.append({
                    'start_date': window_dates[0],
                    'end_date': window_dates[-1],
                    'window_size_days': window_size,
                    'total_events': total_events,
                    'unique_items': unique_items,
                    'unique_rate': float(unique_rate),
                    'repeat_rate': float(repeat_rate),
                    'top_items': [{'item_id': item, 'count': count} 
                                 for item, count in item_counter.most_common(10)],
                    'score': float(score)
                })
        
        # Ordina per score e prendi il migliore
        if candidates:
            candidates.sort(key=lambda x: x['score'], reverse=True)
            best_candidate = candidates[0]
            
            # Normalizza score
            best_candidate['score'] = min(1.0, max(0.0, best_candidate['score']))
            
            # Rinomina campi per corrispondere alla struttura richiesta
            result = {
                'date_range': [best_candidate['start_date'], best_candidate['end_date']],
                'unique_rate': best_candidate['unique_rate'],
                'repeat_rate': best_candidate['repeat_rate'],
                'total_events': best_candidate['total_events'],
                'score': best_candidate['score'],
                'selection_reason': (
                    f"Highest unique rate ({best_candidate['unique_rate']:.1%}) "
                    f"with lowest repeat rate ({best_candidate['repeat_rate']:.1%}). "
                    f"Window: {best_candidate['start_date']} to {best_candidate['end_date']} "
                    f"({best_candidate['window_size_days']} days, {best_candidate['total_events']:,} events)."
                )
            }
            
            logger.info(f"[SUCCESS] Found {len(candidates)} scan attack candidates, selected best")
            logger.info(f"          {result['date_range'][0]} to {result['date_range'][1]}: "
                      f"unique_rate={result['unique_rate']:.2%}, "
                      f"repeat_rate={result['repeat_rate']:.2%}")
            logger.info(f"          Reason: {result['selection_reason']}")
            
            return result
        else:
            logger.warning("[WARNING] No suitable scan attack windows found")
            return None
    
    def find_daily_pattern(self) -> Optional[Dict]:
        """
        Trova pattern giornalieri con alta autocorrelazione.
        
        Criteri:
        - Autocorrelazione lag=1 giorno >= 0.6
        - Almeno 3 cicli completi
        - Pattern stabile nel tempo
        
        Returns:
            dict: Dizionario con informazioni sul pattern giornaliero trovato
        """
        params = Config.SCENARIOS['daily_pattern']
        
        # Costruisci serie temporale di eventi giornalieri
        daily_events = []
        for date in self.dates:
            daily_events.append(self.index[date]['total_events'])
        
        if len(daily_events) < params['num_cycles'] * 2:  # Almeno 2 giorni per ciclo
            logger.warning("[WARNING] Insufficient data for daily pattern analysis")
            return None
        
        # Calcola autocorrelazione con lag=1 giorno
        autocorr = calculate_autocorrelation(daily_events, lag=1)
        
        if autocorr is None or autocorr < params['min_autocorr']:
            logger.warning(f"[WARNING] Low autocorrelation ({autocorr:.3f} < {params['min_autocorr']})")
            return None
        
        # Verifica pattern stabile (bassa varianza nella serie)
        cv = calculate_coefficient_of_variation(daily_events)
        
        # Trova finestra con pattern più stabile
        window_size = params['num_cycles'] * 2  # 2 giorni per ciclo
        best_window = None
        best_stability = float('inf')
        
        for i in range(len(daily_events) - window_size + 1):
            window_events = daily_events[i:i + window_size]
            window_cv = calculate_coefficient_of_variation(window_events)
            
            if window_cv is not None and window_cv < best_stability:
                best_stability = window_cv
                best_window = {
                    'start_date': self.dates[i],
                    'end_date': self.dates[i + window_size - 1],
                    'start_idx': i,
                    'end_idx': i + window_size - 1
                }
        
        if best_window is None:
            logger.warning("[WARNING] No stable daily pattern window found")
            return None
        
        # Estrai pattern orario medio (se disponibile)
        hourly_pattern = None
        if 'hourly_dist' in self.index[self.dates[0]]:
            hourly_counts = defaultdict(int)
            window_dates = self.dates[best_window['start_idx']:best_window['end_idx'] + 1]
            
            for date in window_dates:
                hourly_dist = self.index[date].get('hourly_dist', {})
                for hour, count in hourly_dist.items():
                    hourly_counts[int(hour)] += count
            
            # Normalizza
            total_hourly = sum(hourly_counts.values())
            if total_hourly > 0:
                hourly_pattern = {
                    hour: count / total_hourly
                    for hour, count in sorted(hourly_counts.items())
                }
        
        # Estrai date dei cicli (ogni ciclo = period_days)
        period_days = window_size // params['num_cycles']
        cycle_dates = []
        for cycle_idx in range(params['num_cycles']):
            cycle_start_idx = best_window['start_idx'] + (cycle_idx * period_days)
            cycle_date = self.dates[cycle_start_idx]
            cycle_dates.append(cycle_date)
        
        # Calcola score normalizzato
        score = float(autocorr * (1.0 / (best_stability + 0.1)))
        score = min(1.0, max(0.0, score))
        
        result = {
            'cycle_dates': cycle_dates,
            'period_days': period_days,
            'autocorrelation': float(autocorr),
            'score': score,
            'selection_reason': (
                f"Strong daily pattern with autocorrelation {autocorr:.3f} "
                f"(threshold: {params['min_autocorr']}). "
                f"Found {params['num_cycles']} cycles with period {period_days} days. "
                f"Window stability: {best_stability:.3f}."
            )
        }
        
        logger.info(f"[SUCCESS] Found daily pattern: {result['cycle_dates'][0]} to {result['cycle_dates'][-1]}")
        logger.info(f"          Autocorrelation: {result['autocorrelation']:.3f}, "
                  f"Period: {result['period_days']} days")
        logger.info(f"          Reason: {result['selection_reason']}")
        
        return result
    
    def find_multi_modal(self) -> Optional[Dict]:
        """
        Trova distribuzione multi-modale usando clustering su features temporali.
        
        Criteri:
        - Almeno 3-5 cluster distinti
        - Silhouette score >= 0.5
        - Cluster ben separati
        
        Returns:
            dict: Dizionario con informazioni sulla distribuzione multi-modale
        """
        params = Config.SCENARIOS['multi_modal']
        
        try:
            from sklearn.cluster import KMeans
            from sklearn.metrics import silhouette_score
            from sklearn.preprocessing import StandardScaler
        except ImportError:
            logger.warning("[WARNING] sklearn not available, skipping multi-modal analysis")
            return None
        
        # Costruisci features per ogni giorno
        features = []
        feature_dates = []
        
        for date in self.dates:
            day_data = self.index[date]
            
            # Estrai features disponibili
            feature_vector = []
            
            # Feature 1: Total events (normalizzato)
            feature_vector.append(day_data['total_events'])
            
            # Feature 2: Unique items ratio
            if day_data['total_events'] > 0:
                unique_ratio = day_data['unique_items'] / day_data['total_events']
            else:
                unique_ratio = 0
            feature_vector.append(unique_ratio)
            
            # Feature 3: Zipf alpha (se disponibile)
            zipf_alpha = day_data['statistics'].get('zipf_alpha')
            if zipf_alpha is not None:
                feature_vector.append(zipf_alpha)
            else:
                feature_vector.append(0.0)
            
            # Feature 4: Gini coefficient (se disponibile)
            gini = day_data['statistics'].get('gini_coefficient')
            if gini is not None:
                feature_vector.append(gini)
            else:
                feature_vector.append(0.0)
            
            # Feature 5: Repeat rate (se disponibile)
            repeat_rate = day_data['statistics'].get('repeat_rate')
            if repeat_rate is not None:
                feature_vector.append(repeat_rate)
            else:
                feature_vector.append(0.0)
            
            features.append(feature_vector)
            feature_dates.append(date)
        
        if len(features) < params['max_clusters']:
            logger.warning(f"[WARNING] Insufficient data points ({len(features)}) for {params['max_clusters']} clusters")
            return None
        
        # Normalizza features
        scaler = StandardScaler()
        features_scaled = scaler.fit_transform(features)
        
        # Prova diversi numeri di cluster
        best_result = None
        best_score = -1.0
        
        for n_clusters in range(params['min_clusters'], params['max_clusters'] + 1):
            if n_clusters >= len(features):
                continue
            
            kmeans = KMeans(n_clusters=n_clusters, random_state=Config.RANDOM_SEED, n_init=10)
            labels = kmeans.fit_predict(features_scaled)
            
            # Calcola silhouette score
            if len(set(labels)) < 2:  # Serve almeno 2 cluster per silhouette
                continue
            
            silhouette = silhouette_score(features_scaled, labels)
            
            if silhouette < params['min_silhouette']:
                continue
            
            # Calcola score combinato
            score = silhouette * 0.7 + (n_clusters / params['max_clusters']) * 0.3
            
            if score > best_score:
                best_score = score
                
                # Raggruppa date per cluster
                cluster_dates = defaultdict(list)
                for date, label in zip(feature_dates, labels):
                    cluster_dates[int(label)].append(date)
                
                best_result = {
                    'n_clusters': n_clusters,
                    'silhouette_score': float(silhouette),
                    'score': float(score),
                    'clusters': {
                        str(cluster_id): {
                            'dates': cluster_dates[cluster_id],
                            'size': len(cluster_dates[cluster_id]),
                            'centroid': kmeans.cluster_centers_[cluster_id].tolist()
                        }
                        for cluster_id in range(n_clusters)
                    },
                    'cluster_labels': labels.tolist(),
                    'dates': feature_dates
                }
        
        if best_result:
            # Normalizza score
            best_result['score'] = min(1.0, max(0.0, best_result['score']))
            
            # Estrai date range dalla finestra migliore (usa tutti i giorni)
            best_result['date_range'] = [
                self.dates[0],
                self.dates[-1]
            ]
            
            # Rinomina campi per corrispondere alla struttura richiesta
            result = {
                'date_range': best_result['date_range'],
                'num_clusters': best_result['n_clusters'],
                'silhouette_score': best_result['silhouette_score'],
                'score': best_result['score'],
                'selection_reason': (
                    f"Multi-modal distribution with {best_result['n_clusters']} distinct clusters "
                    f"and silhouette score {best_result['silhouette_score']:.3f} "
                    f"(threshold: {params['min_silhouette']}). "
                    f"Clusters are well-separated, indicating distinct access patterns."
                ),
                'clusters': best_result['clusters']  # Mantieni dettagli per analisi
            }
            
            logger.info(f"[SUCCESS] Found multi-modal distribution with {result['num_clusters']} clusters")
            logger.info(f"          Silhouette score: {result['silhouette_score']:.3f}")
            logger.info(f"          Reason: {result['selection_reason']}")
            for cluster_id, cluster_info in result['clusters'].items():
                logger.info(f"          Cluster {cluster_id}: {cluster_info['size']} days")
            
            return result
        else:
            logger.warning("[WARNING] No suitable multi-modal distribution found")
            return None


def main():
    """
    Entry point per lo script di scenario finding.
    """
    parser = argparse.ArgumentParser(
        description='Find candidate scenarios in Tianchi dataset index',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python 02_find_scenarios.py --index data/tianchi_index.json
  python 02_find_scenarios.py --index data/tianchi_index.json --output custom_scenarios.json
        """
    )
    parser.add_argument(
        '--index',
        type=str,
        default=None,
        help='Path to tianchi_index.json (default: from Config)'
    )
    parser.add_argument(
        '--output',
        type=str,
        default=None,
        help='Output path for found scenarios (default: scenarios_found.json in data dir)'
    )
    
    args = parser.parse_args()
    
    # Set default paths
    if args.index is None:
        index_path = Config.TIANCHI_INDEX
    else:
        index_path = Path(args.index)
    
    if args.output is None:
        output_path = Config.DATA_DIR / 'scenarios_found.json'
    else:
        output_path = Path(args.output)
    
    # Convert to absolute path for clarity
    output_path = output_path.resolve()
    
    # Validate input
    if not index_path.exists():
        logger.error(f"Index file not found: {index_path}")
        logger.error("Please run 01_build_index.py first to create the index")
        return 1
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Run scenario finding
    try:
        finder = ScenarioFinder(index_path)
        results = finder.find_all_scenarios()
        
        # Save results
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info("\n" + "="*60)
        logger.info(f"Results saved to: {output_path}")
        logger.info("="*60)
        
        # Print summary
        logger.info("\nSummary:")
        logger.info(f"  Hot-Cold Shift: {'Found' if results['scenarios']['hot_cold_shift'] else 'Not found'}")
        logger.info(f"  Burst Patterns: {'Found' if results['scenarios']['burst_cooldown'] else 'Not found'}")
        logger.info(f"  Scan Attacks: {'Found' if results['scenarios']['scan_attack'] else 'Not found'}")
        logger.info(f"  Daily Pattern: {'Found' if results['scenarios']['daily_pattern'] else 'Not found'}")
        logger.info(f"  Multi-Modal: {'Found' if results['scenarios']['multi_modal'] else 'Not found'}")
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Error finding scenarios: {e}", exc_info=True)
        return 1


if __name__ == '__main__':
    exit(main())

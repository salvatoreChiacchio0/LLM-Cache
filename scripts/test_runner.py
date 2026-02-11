"""
Test Runner: Esegue test automatici su tutti i dataset.
Per ogni dataset:
1. Avvia streamer+brain con il dataset specifico
2. Attende che processi 100k eventi
3. Raccoglie metriche da Prometheus
4. Salva risultati dettagliati
5. Passa al prossimo dataset

Questo script viene eseguito come servizio Docker e parte automaticamente con docker-compose.
"""
import os
import sys
import json
import time
import requests
import subprocess
import signal
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Try to import docker client (may not be available in container)
try:
    import docker
    DOCKER_AVAILABLE = True
except ImportError:
    DOCKER_AVAILABLE = False

# Aggiungi il path del progetto
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.modules.preprocessed_loader import PreprocessedLoader
from scripts.test_config_manager import TestConfigManager
from typing import Optional


class PrometheusMetricsCollector:
    """Raccoglie metriche da Prometheus API."""
    
    def __init__(self, prometheus_url: str = "http://prometheus:9090", streamer_url: str = "http://streamer:8000"):
        self.prometheus_url = prometheus_url
        self.streamer_url = streamer_url
        self.base_url = f"{prometheus_url}/api/v1"
    
    def query(self, query: str) -> Optional[float]:
        """Esegue una query Prometheus e ritorna il valore."""
        try:
            # Usa query_range per ottenere l'ultimo valore disponibile
            response = requests.get(
                f"{self.base_url}/query",
                params={"query": query},
                timeout=5
            )
            response.raise_for_status()
            data = response.json()
            
            if data["status"] == "success" and data["data"]["result"]:
                value = data["data"]["result"][0]["value"][1]
                return float(value)
            return None
        except Exception as e:
            print(f"Error querying Prometheus: {e}")
            return None
    
    def query_counter_total(self, metric_name: str) -> Optional[float]:
        """Ottiene il totale di un counter usando sum() per aggregare su tutte le istanze."""
        try:
            # Query per sommare il counter su tutte le istanze
            query = f"sum({metric_name})"
            return self.query(query)
        except Exception as e:
            print(f"Error querying counter total for {metric_name}: {e}")
            return None
    
    def get_metric_value(self, metric_name: str) -> Optional[float]:
        """Ottiene il valore corrente di una metrica."""
        return self.query(metric_name)
    
    def get_counter_total(self, counter_name: str) -> Optional[float]:
        """Ottiene il totale di un counter."""
        # Prova prima con sum() per aggregare, poi fallback a query diretta
        result = self.query_counter_total(counter_name)
        if result is None:
            result = self.query(counter_name)
        return result
    
    def _count_llm_calls_from_temporal(self) -> int:
        """Count LLM calls from the brain's temporal metrics file."""
        try:
            temporal_file = Path("data/temporal_metrics/llm_calls_temporal.json")
            if temporal_file.exists():
                with open(temporal_file, 'r') as f:
                    temporal_data = json.load(f)
                    if isinstance(temporal_data, list):
                        return len(temporal_data)
        except Exception:
            pass
        return 0

    def collect_from_streamer_directly(self) -> Optional[Dict]:
        """Prova a leggere le metriche direttamente dallo streamer."""
        # Prima prova a leggere dal file di metriche finali
        try:
            metrics_file = Path("data/streamer_final_metrics.json")
            if metrics_file.exists():
                with open(metrics_file, 'r') as f:
                    file_metrics = json.load(f)
                    llm_calls = self._count_llm_calls_from_temporal()
                    metrics = {
                        "cache_hits": file_metrics.get("aura_hits", 0),
                        "cache_misses": file_metrics.get("aura_misses", 0),
                        "tinylfu_hits": file_metrics.get("baseline_hits", 0),
                        "tinylfu_misses": file_metrics.get("baseline_misses", 0),
                        "llm_calls_total": llm_calls,
                        "llm_errors_total": file_metrics.get("llm_errors", 0),
                        "redis_memory_usage_bytes": file_metrics.get("redis_memory_bytes", 0),
                    }
                    return metrics
        except Exception as e:
            pass
        
        # Fallback: prova a leggere dall'endpoint HTTP
        try:
            response = requests.get(f"{self.streamer_url}/metrics", timeout=2)
            if response.status_code == 200:
                metrics_text = response.text
                metrics = {}
                
                # Parse Prometheus format
                for line in metrics_text.split('\n'):
                    if line.startswith('#') or not line.strip():
                        continue
                    parts = line.split()
                    if len(parts) >= 2:
                        metric_name = parts[0]
                        metric_value = parts[1]
                        
                        if metric_name in ["cache_hits", "cache_misses", "tinylfu_hits", "tinylfu_misses",
                                          "llm_calls_total", "llm_errors_total",
                                          "tinylfu_admissions_total", "tinylfu_rejections_total",
                                          "tinylfu_resets_total", "tinylfu_decay_applications_total",
                                          "tinylfu_reset_interval_updates_total",
                                          "redis_memory_usage_bytes"]:
                            try:
                                metrics[metric_name] = float(metric_value)
                            except ValueError:
                                pass
                
                return metrics if metrics else None
        except Exception as e:
            # Streamer non disponibile o errore
            return None
        return None
    
    def collect_all_metrics(self) -> Dict:
        """Raccoglie tutte le metriche rilevanti."""
        # Prova prima a leggere direttamente dallo streamer
        streamer_metrics = self.collect_from_streamer_directly()
        
        metrics = {}
        
        if streamer_metrics:
            # Usa metriche dirette dallo streamer
            metrics["cache_hits"] = streamer_metrics.get("cache_hits", 0)
            metrics["cache_misses"] = streamer_metrics.get("cache_misses", 0)
            metrics["tinylfu_hits"] = streamer_metrics.get("tinylfu_hits", 0)
            metrics["tinylfu_misses"] = streamer_metrics.get("tinylfu_misses", 0)
            metrics["llm_calls"] = streamer_metrics.get("llm_calls_total", 0)
            metrics["llm_errors"] = streamer_metrics.get("llm_errors_total", 0)
            metrics["tinylfu_admissions"] = streamer_metrics.get("tinylfu_admissions_total", 0)
            metrics["tinylfu_rejections"] = streamer_metrics.get("tinylfu_rejections_total", 0)
            metrics["tinylfu_resets"] = streamer_metrics.get("tinylfu_resets_total", 0)
            metrics["tinylfu_decay_applications"] = streamer_metrics.get("tinylfu_decay_applications_total", 0)
            metrics["tinylfu_reset_interval_updates"] = streamer_metrics.get("tinylfu_reset_interval_updates_total", 0)
        else:
            # Fallback a Prometheus
            metrics["cache_hits"] = self.get_counter_total("cache_hits") or 0
            metrics["cache_misses"] = self.get_counter_total("cache_misses") or 0
            metrics["tinylfu_hits"] = self.get_counter_total("tinylfu_hits") or 0
            metrics["tinylfu_misses"] = self.get_counter_total("tinylfu_misses") or 0
            metrics["llm_calls"] = self.get_counter_total("llm_calls_total") or 0
            metrics["llm_errors"] = self.get_counter_total("llm_errors_total") or 0
            metrics["tinylfu_admissions"] = self.get_counter_total("tinylfu_admissions_total") or 0
            metrics["tinylfu_rejections"] = self.get_counter_total("tinylfu_rejections_total") or 0
            metrics["tinylfu_resets"] = self.get_counter_total("tinylfu_resets_total") or 0
            metrics["tinylfu_decay_applications"] = self.get_counter_total("tinylfu_decay_applications_total") or 0
            metrics["tinylfu_reset_interval_updates"] = self.get_counter_total("tinylfu_reset_interval_updates_total") or 0
        
        # Hit rates
        aura_total = metrics["cache_hits"] + metrics["cache_misses"]
        baseline_total = metrics["tinylfu_hits"] + metrics["tinylfu_misses"]
        
        metrics["aura_hit_rate"] = metrics["cache_hits"] / aura_total if aura_total > 0 else 0.0
        metrics["baseline_hit_rate"] = metrics["tinylfu_hits"] / baseline_total if baseline_total > 0 else 0.0
        metrics["improvement"] = metrics["aura_hit_rate"] - metrics["baseline_hit_rate"]
        metrics["improvement_percent"] = (metrics["improvement"] / metrics["baseline_hit_rate"] * 100) if metrics["baseline_hit_rate"] > 0 else 0.0
        
        # LLM metrics
        metrics["llm_calls"] = self.get_counter_total("llm_calls_total") or 0
        metrics["llm_errors"] = self.get_counter_total("llm_errors_total") or 0
        metrics["llm_latency_ms"] = self.get_metric_value("llm_reasoning_latency_ms") or 0.0
        
        # TinyLFU metrics
        metrics["tinylfu_admissions"] = self.get_counter_total("tinylfu_admissions_total") or 0
        metrics["tinylfu_rejections"] = self.get_counter_total("tinylfu_rejections_total") or 0
        metrics["tinylfu_resets"] = self.get_counter_total("tinylfu_resets_total") or 0
        metrics["tinylfu_decay_applications"] = self.get_counter_total("tinylfu_decay_applications_total") or 0
        metrics["tinylfu_reset_interval_updates"] = self.get_counter_total("tinylfu_reset_interval_updates_total") or 0
        
        
        # Redis memory
        metrics["redis_memory_bytes"] = self.get_metric_value("redis_memory_usage_bytes") or 0.0
        
        return metrics


class TestRunner:
    """Esegue test automatici su tutti i dataset."""
    
    def __init__(self, max_events: int = 1000000):
        self.max_events = max_events
        self.results_dir = project_root / "data" / "test_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.metrics_collector = PrometheusMetricsCollector()
        self.config_manager = TestConfigManager()
        
        print(f"Initialized with max_events={max_events}")
        print(f"Results will be saved to: {self.results_dir}")
    
    def wait_for_services(self, timeout: int = 300):
        """Attende che i servizi siano pronti."""
        print("Waiting for services to be ready...")
        
        services = {
            "Prometheus": "http://prometheus:9090/-/ready",
            "Streamer": "http://streamer:8000/metrics",
            "Kafka": "http://kafka:9092",  # Non ha endpoint HTTP, ma proviamo
        }
        
        for service_name, url in services.items():
            start_time = time.time()
            while time.time() - start_time < timeout:
                try:
                    if "kafka" in url.lower():
                        # Kafka non ha endpoint HTTP, skip
                        break
                    response = requests.get(url, timeout=2)
                    if response.status_code == 200:
                        print(f"{service_name} is ready")
                        break
                except:
                    pass
                time.sleep(2)
            else:
                print(f"WARNING: {service_name} not ready after {timeout}s")
    
    def run_test(self, test_name: str, dataset_path: str, metadata: Optional[Dict] = None, next_test_config: Optional[Dict] = None) -> Dict:
        """
        Esegue un singolo test.
        
        Args:
            test_name: Nome del test
            dataset_path: Path al dataset da testare
            metadata: Metadata aggiuntive
            next_test_config: Configurazione del prossimo test (se esiste) - usata per scrivere il file di config quando lo streamer è vicino al completamento
        
        Returns:
            Dizionario con risultati completi
        """
        print(f"\n{'='*80}")
        print(f"Starting: {test_name}")
        print(f"Dataset: {dataset_path}")
        print(f"{'='*80}")
        
        self.reset_temporal_metrics()
        
        # Reset completo Redis (già fatto in run_all_tests, ma lo facciamo anche qui per sicurezza)
        try:
            import redis
            r_aura = redis.Redis(host="redis-aura", port=6379, decode_responses=True)
            r_baseline = redis.Redis(host="redis-lru", port=6380, decode_responses=True)
            r_aura.flushdb()
            r_baseline.flushdb()
            
            # Verifica che siano vuoti
            aura_keys = r_aura.dbsize()
            baseline_keys = r_baseline.dbsize()
            print(f"Flushed Redis databases (aura keys: {aura_keys}, baseline keys: {baseline_keys})")
        except Exception as e:
            print(f"WARNING: Failed to flush Redis: {e}")
        
        # IMPORTANTE: NON scrivere il file di configurazione qui!
        # Lo streamer deve completare il test corrente prima di leggere il nuovo file.
        # Il file verrà scritto solo dopo che lo streamer ha completato il test precedente.
        # Per il primo test, scrivi il file immediatamente.
        # Per i test successivi, il file verrà scritto dopo che lo streamer ha completato il test precedente.
        
        # Controlla se questo è il primo test (controllando se il file di configurazione esiste già)
        config_file = Path("data/current_test.json")
        is_first_test = not config_file.exists()
        
        if is_first_test:
            # Primo test: scrivi il file di configurazione immediatamente
            print("First test detected, writing config file...")
            self.config_manager.set_current_test(test_name, dataset_path, self.max_events, metadata)
            
            # Riavvia streamer per applicare la nuova configurazione
            print("Restarting streamer to apply new test configuration...")
            try:
                # Try to restart streamer using Docker API
                try:
                    import docker
                    client = docker.from_env()
                    container = client.containers.get("streamer")
                    container.restart(timeout=30)
                    print("Streamer restarted via Docker API, waiting for it to be ready...")
                except Exception as e:
                    print(f"Could not restart streamer via Docker API ({e}), streamer should read config file on next check...")
                time.sleep(10)
            except Exception as e:
                print(f"WARNING: Failed to restart streamer: {e}")
                print("Continuing anyway, assuming streamer will read config file...")
                time.sleep(5)
        else:
            # Test successivi: Il file di configurazione è già stato scritto quando il test precedente era all'80%
            # Ora dobbiamo aspettare che lo streamer:
            # 1. Completare il test precedente
            # 2. Leggere il nuovo file di configurazione
            # 3. Iniziare a processare gli eventi del nuovo test
            print(f"Config file for {test_name} should already be written. Waiting for streamer to start processing this test...")
            
            # Aspetta che lo streamer inizi a processare il nuovo test verificando che le metriche siano per questo test
            max_wait_for_test_start = 60  # Attendi max 60 secondi per l'inizio del test
            wait_start_time = time.time()
            test_started = False
            
            while not test_started and (time.time() - wait_start_time) < max_wait_for_test_start:
                metrics_file = Path("data/streamer_final_metrics.json")
                if metrics_file.exists():
                    try:
                        with open(metrics_file, 'r') as f:
                            file_data = json.load(f)
                            file_test_name = file_data.get("test_name", "")
                            file_events = file_data.get("events_processed", 0)
                            
                            # Se il file è per questo test E ha processato almeno qualche evento, il test è iniziato
                            if file_test_name == test_name and file_events > 0:
                                print(f"[TEST] Test {test_name} started! Streamer has processed {file_events} events.")
                                test_started = True
                                break
                            elif file_test_name != test_name and file_events >= self.max_events:
                                # Il test precedente è completato, ma lo streamer non ha ancora iniziato questo test
                                print(f"[TEST] Previous test ({file_test_name}) completed. Waiting for streamer to start {test_name}...")
                    except Exception as e:
                        pass
                
                time.sleep(2)  # Controlla ogni 2 secondi
            
            if not test_started:
                print(f"WARNING: Test {test_name} may not have started after {max_wait_for_test_start}s. Continuing anyway...")
        
        # Reset metriche Prometheus (non possibile direttamente, ma possiamo tracciare i valori iniziali)
        initial_metrics = self.metrics_collector.collect_all_metrics()
        
        # Nota: streamer dovrebbe già essere in esecuzione e leggerà il file di config
        # Se streamer non è in esecuzione o non supporta il file di config,
        # dobbiamo riavviarlo con variabili d'ambiente
        
        # Attendi che gli eventi vengano processati
        print(f"Waiting for {self.max_events} events to be processed...")
        print(f"next_test_config available: {next_test_config is not None}")
        if next_test_config:
            print(f"Next test will be: {next_test_config.get('name')}")
        start_time = time.time()
        
        # Monitora metriche fino a raggiungere max_events
        # IMPORTANTE: Leggiamo le metriche dal file JSON salvato dallo streamer, non da Prometheus
        # perché Prometheus accumula metriche tra tutti i test
        last_total_events = 0
        stall_count = 0
        max_stall = 30  # 30 controlli senza progresso = timeout
        
        # Leggi le metriche finali PRIMA che lo streamer termini
        # Monitora lo streamer e leggi le metriche mentre è ancora in esecuzione
        final_metrics_read = False
        last_streamer_status = None
        
        print(f"Starting monitoring loop for test {test_name}...")
        while True:
            time.sleep(2)  # Controlla ogni 2 secondi (più frequente per catturare le metriche)
            
            # Verifica se lo streamer è ancora in esecuzione
            streamer_running = False
            try:
                if DOCKER_AVAILABLE:
                    import docker
                    client = docker.from_env()
                    container = client.containers.get("streamer")
                    container.reload()
                    streamer_running = container.status == "running"
                else:
                    # Fallback: verifica se l'endpoint HTTP dello streamer risponde
                    try:
                        response = requests.get("http://streamer:8000/metrics", timeout=2)
                        streamer_running = response.status_code == 200
                    except:
                        # Se l'endpoint non risponde, controlla se il file di metriche finali è stato scritto di recente
                        metrics_file = Path("data/streamer_final_metrics.json")
                        if metrics_file.exists():
                            # Se il file esiste ma è stato scritto più di 30 secondi fa, probabilmente lo streamer è terminato
                            file_mtime = metrics_file.stat().st_mtime
                            if time.time() - file_mtime < 30:
                                # File scritto di recente, lo streamer potrebbe essere ancora in esecuzione
                                # Ma controlliamo anche se contiene il numero corretto di eventi
                                try:
                                    with open(metrics_file, 'r') as f:
                                        file_data = json.load(f)
                                        file_test_name = file_data.get("test_name", "")
                                        file_events = file_data.get("events_processed", 0)
                                        # Se il file contiene meno eventi del target E è per il test corrente, lo streamer è ancora in esecuzione
                                        if file_test_name == test_name and file_events < self.max_events:
                                            streamer_running = True
                                        elif file_test_name != test_name:
                                            # Il file è per un altro test, lo streamer potrebbe essere ancora in esecuzione per questo test
                                            streamer_running = True
                                except:
                                    pass
            except Exception as e:
                # Se non possiamo verificare lo stato, assumiamo che lo streamer sia ancora in esecuzione
                # e monitoriamo le metriche direttamente
                streamer_running = True
            
            # Se lo streamer è ancora in esecuzione (o non possiamo verificarlo), leggi le metriche
            if streamer_running:
                # IMPORTANTE: Usa il file JSON come fonte primaria perché viene aggiornato periodicamente dallo streamer
                # e contiene il numero esatto di eventi processati per il test corrente
                metrics_file = Path("data/streamer_final_metrics.json")
                total_events = 0
                
                if metrics_file.exists():
                    try:
                        with open(metrics_file, 'r') as f:
                            file_data = json.load(f)
                            file_test_name = file_data.get("test_name", "")
                            file_events = file_data.get("events_processed", 0)
                            # Usa il file solo se è per il test corrente
                            if file_test_name == test_name:
                                total_events = file_events
                            else:
                                # Se il file è per un altro test, NON usarlo e usa Prometheus come fallback
                                # Ma anche Prometheus potrebbe avere metriche accumulate, quindi meglio aspettare
                                # che lo streamer processi gli eventi del test corrente
                                if file_test_name:
                                    # Il file esiste ma è per un altro test - lo streamer deve ancora processare questo test
                                    total_events = 0  # Reset per forzare l'attesa
                                else:
                                    # File senza test_name, usa Prometheus come fallback
                                    current_metrics = self.metrics_collector.collect_all_metrics()
                                    total_events = (
                                        current_metrics["cache_hits"] + current_metrics["cache_misses"] +
                                        current_metrics["tinylfu_hits"] + current_metrics["tinylfu_misses"]
                                    ) / 2
                    except Exception as e:
                        # Fallback: usa Prometheus se il file non è disponibile
                        current_metrics = self.metrics_collector.collect_all_metrics()
                        total_events = (
                            current_metrics["cache_hits"] + current_metrics["cache_misses"] +
                            current_metrics["tinylfu_hits"] + current_metrics["tinylfu_misses"]
                        ) / 2
                else:
                    # Fallback: usa Prometheus se il file non esiste ancora
                    current_metrics = self.metrics_collector.collect_all_metrics()
                    total_events = (
                        current_metrics["cache_hits"] + current_metrics["cache_misses"] +
                        current_metrics["tinylfu_hits"] + current_metrics["tinylfu_misses"]
                    ) / 2
                
                # Log progresso ogni 10% o ogni 5 secondi quando è vicino al 95%
                progress_pct = (total_events / self.max_events * 100) if self.max_events > 0 else 0
                if not hasattr(self, '_last_progress_log') or time.time() - self._last_progress_log > 5:
                    if progress_pct >= 80 or total_events % 1000 == 0:  # Log ogni 1000 eventi o quando è sopra l'80%
                        print(f"Progress: {total_events:.0f}/{self.max_events} ({progress_pct:.1f}%)")
                        self._last_progress_log = time.time()
                
                # Debug: mostra sempre quando abbiamo next_test_config
                if next_test_config:
                    progress_pct = (total_events / self.max_events * 100) if self.max_events > 0 else 0
                    if progress_pct >= 90:
                        # Log dettagliato quando siamo vicini al 95%
                        if not hasattr(self, '_last_95_log') or time.time() - self._last_95_log > 2:
                            print(f"Progress: {total_events:.0f}/{self.max_events} ({progress_pct:.1f}%) - next_test_config available: {next_test_config.get('name')}")
                            self._last_95_log = time.time()
                
                # IMPORTANTE: Scrivi il file di configurazione per il prossimo test quando lo streamer è al 95%+
                # Questo garantisce che il file sia disponibile quando lo streamer completa il test corrente
                # ma non troppo presto per evitare che lo streamer si fermi prima di completare il test corrente
                if next_test_config and total_events >= self.max_events * 0.95:
                    # Scrivi il file di configurazione solo una volta quando raggiunge il 95%
                    if not hasattr(self, '_config_written_for_next_test'):
                        self._config_written_for_next_test = set()
                    next_test_name = next_test_config.get('name')
                    if next_test_name and next_test_name not in self._config_written_for_next_test:
                        next_test_file = next_test_config.get('test_file')
                        next_metadata = next_test_config.get('metadata', {})
                        print(f"Streamer near completion ({total_events:.0f}/{self.max_events}, {total_events/self.max_events*100:.1f}%), writing config file for NEXT test: {next_test_name}...")
                        print(f"Next test file: {next_test_file}")
                        self.config_manager.set_current_test(next_test_name, str(next_test_file), self.max_events, next_metadata)
                        self._config_written_for_next_test.add(next_test_name)
                        print(f"Config file written for {next_test_name}, streamer will pick it up when current test completes")
                        time.sleep(1)  # Breve pausa per permettere allo streamer di leggere il file
                elif not next_test_config:
                    # Debug: mostra se non abbiamo next_test_config
                    if not hasattr(self, '_no_next_test_logged'):
                        print(f"WARNING: No next_test_config available for test {test_name}")
                        self._no_next_test_logged = True
                
                if total_events >= self.max_events:
                    print(f"Processed {total_events:.0f} events (target: {self.max_events})")
                    # Lo streamer ha completato il test corrente
                    # Il file di configurazione per questo test è già stato scritto da run_all_tests()
                    # Lo streamer dovrebbe averlo già letto e dovrebbe essere passato al prossimo test
                    # Leggi le metriche finali PRIMA che lo streamer termini
                    final_metrics = self.metrics_collector.collect_all_metrics()
                    final_metrics_read = True
                    break
                
                if total_events > last_total_events:
                    print(f"Progress: {total_events:.0f}/{self.max_events} events ({total_events/self.max_events*100:.1f}%)")
                    last_total_events = total_events
                    stall_count = 0
                else:
                    stall_count += 1
                    if stall_count >= max_stall:
                        print(f"WARNING: No progress for {max_stall*2}s, stopping...")
                        # Leggi le metriche finali prima di uscire
                        final_metrics = self.metrics_collector.collect_all_metrics()
                        final_metrics_read = True
                        break
            else:
                # Streamer potrebbe essere terminato - verifica leggendo le metriche dal file JSON
                if not final_metrics_read:
                    # Prima prova a leggere dal file JSON salvato dallo streamer
                    metrics_file = Path("data/streamer_final_metrics.json")
                    if metrics_file.exists():
                        try:
                            # Aspetta un po' per assicurarsi che il file sia stato scritto completamente
                            time.sleep(5)
                            with open(metrics_file, 'r') as f:
                                file_data = json.load(f)
                                file_test_name = file_data.get("test_name", "")
                                file_events = file_data.get("events_processed", 0)
                                
                                # IMPORTANTE: Verifica che il file sia per il test corrente
                                # Se il file è per un altro test, NON usarlo e continua a monitorare
                                if file_test_name != test_name:
                                    print(f"WARNING: Metrics file is for test '{file_test_name}', but current test is '{test_name}'. Waiting for streamer to process current test...")
                                    # Continua il loop per aspettare che lo streamer processi il test corrente
                                    continue
                                
                                # Se il file contiene il numero corretto di eventi E è per il test corrente, usa quelle metriche
                                if file_events >= self.max_events * 0.95:  # Almeno 95% del target
                                    print(f"Streamer completed, reading final metrics from JSON file ({file_events:.0f} events) for test '{test_name}'...")
                                    final_metrics = {
                                        "cache_hits": file_data.get("aura_hits", 0),
                                        "cache_misses": file_data.get("aura_misses", 0),
                                        "tinylfu_hits": file_data.get("baseline_hits", 0),
                                        "tinylfu_misses": file_data.get("baseline_misses", 0),
                                    }
                                    total_events = file_events
                                    print(f"Final metrics read from file: {total_events:.0f} events")
                                    final_metrics_read = True
                                    break
                        except Exception as e:
                            print(f"WARNING: Failed to read metrics from file: {e}")
                    
                    # Fallback: prova a leggere da Prometheus
                    print("Streamer terminated, reading final metrics from Prometheus...")
                    time.sleep(10)  # Aspetta un po' per permettere a Prometheus di scrapare le metriche finali
                    final_metrics = self.metrics_collector.collect_all_metrics()
                    total_events = (
                        final_metrics["cache_hits"] + final_metrics["cache_misses"] +
                        final_metrics["tinylfu_hits"] + final_metrics["tinylfu_misses"]
                    ) / 2
                    print(f"Final metrics read from Prometheus: {total_events:.0f} events")
                    
                    # Se abbiamo ancora 0 eventi, potrebbe essere che lo streamer stia ancora processando
                    # In questo caso, continua a monitorare per un po' di più
                    if total_events == 0:
                        print("WARNING: No events detected, waiting a bit more for streamer to finish...")
                        time.sleep(30)  # Aspetta altri 30 secondi
                        final_metrics = self.metrics_collector.collect_all_metrics()
                        total_events = (
                            final_metrics["cache_hits"] + final_metrics["cache_misses"] +
                            final_metrics["tinylfu_hits"] + final_metrics["tinylfu_misses"]
                        ) / 2
                        print(f"Final metrics after additional wait: {total_events:.0f} events")
                    
                    final_metrics_read = True
                    break
                else:
                    # Già letto, esci
                    break
            
            # Timeout di sicurezza
            if time.time() - start_time > 3600:  # 1 ora max
                print(f"WARNING: Timeout reached (1 hour)")
                final_metrics = self.metrics_collector.collect_all_metrics()
                final_metrics_read = True
                break
        
        # Se non abbiamo ancora letto le metriche finali, leggile ora
        if not final_metrics_read:
            print("Reading final metrics...")
            time.sleep(5)  # Aspetta un po' per permettere allo streamer di salvare le metriche finali
            final_metrics = self.metrics_collector.collect_all_metrics()
            total_events = (
                final_metrics["cache_hits"] + final_metrics["cache_misses"] +
                final_metrics["tinylfu_hits"] + final_metrics["tinylfu_misses"]
            ) / 2
        else:
            final_metrics = current_metrics if 'current_metrics' in locals() else self.metrics_collector.collect_all_metrics()
            total_events = (
                final_metrics["cache_hits"] + final_metrics["cache_misses"] +
                final_metrics["tinylfu_hits"] + final_metrics["tinylfu_misses"]
            ) / 2
        
        elapsed_time = time.time() - start_time
        
        # Le metriche finali sono già state raccolte sopra, non serve rileggerle
        
        # Calcola metriche delta (sottrai valori iniziali)
        delta_metrics = {}
        for key in final_metrics:
            if isinstance(final_metrics[key], (int, float)):
                initial_val = initial_metrics.get(key, 0)
                delta_metrics[key] = final_metrics[key] - initial_val
        
        # Costruisci risultati
        results = {
            "test_name": test_name,
            "timestamp": time.time(),
            "datetime": datetime.now().isoformat(),
            "dataset_path": dataset_path,
            "max_events": self.max_events,
            "events_processed": total_events,
            "elapsed_time_seconds": elapsed_time,
            "events_per_second": total_events / elapsed_time if elapsed_time > 0 else 0,
            "metrics": final_metrics,
            "delta_metrics": delta_metrics,
        }
        
        if metadata:
            results["metadata"] = metadata
        
        print(f"\nResults for {test_name}:")
        print(f"  Events processed: {total_events:.0f}")
        print(f"  Time elapsed: {elapsed_time:.2f}s ({total_events/elapsed_time:.0f} events/s)")
        print(f"  Baseline hit rate: {final_metrics['baseline_hit_rate']:.4f}")
        print(f"  AURA hit rate: {final_metrics['aura_hit_rate']:.4f}")
        print(f"  Improvement: {final_metrics['improvement']:+.4f} ({final_metrics['improvement_percent']:+.2f}%)")
        print(f"  LLM calls: {final_metrics['llm_calls']:.0f}")
        print(f"  LLM errors: {final_metrics['llm_errors']:.0f}")
        
        try:
            self.generate_temporal_graph(test_name)
        except Exception as e:
            print(f"WARNING: Failed to generate temporal graph: {e}")
        
        return results
    
    def _get_short_filename(self, test_name: str) -> str:
        """Genera un nome file breve e descrittivo basato sul nome del test."""
        if not test_name:
            return "cache_performance"
        
        name_mapping = {
            "normal_dataset": "normal",
            "burst_cooldown": "burst",
            "hot_cold_shift": "hotcold",
            "daily_pattern": "daily",
            "adversarial": "adversarial"
        }
        
        short_name = name_mapping.get(test_name, test_name.replace("_", ""))
        return f"{short_name}_performance"
    
    def reset_temporal_metrics(self):
        """Resetta le metriche temporali per un nuovo test."""
        try:
            metrics_file = Path("data/temporal_metrics/llm_calls_temporal.json")
            if metrics_file.exists():
                metrics_file.unlink()
                print("Temporal metrics file reset for new test")
        except Exception as e:
            print(f"WARNING: Failed to reset temporal metrics: {e}")
    
    def generate_temporal_graph(self, test_name: str):
        """Genera un grafico temporale delle performance delle cache."""
        try:
            import subprocess
            import sys
            import os
            
            metrics_file = Path("data/temporal_metrics/llm_calls_temporal.json")
            if not metrics_file.exists():
                print(f"WARNING: Temporal metrics file not found: {metrics_file}. No LLM calls were made or metrics were not saved.")
                return
            
            graph_script = project_root / "scripts" / "generate_temporal_graph.py"
            if not graph_script.exists():
                print(f"WARNING: Graph generation script not found: {graph_script}")
                return
            
            llm_model = os.getenv("OLLAMA_MODEL", "gemma2:2b").strip()
            llm_model_safe = llm_model.replace(":", "_").replace("/", "_")
            
            base_dir = self.results_dir / "temporal_graphs" / llm_model_safe / test_name
            base_dir.mkdir(parents=True, exist_ok=True)
            
            print(f"Generating temporal graphs for {test_name}...")
            print(f"LLM Model: {llm_model}")
            print(f"Input metrics file: {metrics_file}")
            print(f"Output directory: {base_dir}")
            
            result = subprocess.run(
                [sys.executable, str(graph_script), 
                 "--input", str(metrics_file),
                 "--output", str(base_dir),
                 "--test-name", test_name,
                 "--llm-model", llm_model,
                 "--stats"],
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(project_root)
            )
            
            if result.returncode == 0:
                print(f"Temporal graphs generated successfully in: {base_dir}")
                if result.stdout:
                    print(result.stdout)
            else:
                print(f"ERROR: Graph generation failed with return code {result.returncode}")
                print(f"STDOUT: {result.stdout}")
                print(f"STDERR: {result.stderr}")
        except Exception as e:
            print(f"WARNING: Failed to generate temporal graph: {e}")
            import traceback
            traceback.print_exc()
    
    def reset_redis_completely(self):
        """Resetta completamente entrambi i database Redis."""
        print("Resetting Redis databases completely...")
        try:
            import redis
            r_aura = redis.Redis(host="redis-aura", port=6379, decode_responses=True)
            r_baseline = redis.Redis(host="redis-lru", port=6380, decode_responses=True)
            
            # Flush all databases
            r_aura.flushdb()
            r_baseline.flushdb()
            
            # Verifica che siano vuoti
            aura_keys = r_aura.dbsize()
            baseline_keys = r_baseline.dbsize()
            
            if aura_keys == 0 and baseline_keys == 0:
                print("Redis databases reset successfully (both empty)")
            else:
                print(f"[WARNING] Redis databases may not be empty: aura={aura_keys}, baseline={baseline_keys}")
            
            # Pausa per permettere al sistema di stabilizzarsi
            time.sleep(2)
            
        except Exception as e:
            print(f"[ERROR] Failed to reset Redis: {e}")
            import traceback
            traceback.print_exc()
    
    def ensure_test_file_exists(self, dataset_name: str, source_path: Path) -> Optional[Path]:
        """
        Assicura che il file di test esista, creandolo se necessario.
        
        Args:
            dataset_name: Nome del dataset (es: "normal_dataset", "burst_cooldown")
            source_path: Path al file sorgente originale
        
        Returns:
            Path al file di test (100k eventi) o None se non può essere creato
        """
        test_data_dir = project_root / "data" / "test_data"
        test_data_dir.mkdir(parents=True, exist_ok=True)
        
        test_file = test_data_dir / f"{dataset_name}_100k.json"
        
        if test_file.exists():
            print(f"Test file exists: {test_file.name}")
            return test_file
        
        print(f"Creating test file: {test_file.name} from {source_path.name}")
        
        # Crea il file usando prepare_test_data logic
        try:
            if source_path.suffix == '.json':
                # Scenario JSON
                from src.modules.scenario_loader import ScenarioLoader
                loader = ScenarioLoader(str(source_path), max_events=self.max_events)
                metadata = loader.load_metadata()
                metadata["events_count"] = self.max_events
                metadata["max_events"] = self.max_events
                
                # Salva eventi
                events_list = []
                for i, event in enumerate(loader.generate_events()):
                    if i >= self.max_events:
                        break
                    item_id, user_id, action, timestamp = event
                    events_list.append({
                        "item_id": str(item_id),
                        "user_id": user_id,
                        "action": action,
                        "timestamp": timestamp
                    })
                    if (i + 1) % 10000 == 0:
                        print(f"  Created {i+1}/{self.max_events} events...")
                
                output_data = {
                    "metadata": metadata,
                    "events": events_list
                }
                
                with open(test_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                
                print(f"Created {len(events_list)} events in {test_file.name}")
                
            else:
                # CSV file (dataset normale)
                from src.modules.limited_dataset_loader import LimitedDatasetLoader
                loader = LimitedDatasetLoader(log_file=str(source_path), max_events=self.max_events)
                
                events_list = []
                for i, event in enumerate(loader.generate_events()):
                    if i >= self.max_events:
                        break
                    item_id, user_id, action, timestamp = event
                    events_list.append({
                        "item_id": str(item_id),
                        "user_id": user_id,
                        "action": action,
                        "timestamp": timestamp
                    })
                    if (i + 1) % 10000 == 0:
                        print(f"  Created {i+1}/{self.max_events} events...")
                
                output_data = {
                    "metadata": {
                        "dataset_type": "normal",
                        "source": str(source_path),
                        "events_count": len(events_list),
                        "max_events": self.max_events
                    },
                    "events": events_list
                }
                
                with open(test_file, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)
                
                print(f"Created {len(events_list)} events in {test_file.name}")
            
            return test_file
            
        except Exception as e:
            print(f"ERROR: Failed to create test file: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def run_all_tests(self) -> List[Dict]:
        """
        Esegue i 4 test specifici richiesti:
        1. normal_dataset (100k eventi)
        2. burst_cooldown (100k eventi)
        3. daily_pattern (100k eventi)
        4. hot_cold_shift (100k eventi)
        """
        # IMPORTANTE: Elimina qualsiasi file di configurazione residuo per evitare conflitti
        # con valori vecchi (es. max_events=5000 da esecuzioni precedenti)
        config_file = Path("data/current_test.json")
        if config_file.exists():
            print(f"Removing old config file ({config_file}) to avoid conflicts with previous runs...")
            config_file.unlink()
            time.sleep(1)  # Breve pausa per assicurarsi che il file sia eliminato
        
        all_results = []
        
        # Attendi che i servizi siano pronti
        self.wait_for_services()
        
        # Definisci i test da eseguire (usa i file pre-processati da 100k)
        test_data_dir = project_root / "data" / "test_data"
        tests = [
            {
                "name": "normal_dataset",
                "dataset_name": "normal_dataset",
                "test_file": test_data_dir / "normal_dataset_100k.json",
                "source": project_root / "data" / "log_15M_subset.txt",
                "metadata": {"dataset_type": "normal", "source": "log_15M_subset.txt"}
            },
            {
                "name": "burst_cooldown",
                "dataset_name": "02_burst_cooldown",
                "test_file": test_data_dir / "02_burst_cooldown_100k.json",
                "source": project_root / "data" / "scenarios" / "final" / "02_burst_cooldown.json",
                "metadata": {"dataset_type": "scenario", "scenario_type": "burst_cooldown"}
            },
            {
                "name": "daily_pattern",
                "dataset_name": "04_daily_pattern",
                "test_file": test_data_dir / "04_daily_pattern_100k.json",
                "source": project_root / "data" / "scenarios" / "final" / "04_daily_pattern.json",
                "metadata": {"dataset_type": "scenario", "scenario_type": "daily_pattern"}
            },
            {
                "name": "hot_cold_shift",
                "dataset_name": "01_hot_cold_shift",
                "test_file": test_data_dir / "01_hot_cold_shift_100k.json",
                "source": project_root / "data" / "scenarios" / "final" / "01_hot_cold_shift.json",
                "metadata": {"dataset_type": "scenario", "scenario_type": "hot_cold_shift"}
            }
        ]
        
        # Reset del flag per tracciare quali test hanno già il file di configurazione scritto
        if hasattr(self, '_config_written_for_next_test'):
            delattr(self, '_config_written_for_next_test')
        
        for i, test_config in enumerate(tests, start=1):
            print("\n" + "="*80)
            print(f"TEST {i}/4: {test_config['name']}")
            print("="*80)
            
            # Reset completo Redis prima di ogni test
            if i > 1:  # Non resettare prima del primo test (già fatto in run_test)
                self.reset_redis_completely()
            
            # Usa il file pre-processato se esiste, altrimenti crealo
            test_file = test_config.get('test_file')
            if not test_file or not test_file.exists():
                print(f"Pre-processed file not found, creating it...")
                test_file = self.ensure_test_file_exists(
                    test_config['dataset_name'],
                    test_config['source']
                )
            
            if not test_file or not test_file.exists():
                print(f"ERROR: Cannot proceed with test {test_config['name']}: test file not available")
                continue
            
            try:
                # Carica metadata se è un file JSON
                metadata = test_config['metadata'].copy()
                if test_file.suffix == '.json':
                    try:
                        with open(test_file, 'r') as f:
                            file_data = json.load(f)
                            file_metadata = file_data.get("metadata", {})
                            metadata.update(file_metadata)
                    except:
                        pass
                
                # IMPORTANTE: Per il primo test, scrivi il file di configurazione immediatamente
                # Per i test successivi, il file verrà scritto quando lo streamer è vicino al completamento del test precedente
                if i == 1:
                    print(f"Writing config file for first test: {test_config['name']}")
                    self.config_manager.set_current_test(test_config['name'], str(test_file), self.max_events, metadata)
                    time.sleep(1)
                else:
                    print(f"Test {i}/4: {test_config['name']} - config file will be written when previous test is near completion")
                
                # Determina il prossimo test (se esiste)
                next_test_config = None
                if i < len(tests):
                    next_test_config = tests[i]  # Il prossimo test nella lista
                
                result = self.run_test(
                    test_config['name'],
                    str(test_file),
                    metadata=metadata,
                    next_test_config=next_test_config  # Passa il prossimo test se esiste
                )
                all_results.append(result)
                
                # Genera e salva report dopo ogni test
                print(f"\nGenerating report for test {i}...")
                self.save_report(result)
                
                # Reset completo Redis dopo ogni test (tranne l'ultimo)
                if i < len(tests):
                    print(f"\nTest {i} completed. Resetting Redis for next test...")
                    self.reset_redis_completely()
                    time.sleep(5)  # Pausa per stabilizzazione
                
            except Exception as e:
                print(f"ERROR: Failed to test {test_config['name']}: {e}")
                import traceback
                traceback.print_exc()
                
                # Reset Redis anche in caso di errore
                self.reset_redis_completely()
        
        return all_results
    
    def generate_report(self, result: Dict) -> str:
        """
        Genera un report testuale dettagliato per un singolo test.
        
        Args:
            result: Dizionario con i risultati del test
        
        Returns:
            Stringa con il report formattato
        """
        lines = []
        
        lines.append("="*80)
        lines.append(f"TEST REPORT: {result['test_name']}")
        lines.append("="*80)
        lines.append(f"Timestamp: {result.get('datetime', 'N/A')}")
        lines.append(f"Dataset: {result.get('dataset_path', 'N/A')}")
        lines.append("")
        
        # Performance
        lines.append("PERFORMANCE METRICS:")
        lines.append(f"  Events Processed: {result.get('events_processed', 0):.0f}")
        lines.append(f"  Time Elapsed: {result.get('elapsed_time_seconds', 0):.2f} seconds")
        lines.append(f"  Throughput: {result.get('events_per_second', 0):.0f} events/second")
        lines.append("")
        
        # Cache Performance
        metrics = result.get('metrics', {})
        lines.append("CACHE PERFORMANCE:")
        lines.append(f"  Baseline Hit Rate: {metrics.get('baseline_hit_rate', 0.0):.4f} ({metrics.get('baseline_hit_rate', 0.0)*100:.2f}%)")
        lines.append(f"  AURA Hit Rate: {metrics.get('aura_hit_rate', 0.0):.4f} ({metrics.get('aura_hit_rate', 0.0)*100:.2f}%)")
        lines.append(f"  Improvement: {metrics.get('improvement', 0.0):+.4f} ({metrics.get('improvement_percent', 0.0):+.2f}%)")
        lines.append("")
        lines.append("  Baseline:")
        lines.append(f"    Hits: {metrics.get('tinylfu_hits', 0):.0f}")
        lines.append(f"    Misses: {metrics.get('tinylfu_misses', 0):.0f}")
        lines.append(f"    Total: {metrics.get('tinylfu_hits', 0) + metrics.get('tinylfu_misses', 0):.0f}")
        lines.append("  AURA:")
        lines.append(f"    Hits: {metrics.get('cache_hits', 0):.0f}")
        lines.append(f"    Misses: {metrics.get('cache_misses', 0):.0f}")
        lines.append(f"    Total: {metrics.get('cache_hits', 0) + metrics.get('cache_misses', 0):.0f}")
        lines.append("")
        
        # LLM Metrics
        lines.append("LLM METRICS:")
        lines.append(f"  LLM Calls: {metrics.get('llm_calls', 0):.0f}")
        lines.append(f"  LLM Errors: {metrics.get('llm_errors', 0):.0f}")
        lines.append(f"  LLM Latency: {metrics.get('llm_latency_ms', 0.0):.2f} ms")
        if metrics.get('llm_calls', 0) > 0:
            error_rate = metrics.get('llm_errors', 0) / metrics.get('llm_calls', 1) * 100
            lines.append(f"  LLM Error Rate: {error_rate:.2f}%")
        lines.append("")
        
        # TinyLFU Metrics
        lines.append("TINYLFU METRICS:")
        lines.append(f"  Admissions: {metrics.get('tinylfu_admissions', 0):.0f}")
        lines.append(f"  Rejections: {metrics.get('tinylfu_rejections', 0):.0f}")
        if metrics.get('tinylfu_admissions', 0) + metrics.get('tinylfu_rejections', 0) > 0:
            admission_rate = metrics.get('tinylfu_admissions', 0) / (metrics.get('tinylfu_admissions', 0) + metrics.get('tinylfu_rejections', 0)) * 100
            lines.append(f"  Admission Rate: {admission_rate:.2f}%")
        lines.append(f"  Resets: {metrics.get('tinylfu_resets', 0):.0f}")
        lines.append(f"  Decay Applications: {metrics.get('tinylfu_decay_applications', 0):.0f}")
        lines.append(f"  Reset Interval Updates: {metrics.get('tinylfu_reset_interval_updates', 0):.0f}")
        lines.append("")
        
        # Safety Guard
        lines.append("SAFETY GUARD:")
        
        # System Metrics
        lines.append("SYSTEM METRICS:")
        lines.append(f"  Redis Memory Usage: {metrics.get('redis_memory_bytes', 0):.0f} bytes ({metrics.get('redis_memory_bytes', 0)/(1024*1024):.2f} MB)")
        lines.append("")
        
        # Delta Metrics (se disponibili)
        delta_metrics = result.get('delta_metrics', {})
        if delta_metrics:
            lines.append("METRICS DELTA (from test start):")
            if delta_metrics.get('llm_calls', 0) > 0:
                lines.append(f"  LLM Calls: +{delta_metrics.get('llm_calls', 0):.0f}")
            if delta_metrics.get('tinylfu_resets', 0) > 0:
                lines.append(f"  TinyLFU Resets: +{delta_metrics.get('tinylfu_resets', 0):.0f}")
            lines.append("")
        
        # Metadata
        metadata = result.get('metadata', {})
        if metadata:
            lines.append("METADATA:")
            for key, value in metadata.items():
                lines.append(f"  {key}: {value}")
            lines.append("")
        
        lines.append("="*80)
        
        return "\n".join(lines)
    
    def save_report(self, result: Dict):
        """
        Salva un report testuale per un singolo test.
        
        Args:
            result: Dizionario con i risultati del test
        """
        reports_dir = self.results_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        test_name = result.get('test_name', 'unknown')
        report_filename = f"report_{test_name}_{timestamp}.txt"
        report_path = reports_dir / report_filename
        
        report_text = self.generate_report(result)
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(f"\nReport saved to: {report_path}")
        print("\n" + report_text)
        
        return report_path
    
    def save_results(self, results: List[Dict], filename: Optional[str] = None):
        """Salva i risultati in un file JSON."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"test_results_{timestamp}.json"
        
        output_path = self.results_dir / filename
        
        output_data = {
            "test_run": {
                "timestamp": time.time(),
                "datetime": datetime.now().isoformat(),
                "max_events_per_test": self.max_events,
                "total_tests": len(results)
            },
            "results": results
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\nResults saved to: {output_path}")
        return output_path


def main():
    """Main entry point per il test runner."""
    max_events = int(os.getenv("MAX_EVENTS", "1000000"))
    
    print("="*80)
    print("AUTOMATED TEST RUNNER")
    print("="*80)
    print(f"Max events per test: {max_events}")
    print(f"Starting at: {datetime.now().isoformat()}")
    print("="*80)
    
    runner = TestRunner(max_events=max_events)
    results = runner.run_all_tests()
    
    # Salva risultati aggregati
    results_file = runner.save_results(results)
    
    # Genera report finale riepilogativo
    print("\n" + "="*80)
    print("FINAL SUMMARY REPORT")
    print("="*80)
    
    if results:
        print(f"\nTotal tests completed: {len(results)}")
        print(f"Completed at: {datetime.now().isoformat()}")
        print("\nTest Results Summary:")
        print("-" * 80)
        
        for i, result in enumerate(results, 1):
            test_name = result.get('test_name', 'unknown')
            metrics = result.get('metrics', {})
            baseline_hr = metrics.get('baseline_hit_rate', 0.0)
            aura_hr = metrics.get('aura_hit_rate', 0.0)
            improvement = metrics.get('improvement', 0.0)
            improvement_pct = metrics.get('improvement_percent', 0.0)
            elapsed = result.get('elapsed_time_seconds', 0.0)
            
            print(f"{i}. {test_name}:")
            print(f"   Baseline HR: {baseline_hr:.4f} | AURA HR: {aura_hr:.4f} | Improvement: {improvement:+.4f} ({improvement_pct:+.2f}%)")
            print(f"   Time: {elapsed:.2f}s | Events: {result.get('events_processed', 0):.0f}")
        
        # Calcola medie
        avg_baseline_hr = sum(r.get('metrics', {}).get('baseline_hit_rate', 0.0) for r in results) / len(results)
        avg_aura_hr = sum(r.get('metrics', {}).get('aura_hit_rate', 0.0) for r in results) / len(results)
        avg_improvement = sum(r.get('metrics', {}).get('improvement', 0.0) for r in results) / len(results)
        total_llm_calls = sum(r.get('metrics', {}).get('llm_calls', 0) for r in results)
        total_llm_errors = sum(r.get('metrics', {}).get('llm_errors', 0) for r in results)
        
        print("\n" + "-" * 80)
        print("AVERAGES:")
        print(f"  Baseline Hit Rate: {avg_baseline_hr:.4f}")
        print(f"  AURA Hit Rate: {avg_aura_hr:.4f}")
        print(f"  Average Improvement: {avg_improvement:+.4f}")
        print(f"  Total LLM Calls: {total_llm_calls:.0f}")
        print(f"  Total LLM Errors: {total_llm_errors:.0f}")
        if total_llm_calls > 0:
            print(f"  LLM Error Rate: {total_llm_errors/total_llm_calls*100:.2f}%")
        
        print(f"\nDetailed results saved to: {results_file}")
        print(f"Individual reports saved to: {runner.results_dir / 'reports'}")
    
    print("\n" + "="*80)
    print("ALL TESTS COMPLETED")
    print("="*80)


if __name__ == "__main__":
    main()

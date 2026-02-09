"""
Configurazione globale del sistema di scenario generation.

Questo modulo centralizza tutti i path, parametri e configurazioni
necessarie per il sistema di generazione scenari.

Author: Scenario Generation System
Date: 2025-02-02
"""

from pathlib import Path
import os


class Config:
    """
    Configurazione globale per scenario generation.
    
    Tutti i path sono relativi alla root del progetto (directory contenente 'scripts').
    """
    
    # ========================================================================
    # PATH CONFIGURATION
    # ========================================================================
    
    # Root directory del progetto (assume config.py è in scripts/scenario_generation/)
    ROOT_DIR = Path(__file__).parent.parent.parent
    
    # Data directories
    DATA_DIR = ROOT_DIR / 'data'
    SCENARIOS_DIR = DATA_DIR / 'scenarios'
    RAW_SCENARIOS_DIR = SCENARIOS_DIR / 'raw'
    FINAL_SCENARIOS_DIR = SCENARIOS_DIR / 'final'
    
    # Dataset files
    TIANCHI_DATASET = DATA_DIR / 'tianchi_dataset.csv'
    TIANCHI_INDEX = DATA_DIR / 'tianchi_index.json'
    
    # Analysis output
    ANALYSIS_DIR = ROOT_DIR / 'analysis'
    SCENARIO_ANALYSIS_REPORT = ANALYSIS_DIR / 'scenario_analysis_report.md'
    SCENARIO_COMPARISON_PLOT = ANALYSIS_DIR / 'scenario_comparison.png'
    STATISTICAL_VALIDATION = ANALYSIS_DIR / 'statistical_validation.json'
    
    # Documentation
    DOCS_DIR = ROOT_DIR / 'docs'
    SCENARIO_METHODOLOGY = DOCS_DIR / 'scenario_methodology.md'
    
    # ========================================================================
    # PROCESSING PARAMETERS
    # ========================================================================
    
    # Chunked reading
    CHUNK_SIZE = 500000  # Righe per chunk (memory-safe per 15M rows)
    
    # Memory limits
    MAX_RAM_MB = 500  # Limite massimo RAM in MB
    
    # Progress reporting
    PROGRESS_UPDATE_INTERVAL = 10  # Log memory ogni N chunks
    
    # ========================================================================
    # SCENARIO PARAMETERS
    # ========================================================================
    
    SCENARIOS = {
        'hot_cold_shift': {
            'max_overlap': 0.3,  # Top-K overlap massimo tra finestre
            'min_shift_days': 14,  # Giorni minimi tra le finestre calde/fredde
            'target_events': 12000,  # Eventi target per scenario
            'top_k': 100,  # Top-K items da considerare
        },
        'burst_cooldown': {
            'min_intensity': 50.0,  # Ratio peak/baseline minimo
            'min_cv': 2.0,  # Coefficient of variation minimo
            'context_days': 3,  # Giorni prima/dopo burst da includere
            'target_events': 8000,  # Eventi target per scenario
            'min_burst_duration_hours': 2,  # Durata minima burst in ore
        },
        'scan_attack': {
            'min_unique_rate': 0.80,  # Percentuale item unici minima
            'max_repeat_rate': 0.15,  # Percentuale item ripetuti massima
            'target_events': 10000,  # Eventi target per scenario
            'min_scan_window_hours': 1,  # Finestra minima per scan
        },
        'daily_pattern': {
            'min_autocorr': 0.6,  # Autocorrelazione minima (lag=24h)
            'num_cycles': 3,  # Numero di cicli giornalieri da includere
            'target_events': 15000,  # Eventi target per scenario
            'min_cycle_length_days': 1,  # Lunghezza minima ciclo
        },
        'multi_modal': {
            'min_clusters': 3,  # Numero minimo di cluster
            'max_clusters': 5,  # Numero massimo di cluster
            'min_silhouette': 0.5,  # Silhouette score minimo
            'target_events': 10000,  # Eventi target per scenario
            'features': ['hour', 'day_of_week', 'item_frequency'],  # Features per clustering
        },
        'adversarial': {
            'num_items': 200,  # Numero di item da generare
            'threshold_factor': 1.1,  # Frequenza = threshold * factor
            'target_events': 5000,  # Eventi target per scenario
            'attack_pattern': 'uniform',  # Pattern: 'uniform', 'burst', 'gradual'
        }
    }
    
    # ========================================================================
    # INDEXING PARAMETERS
    # ========================================================================
    
    # Top items per giorno nell'indice
    INDEX_TOP_ITEMS = 100
    
    # Statistiche da calcolare nell'indice
    INDEX_STATISTICS = {
        'zipf_alpha': True,
        'gini_coefficient': True,
        'repeat_rate': True,
        'hourly_distribution': True,
        'action_distribution': True,
    }
    
    # ========================================================================
    # REPRODUCIBILITY
    # ========================================================================
    
    RANDOM_SEED = 42  # Seed per riproducibilità
    
    # ========================================================================
    # LOGGING
    # ========================================================================
    
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # ========================================================================
    # DATASET FORMAT
    # ========================================================================
    
    # Colonne del dataset Tianchi
    DATASET_COLUMNS = ['Item_id', 'User_id', 'Action', 'Vtime']
    
    # Formato timestamp
    TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
    
    # Encoding del dataset
    DATASET_ENCODING = 'utf-8'  # Fallback: 'latin-1'
    
    # Delimitatore CSV
    # Nota: Il file tianchi_2014002_rec_tmall_log_parta.txt usa '\x01' come separatore
    CSV_DELIMITER = '\x01'  # Carattere di controllo (SOH - Start of Heading)
    
    # Il file non ha header, quindi dobbiamo specificare i nomi colonne
    DATASET_HAS_HEADER = False
    
    # ========================================================================
    # VALIDATION THRESHOLDS
    # ========================================================================
    
    # Validazione scenari
    MIN_SCENARIO_EVENTS = 1000  # Eventi minimi per scenario valido
    MAX_SCENARIO_EVENTS = 50000  # Eventi massimi per scenario
    
    # Validazione indice
    MIN_INDEX_DAYS = 1  # Giorni minimi nell'indice
    MAX_MISSING_DAYS_RATIO = 0.1  # Percentuale massima giorni mancanti
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    @classmethod
    def ensure_directories(cls):
        """
        Crea tutte le directory necessarie se non esistono.
        
        Returns:
            None
        """
        directories = [
            cls.DATA_DIR,
            cls.SCENARIOS_DIR,
            cls.RAW_SCENARIOS_DIR,
            cls.FINAL_SCENARIOS_DIR,
            cls.ANALYSIS_DIR,
            cls.DOCS_DIR,
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_scenario_config(cls, scenario_name: str) -> dict:
        """
        Ottiene la configurazione per un scenario specifico.
        
        Args:
            scenario_name: Nome dello scenario
            
        Returns:
            dict: Configurazione dello scenario
            
        Raises:
            KeyError: Se lo scenario non esiste
        """
        if scenario_name not in cls.SCENARIOS:
            available = ', '.join(cls.SCENARIOS.keys())
            raise KeyError(
                f"Unknown scenario '{scenario_name}'. "
                f"Available scenarios: {available}"
            )
        return cls.SCENARIOS[scenario_name]
    
    @classmethod
    def validate_paths(cls) -> bool:
        """
        Valida che i path principali esistano.
        
        Returns:
            bool: True se tutti i path sono validi
            
        Note:
            Non crea directory, solo verifica esistenza.
        """
        required_paths = [
            cls.ROOT_DIR,
            cls.DATA_DIR,
        ]
        
        missing = [p for p in required_paths if not p.exists()]
        
        if missing:
            print(f"[WARNING] Missing required paths:")
            for p in missing:
                print(f"  - {p}")
            return False
        
        return True
    
    @classmethod
    def get_dataset_path(cls) -> Path:
        """
        Ottiene il path del dataset, con fallback se non esiste il file principale.
        
        Returns:
            Path: Path al dataset
            
        Note:
            Cerca anche varianti comuni del nome file.
        """
        # Try primary path
        if cls.TIANCHI_DATASET.exists():
            return cls.TIANCHI_DATASET
        
        # Try alternative names
        alternatives = [
            cls.DATA_DIR / 'tianchi_2014002_rec_tmall_log_parta.txt',
            cls.DATA_DIR / 'UserBehavior.csv',
            cls.DATA_DIR / 'tianchi_dataset.txt',
        ]
        
        for alt in alternatives:
            if alt.exists():
                print(f"[INFO] Using alternative dataset path: {alt}")
                return alt
        
        # Return primary path anyway (will fail later with clear error)
        return cls.TIANCHI_DATASET

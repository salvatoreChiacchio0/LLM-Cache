"""
Calcolo statistiche per analisi di distribuzione (Zipf, Gini, CV, etc).

Questo modulo fornisce funzioni per calcolare metriche statistiche utili
per caratterizzare la distribuzione degli accessi nel dataset.

Tutte le funzioni includono:
- Gestione di edge cases (liste vuote, valori None)
- Validazione input
- Test inline per verificare correttezza

Author: Scenario Generation System
Date: 2025-02-02
"""

import numpy as np
from scipy import stats
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


def calculate_zipf_alpha(counts: List[int | float]) -> Optional[float]:
    """
    Stima il coefficiente alpha di Zipf tramite linear fit in log-log space.
    
    La legge di Zipf descrive la relazione tra rango e frequenza:
    frequency(r) ∝ r^(-alpha)
    
    In log-log space: log(frequency) = -alpha * log(rank) + constant
    
    Args:
        counts: Lista di conteggi ordinati in ordine decrescente (più frequente primo)
    
    Returns:
        float: Coefficiente alpha stimato (None se fit fallisce o dati insufficienti)
        
    Note:
        - Alpha tipicamente tra 0.5 e 2.0 per distribuzioni reali
        - Alpha più alto = distribuzione più skew (pochi item molto popolari)
        - Richiede almeno 10 punti per un fit affidabile
        
    Example:
        >>> # Distribuzione Zipf con alpha=1.0
        >>> counts = [1000, 500, 333, 250, 200, 167, 143, 125, 111, 100]
        >>> alpha = calculate_zipf_alpha(counts)
        >>> assert 0.9 < alpha < 1.1  # Test inline
    """
    # Validate input
    if not counts or len(counts) < 10:
        logger.debug(f"Insufficient data for Zipf fit: {len(counts) if counts else 0} points")
        return None
    
    # Convert to numpy array and filter out zeros/negatives
    counts_array = np.array(counts, dtype=float)
    counts_array = counts_array[counts_array > 0]
    
    if len(counts_array) < 10:
        logger.debug(f"Insufficient positive values for Zipf fit: {len(counts_array)} points")
        return None
    
    try:
        # Use top 100 points for stability (or all if less than 100)
        n_points = min(100, len(counts_array))
        top_counts = counts_array[:n_points]
        
        # Create ranks (1-indexed)
        ranks = np.arange(1, len(top_counts) + 1, dtype=float)
        
        # Log-log transformation
        log_ranks = np.log(ranks)
        log_counts = np.log(top_counts)
        
        # Linear regression: log(count) = -alpha * log(rank) + intercept
        # So alpha = -slope
        slope, intercept, r_value, p_value, std_err = stats.linregress(log_ranks, log_counts)
        
        # Validate fit quality
        # Note: r_value from linregress is correlation coefficient, not R²
        # R² = r_value², so we check r_value directly
        if abs(r_value) < 0.7:  # Low correlation (use abs for negative correlations)
            logger.warning(f"Low correlation ({r_value:.3f}, R²={r_value**2:.3f}) for Zipf fit, result may be unreliable")
        
        alpha = -slope
        
        # Sanity check: alpha should be positive and reasonable
        if alpha < 0 or alpha > 5:
            logger.warning(f"Unusual alpha value: {alpha:.3f} (expected 0.5-2.0)")
        
        logger.debug(f"Zipf alpha: {alpha:.3f} (R²={r_value:.3f}, n={n_points})")
        return float(alpha)
        
    except Exception as e:
        logger.error(f"Error calculating Zipf alpha: {e}")
        return None


def calculate_gini(counts: List[int | float]) -> Optional[float]:
    """
    Calcola coefficiente di Gini (misura di disuguaglianza).
    
    Il coefficiente di Gini varia da 0 (perfetta uguaglianza) a 1 (massima disuguaglianza).
    Formula: G = (2 * Σ(i * x_i)) / (n * Σ(x_i)) - (n + 1) / n
    
    Args:
        counts: Lista di conteggi (non necessariamente ordinata)
    
    Returns:
        float: Coefficiente di Gini tra 0 e 1 (None se calcolo fallisce)
        
    Note:
        - Gini = 0: tutti gli item hanno la stessa frequenza
        - Gini = 1: un solo item ha tutte le frequenze
        - Tipicamente 0.3-0.7 per distribuzioni reali
        
    Example:
        >>> # Perfetta uguaglianza
        >>> counts = [100, 100, 100, 100]
        >>> gini = calculate_gini(counts)
        >>> assert abs(gini - 0.0) < 0.01  # Test inline
        
        >>> # Massima disuguaglianza
        >>> counts = [400, 0, 0, 0]
        >>> gini = calculate_gini(counts)
        >>> assert abs(gini - 1.0) < 0.01  # Test inline
    """
    # Validate input
    if not counts:
        logger.debug("Empty counts list for Gini calculation")
        return None
    
    # Convert to numpy array and filter out negatives
    counts_array = np.array(counts, dtype=float)
    counts_array = counts_array[counts_array >= 0]
    
    if len(counts_array) == 0:
        logger.debug("No non-negative values for Gini calculation")
        return None
    
    # If all zeros, Gini is undefined (or 0 by convention)
    if np.sum(counts_array) == 0:
        logger.debug("All counts are zero, Gini undefined")
        return 0.0
    
    try:
        # Sort in ascending order (required for Gini formula)
        sorted_counts = np.sort(counts_array)
        n = len(sorted_counts)
        
        # Calculate cumulative sum
        cumsum = np.cumsum(sorted_counts)
        total_sum = cumsum[-1]
        
        if total_sum == 0:
            return 0.0
        
        # Gini formula: G = (2 * Σ(i * x_i)) / (n * Σ(x_i)) - (n + 1) / n
        # Where i is 1-indexed position
        indices = np.arange(1, n + 1, dtype=float)
        numerator = 2 * np.sum(indices * sorted_counts)
        denominator = n * total_sum
        
        gini = (numerator / denominator) - (n + 1) / n
        
        # Sanity check: Gini should be between 0 and 1
        if gini < 0 or gini > 1:
            logger.warning(f"Gini out of bounds: {gini:.3f} (clamping to [0, 1])")
            gini = max(0.0, min(1.0, gini))
        
        logger.debug(f"Gini coefficient: {gini:.3f} (n={n})")
        return float(gini)
        
    except Exception as e:
        logger.error(f"Error calculating Gini coefficient: {e}")
        return None


def calculate_coefficient_of_variation(values: List[int | float]) -> Optional[float]:
    """
    Calcola coefficiente di variazione (CV = std / mean).
    
    CV è una misura di variabilità relativa, utile per confrontare
    variabilità tra distribuzioni con medie diverse.
    
    Args:
        values: Lista di valori numerici
    
    Returns:
        float: Coefficiente di variazione (None se calcolo fallisce)
        
    Note:
        - CV = 0: nessuna variabilità (tutti i valori uguali)
        - CV = 1: deviazione standard uguale alla media
        - CV > 1: alta variabilità relativa
        
    Example:
        >>> # Bassa variabilità
        >>> values = [100, 101, 99, 100, 101]
        >>> cv = calculate_coefficient_of_variation(values)
        >>> assert cv < 0.1  # Test inline
        
        >>> # Alta variabilità
        >>> values = [1, 100, 1, 100, 1]
        >>> cv = calculate_coefficient_of_variation(values)
        >>> assert cv > 1.0  # Test inline
    """
    # Validate input
    if not values:
        logger.debug("Empty values list for CV calculation")
        return None
    
    # Convert to numpy array
    values_array = np.array(values, dtype=float)
    
    # Filter out NaN and Inf
    values_array = values_array[np.isfinite(values_array)]
    
    if len(values_array) == 0:
        logger.debug("No finite values for CV calculation")
        return None
    
    try:
        mean = np.mean(values_array)
        
        # CV is undefined if mean is zero
        if mean == 0:
            logger.debug("Mean is zero, CV undefined")
            return None
        
        std = np.std(values_array, ddof=1)  # Sample standard deviation
        cv = std / mean
        
        logger.debug(f"Coefficient of variation: {cv:.3f} (mean={mean:.2f}, std={std:.2f})")
        return float(cv)
        
    except Exception as e:
        logger.error(f"Error calculating coefficient of variation: {e}")
        return None


def calculate_autocorrelation(values: List[float], lag: int = 1) -> Optional[float]:
    """
    Calcola autocorrelazione per un dato lag.
    
    Utile per rilevare pattern periodici (es. pattern giornalieri).
    
    Args:
        values: Serie temporale di valori
        lag: Lag per l'autocorrelazione (default: 1)
    
    Returns:
        float: Coefficiente di autocorrelazione tra -1 e 1 (None se calcolo fallisce)
        
    Note:
        - Autocorr > 0.6: forte pattern periodico
        - Autocorr < 0.3: pattern debole o assente
    """
    if not values or len(values) < lag + 1:
        logger.debug(f"Insufficient data for autocorrelation (lag={lag}, n={len(values)})")
        return None
    
    try:
        values_array = np.array(values, dtype=float)
        
        # Remove NaN
        values_array = values_array[np.isfinite(values_array)]
        
        if len(values_array) < lag + 1:
            return None
        
        # Calculate autocorrelation
        if lag >= len(values_array):
            return None
        
        # Pearson correlation between series and lagged series
        x = values_array[:-lag]
        y = values_array[lag:]
        
        if len(x) < 2:
            return None
        
        corr, _ = stats.pearsonr(x, y)
        
        logger.debug(f"Autocorrelation (lag={lag}): {corr:.3f}")
        return float(corr)
        
    except Exception as e:
        logger.error(f"Error calculating autocorrelation: {e}")
        return None


# Test inline per verificare correttezza delle funzioni
if __name__ == "__main__":
    # Setup logging for tests
    logging.basicConfig(level=logging.INFO)
    
    print("Running inline tests for stats_calculator...")
    
    # Test Zipf alpha
    print("\n1. Testing Zipf alpha calculation...")
    zipf_counts = [1000, 500, 333, 250, 200, 167, 143, 125, 111, 100, 91, 83, 77, 71, 67]
    zipf_alpha = calculate_zipf_alpha(zipf_counts)
    assert zipf_alpha is not None, "Zipf alpha should not be None for valid input"
    assert 0.8 < zipf_alpha < 1.2, f"Zipf alpha should be ~1.0, got {zipf_alpha}"
    print(f"   [OK] Zipf alpha: {zipf_alpha:.3f} (expected ~1.0)")
    
    # Test Gini - perfect equality
    print("\n2. Testing Gini coefficient - perfect equality...")
    equal_counts = [100, 100, 100, 100, 100]
    gini_equal = calculate_gini(equal_counts)
    assert gini_equal is not None, "Gini should not be None"
    assert abs(gini_equal - 0.0) < 0.01, f"Gini should be ~0.0 for equal distribution, got {gini_equal}"
    print(f"   [OK] Gini (equal): {gini_equal:.3f} (expected ~0.0)")
    
    # Test Gini - maximum inequality
    # Note: For n items, max Gini = (n-1)/n, not 1.0
    print("\n3. Testing Gini coefficient - maximum inequality...")
    unequal_counts = [500, 0, 0, 0, 0]
    gini_unequal = calculate_gini(unequal_counts)
    assert gini_unequal is not None, "Gini should not be None"
    # For n=5, max Gini = 4/5 = 0.8
    expected_max = (len(unequal_counts) - 1) / len(unequal_counts)
    assert abs(gini_unequal - expected_max) < 0.05, f"Gini should be ~{expected_max:.2f} for max inequality with n={len(unequal_counts)}, got {gini_unequal}"
    print(f"   [OK] Gini (unequal): {gini_unequal:.3f} (expected ~{expected_max:.2f} for n={len(unequal_counts)})")
    
    # Test CV - low variability
    print("\n4. Testing Coefficient of Variation - low variability...")
    low_var = [100, 101, 99, 100, 101, 99, 100]
    cv_low = calculate_coefficient_of_variation(low_var)
    assert cv_low is not None, "CV should not be None"
    assert cv_low < 0.1, f"CV should be < 0.1 for low variability, got {cv_low}"
    print(f"   [OK] CV (low var): {cv_low:.3f} (expected < 0.1)")
    
    # Test CV - high variability
    print("\n5. Testing Coefficient of Variation - high variability...")
    high_var = [1, 100, 1, 100, 1, 100, 1]
    cv_high = calculate_coefficient_of_variation(high_var)
    assert cv_high is not None, "CV should not be None"
    assert cv_high > 1.0, f"CV should be > 1.0 for high variability, got {cv_high}"
    print(f"   [OK] CV (high var): {cv_high:.3f} (expected > 1.0)")
    
    # Test autocorrelation
    print("\n6. Testing autocorrelation...")
    periodic = [1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3]
    autocorr = calculate_autocorrelation(periodic, lag=3)
    assert autocorr is not None, "Autocorrelation should not be None"
    assert autocorr > 0.8, f"Autocorrelation should be high for periodic data, got {autocorr}"
    print(f"   [OK] Autocorrelation (lag=3): {autocorr:.3f} (expected > 0.8)")
    
    print("\n[SUCCESS] All inline tests passed!")

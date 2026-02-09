import time
import hashlib
import threading
from collections import OrderedDict
from typing import Optional, Dict, Any
import redis
try:
    from ..modules.metrics import (
        TINYLFU_ADMISSIONS, TINYLFU_REJECTIONS, TINYLFU_RESETS,
        TINYLFU_BIAS_ADMISSIONS, TINYLFU_DECAY_APPLICATIONS, TINYLFU_RESET_INTERVAL_UPDATES
    )
except ImportError:
    class DummyCounter:
        def inc(self): pass
    TINYLFU_ADMISSIONS = DummyCounter()
    TINYLFU_REJECTIONS = DummyCounter()
    TINYLFU_RESETS = DummyCounter()
    TINYLFU_BIAS_ADMISSIONS = DummyCounter()
    TINYLFU_DECAY_APPLICATIONS = DummyCounter()
    TINYLFU_RESET_INTERVAL_UPDATES = DummyCounter()


class CountMinSketch:
    """
    Count-Min Sketch data structure for approximate frequency counting.
    Uses multiple hash functions to estimate item frequencies with bounded error.
    """
    
    def __init__(self, width: int = 256, depth: int = 4):
        """
        Args:
            width: Number of counters per hash function (larger = more accurate, more memory)
            depth: Number of hash functions (more = better accuracy, more computation)
        """
        self.width = width
        self.depth = depth
        self.counters = [[0] * width for _ in range(depth)]
        self.total_additions = 0
        self.lock = threading.RLock()
    
    def _hash(self, key: str, seed: int) -> int:
        """Generate hash for key with given seed."""
        h = hashlib.md5(f"{key}:{seed}".encode()).hexdigest()
        return int(h, 16) % self.width
    
    def increment(self, key: str, count: int = 1):
        """Increment frequency estimate for key."""
        with self.lock:
            self.total_additions += count
            for i in range(self.depth):
                idx = self._hash(key, i)
                self.counters[i][idx] += count
    
    def estimate(self, key: str) -> int:
        """Estimate frequency of key (returns minimum across all hash functions)."""
        with self.lock:
            min_count = float('inf')
            for i in range(self.depth):
                idx = self._hash(key, i)
                min_count = min(min_count, self.counters[i][idx])
            return int(min_count)
    
    def reset(self):
        """Reset all counters to zero."""
        with self.lock:
            for i in range(self.depth):
                for j in range(self.width):
                    self.counters[i][j] = 0
            self.total_additions = 0


class Doorkeeper:
    """
    Admission filter that gives items a "free pass" on first access.
    Prevents very low-frequency items from being admitted to cache.
    Implements a Bloom-filter-style doorkeeper with multiple hashes.
    """
    
    def __init__(self, size: int = 8192, num_hashes: int = 4):
        """
        Args:
            size: Size of the bloom-filter-like structure (must be power of 2)
            num_hashes: Number of hash functions used by the Bloom filter
        """
        self.size = 1
        while self.size < size:
            self.size <<= 1
        self.mask = self.size - 1
        if num_hashes <= 0:
            raise ValueError(f"num_hashes must be >= 1, got {num_hashes}")
        self.num_hashes = num_hashes
        self.bitmap = [False] * self.size
        self.lock = threading.RLock()

    def _hash(self, key: str, seed: int) -> int:
        """Generate hash for key with given seed."""
        h = hashlib.md5(f"{key}:{seed}".encode()).hexdigest()
        return int(h, 16) & self.mask
    
    def allow(self, key: str) -> bool:
        """
        Check if key should be allowed (first access gets free pass).
        Returns True if key should be allowed, False otherwise.
        """
        with self.lock:
            present = True
            for i in range(self.num_hashes):
                idx = self._hash(key, i)
                if not self.bitmap[idx]:
                    present = False
            if not present:
                for i in range(self.num_hashes):
                    idx = self._hash(key, i)
                    self.bitmap[idx] = True
                return True
            return False

    def contains(self, key: str) -> bool:
        """Check if key is present in the doorkeeper."""
        with self.lock:
            for i in range(self.num_hashes):
                idx = self._hash(key, i)
                if not self.bitmap[idx]:
                    return False
            return True
    
    def reset(self):
        """Reset the doorkeeper (clear all bits)."""
        with self.lock:
            for i in range(self.size):
                self.bitmap[i] = False


class TinyLFU:
    """
    TinyLFU cache admission policy (standard implementation).
    
    Implements the standard TinyLFU algorithm as described in the research paper:
    - Combines Count-Min Sketch for approximate frequency estimation
    - Uses Doorkeeper filter for admission filtering (first access gets free pass)
    - Periodic reset mechanism to prevent stale frequency data
    Standard TinyLFU admission behavior:
    1. When a new item arrives, take the cache's eviction candidate (e.g., LRU victim)
    2. Compare new item frequency with victim frequency
    3. Admit new item only if its frequency > victim frequency
    
    This implementation integrates with Redis for actual storage while maintaining
    frequency statistics in memory.
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        sketch_width: int = 256,
        sketch_depth: int = 4,
        doorkeeper_size: int = 8192,
        reset_interval: int = 100000,
        sample_size: int = 10
    ):
        """
        Args:
            redis_client: Redis client for actual storage
            sketch_width: Width of Count-Min Sketch
            sketch_depth: Depth of Count-Min Sketch
            doorkeeper_size: Size of Doorkeeper filter
            reset_interval: Sample size (W) before halving frequency data
            sample_size: Legacy parameter, not used for LRU eviction
        """
        self.redis = redis_client
        self.sketch = CountMinSketch(width=sketch_width, depth=sketch_depth)
        self.doorkeeper = Doorkeeper(size=doorkeeper_size)
        self.reset_interval = reset_interval
        self.sample_size = sample_size
        self.access_count = 0
        self.lock = threading.RLock()
        self.decay_factor_applied = None
        self.admission_bias = 0
        
        self.cache_order = OrderedDict()
        self.max_cache_size = 10000

        self.ghost_cache = OrderedDict() 
        self.ghost_cache_size = 1000
        self.eviction_count = 0
        self.ghost_hits = 0
        
        self.should_admit_calls = 0
        self.should_admit_rejections = 0
        self.should_admit_cache_full_calls = 0
        self.debug_logging = False
    
    def _sync_cache_tracking(self):
        """
        Synchronize cache_order tracking with actual Redis cache.
        Removes keys from tracking that no longer exist in Redis.
        This helps maintain consistency when Redis evicts items automatically.
        """
        with self.lock:
            keys_to_remove = []
            sample_keys = list(self.cache_order.keys())[:100]
            
            for key in sample_keys:
                if not self.redis.exists(key):
                    keys_to_remove.append(key)
            
            for key in keys_to_remove:
                self.cache_order.pop(key, None)
    
    def _should_reset(self) -> bool:
        """Check if frequency data should be halved."""
        with self.lock:
            if self.access_count >= self.reset_interval:
                return True
            return False
    
    def _reset_frequency_data(self):
        """Halve frequency estimation data and clear Doorkeeper (paper behavior)."""
        with self.lock:
            for i in range(self.sketch.depth):
                for j in range(self.sketch.width):
                    self.sketch.counters[i][j] //= 2
            self.sketch.total_additions //= 2
            self.doorkeeper.reset()
            self.access_count = 0
            TINYLFU_RESETS.inc()
    
    def access(self, key: str) -> bool:
        """
        Record an access to key and update frequency.
        Returns True if key is in cache, False otherwise.
        """
        exists = self.redis.exists(key)
        
        with self.lock:
            first_time = self.doorkeeper.allow(key)
            if not first_time:
                self.sketch.increment(key, 1)
                self.access_count += 1
            
            if exists:
                if key in self.cache_order:
                    self.cache_order.move_to_end(key)
                else:
                    if len(self.cache_order) >= self.max_cache_size:
                        self.cache_order.popitem(last=False)
                    self.cache_order[key] = time.time()
            else:
                self.cache_order.pop(key, None)
                
                if key in self.ghost_cache:
                    self.ghost_hits += 1
            
            if self._should_reset():
                self._reset_frequency_data()
        
        return bool(exists)
    
    def record_access(self, key: str):
        """
        Record an access to key and update frequency (for tracking purposes).
        This is called for every item access, even if not in cache.
        """
        with self.lock:
            first_time = self.doorkeeper.allow(key)
            if not first_time:
                self.sketch.increment(key, 1)
                self.access_count += 1
            
            if self._should_reset():
                self._reset_frequency_data()
    
    def should_admit(self, key: str, victim_key: Optional[str] = None) -> bool:
        """
        Determine if a new key should be admitted to cache.
        Uses admission_bias to shift the decision threshold.
        
        admission_bias > 0: favors new items (admit more, faster adaptation)
        admission_bias < 0: favors existing items (reject more, preserve cache)
        admission_bias = 0: standard TinyLFU behavior
        
        Args:
            key: The new key to potentially admit
            victim_key: Optional key that would be evicted to make room
        
        Returns:
            True if key should be admitted, False otherwise
        """
        with self.lock:
            self.should_admit_calls += 1
        
        if victim_key is None:
            TINYLFU_ADMISSIONS.inc()
            return True
        
        with self.lock:
            self.should_admit_cache_full_calls += 1
        
        new_freq = self.get_frequency(key)
        victim_freq = self.get_frequency(victim_key)
        
        should_admit = (new_freq + self.admission_bias) > victim_freq
        
        if self.debug_logging:
            print(f"[TinyLFU should_admit] key={key[:20]}, victim={victim_key[:20] if victim_key else 'None'}, "
                  f"new_freq={new_freq}, victim_freq={victim_freq}, bias={self.admission_bias}, "
                  f"decision={'ADMIT' if should_admit else 'REJECT'}")
        
        if should_admit:
            TINYLFU_ADMISSIONS.inc()
        else:
            TINYLFU_REJECTIONS.inc()
            with self.lock:
                self.should_admit_rejections += 1
        
        return should_admit
    
    def set_admission_bias(self, bias: int):
        """
        Set the admission bias for TinyLFU admission decisions.
        
        Args:
            bias: Integer in [-5, 5].
                  Positive = favor new items (admit more, faster adaptation)
                  Negative = favor existing items (reject more, preserve cache stability)
                  0 = standard TinyLFU behavior
        """
        if not (-5 <= bias <= 5):
            raise ValueError(f"admission_bias must be in [-5, 5], got {bias}")
        
        with self.lock:
            old_bias = self.admission_bias
            self.admission_bias = bias
        
        print(f"[TinyLFU] Admission bias updated: {old_bias} -> {bias}")
    
    def get_cache_fullness(self) -> float:
        """
        Get cache fullness percentage (0.0 to 1.0).
        """
        try:
            cache_info = self.redis.info("memory")
            used_memory = cache_info.get("used_memory", 0)
            max_memory = cache_info.get("maxmemory", 0)
            if max_memory > 0:
                return used_memory / max_memory
        except:
            pass
        
        with self.lock:
            return len(self.cache_order) / self.max_cache_size if self.max_cache_size > 0 else 0.0

    
    def _sample_eviction_candidates(self, sample_size: int) -> list:
        """
        Sample multiple items from cache for eviction comparison.
        Samples from the LRU end (oldest items) of the cache.
        
        Args:
            sample_size: Number of items to sample
            
        Returns:
            List of (key, timestamp) tuples from cache, sampled from LRU end
        """
        with self.lock:
            if not self.cache_order:
                return []
            
            items = list(self.cache_order.items())
            
            sampled = items[:min(sample_size, len(items))]
            
            return [(key, timestamp) for key, timestamp in sampled]
    
    def get_eviction_victim(self) -> Optional[str]:
        """
        Get victim key for eviction based on cache policy.

        For LRU cache integration, the eviction candidate is the LRU item.
        
        Returns:
            Key of the LRU victim, or None if cache is empty
        """
        with self.lock:
            if not self.cache_order:
                return None

            while self.cache_order:
                victim_key = next(iter(self.cache_order))
                if self.redis.exists(victim_key):
                    return victim_key
                self.cache_order.pop(victim_key, None)

            return None
    
    def get_eviction_candidate(self) -> Optional[str]:
        """
        Legacy method for backward compatibility.
        Now uses standard TinyLFU sampling.
        """
        return self.get_eviction_victim()
    
    def evict(self, key: str):
        """
        Remove key from cache and tracking.
        This is called when TinyLFU decides to evict a victim.
        """
        self.redis.delete(key)
        
        with self.lock:
            if key in self.cache_order:
                del self.cache_order[key]
            
            self.eviction_count += 1
            self.ghost_cache[key] = time.time()
            if len(self.ghost_cache) > self.ghost_cache_size:
                self.ghost_cache.popitem(last=False)
    
    def set(self, key: str, value: str, ttl: Optional[int] = None):
        """
        Set key-value in cache with optional TTL.
        Uses TinyLFU admission policy.
        
        Args:
            key: Cache key
            value: Cache value
            ttl: Optional TTL in seconds
        """
        if self.redis.exists(key):
            if ttl:
                self.redis.setex(key, ttl, value)
            else:
                self.redis.set(key, value)
            with self.lock:
                if key in self.cache_order:
                    self.cache_order.move_to_end(key)
                else:
                    if len(self.cache_order) >= self.max_cache_size:
                        self.cache_order.popitem(last=False)
                    self.cache_order[key] = time.time()
            return
        
        cache_full = False
        cache_fullness_pct = 0.0
        try:
            cache_info = self.redis.info("memory")
            used_memory = cache_info.get("used_memory", 0)
            max_memory = cache_info.get("maxmemory", 0)
            if max_memory > 0:
                cache_fullness_pct = (used_memory / max_memory) * 100
                cache_full = cache_fullness_pct > 90.0
        except:
            with self.lock:
                cache_full = len(self.cache_order) >= self.max_cache_size
                if self.max_cache_size > 0:
                    cache_fullness_pct = (len(self.cache_order) / self.max_cache_size) * 100
        
        victim = None
        if cache_full:
            victim = self.get_eviction_victim()
            if self.debug_logging:
                print(f"[TinyLFU set] Cache FULL ({cache_fullness_pct:.1f}%), checking admission for key={key[:20]}, victim={victim[:20] if victim else 'None'}")
        elif self.debug_logging:
            print(f"[TinyLFU set] Cache NOT full ({cache_fullness_pct:.1f}%), auto-admitting key={key[:20]}")
        
        if self.should_admit(key, victim):
            if victim and victim != key:
                if self.redis.exists(victim):
                    self.evict(victim)
                else:
                    with self.lock:
                        self.cache_order.pop(victim, None)
            
            try:
                if ttl:
                    self.redis.setex(key, ttl, value)
                else:
                    self.redis.set(key, value)
            except Exception as e:
                self._sync_cache_tracking()
                if not self.redis.exists(key):
                    return
            
            with self.lock:
                if len(self.cache_order) >= self.max_cache_size:
                    self.cache_order.popitem(last=False)
                self.cache_order[key] = time.time()
        else:
            pass
    
    def get(self, key: str) -> Optional[str]:
        """Get value from cache and update access frequency."""
        value = self.redis.get(key)
        if value:
            if isinstance(value, bytes):
                value = value.decode('utf-8')
            self.access(key)
        return value
    
    def exists(self, key: str) -> bool:
        """Check if key exists in cache and update frequency."""
        return self.access(key)
    
    def get_frequency(self, key: str) -> int:
        """Get estimated frequency of key (sketch + doorkeeper)."""
        estimate = self.sketch.estimate(key)
        if self.doorkeeper.contains(key):
            return estimate + 1
        return estimate
    
    def apply_decay(self, factor: float):
        """
        Apply decay factor to all Count-Min Sketch counters and reset the Doorkeeper.
        
        This creates a real difference from the Baseline:
        - Sketch counters are scaled down → old items have reduced frequency
        - Doorkeeper is cleared → all items get a "free pass" on next access
        - Items accessed AFTER decay rebuild their frequency from a lower base
        - Items NOT accessed after decay remain at reduced frequency
        
        Net effect: favors recently accessed items over old items.
        Lower factor = more aggressive recency bias.
        
        Args:
            factor: Decay factor in [0.0, 1.0]. Values < 1.0 reduce frequencies.
        """
        if not (0.0 <= factor <= 1.0):
            raise ValueError(f"decay_factor must be in [0.0, 1.0], got {factor}")
        
        with self.lock:
            for i in range(self.sketch.depth):
                for j in range(self.sketch.width):
                    self.sketch.counters[i][j] = int(self.sketch.counters[i][j] * factor)
            
            self.sketch.total_additions = int(self.sketch.total_additions * factor)
            self.doorkeeper.reset()
            self.access_count = 0
            
            if not hasattr(self, 'decay_factor_applied'):
                self.decay_factor_applied = None
            self.decay_factor_applied = factor
        
        TINYLFU_DECAY_APPLICATIONS.inc()
        print(f"[TinyLFU] Applied decay factor {factor}: sketch scaled, doorkeeper+access_count reset")
    
    def set_reset_interval(self, n: int):
        """
        Update the reset interval (sample size) for frequency data halving.
        
        Args:
            n: Number of sketch insertions before halving data (50000-500000)
        """
        if not (50000 <= n <= 500000):
            raise ValueError(f"reset_interval must be in [50000, 500000], got {n}")
        
        with self.lock:
            self.reset_interval = n
        
        TINYLFU_RESET_INTERVAL_UPDATES.inc()
        print(f"[TinyLFU] Reset interval updated to {n}")
    
    def force_reset_sketch(self):
        """
        Force a complete reset of the Count-Min Sketch and Doorkeeper.
        This completely zeros all frequency data, providing a fresh start.
        Useful when workload changes drastically or sketch is saturated with stale patterns.
        """
        with self.lock:
            self.sketch.reset()
            self.doorkeeper.reset()
            self.access_count = 0
            self.decay_factor_applied = None
        
        TINYLFU_RESETS.inc()
        print(f"[TinyLFU] Force reset: Count-Min Sketch and Doorkeeper completely zeroed")
    
    def enable_debug_logging(self, enable: bool = True):
        """Enable or disable detailed debug logging for admission decisions."""
        with self.lock:
            self.debug_logging = enable
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about TinyLFU state."""
        with self.lock:
            stats = {
                "access_count": self.access_count,
                "reset_interval": self.reset_interval,
                "tracked_items": len(self.cache_order),
                "sketch_total_additions": self.sketch.total_additions,
                "doorkeeper_size": self.doorkeeper.size,
                "eviction_count": self.eviction_count,
                "ghost_hits": self.ghost_hits,
                "admission_bias": self.admission_bias,
                "should_admit_calls": self.should_admit_calls,
                "should_admit_cache_full_calls": self.should_admit_cache_full_calls,
                "should_admit_rejections": self.should_admit_rejections
            }
            if hasattr(self, 'decay_factor_applied') and self.decay_factor_applied is not None:
                stats["decay_factor_applied"] = self.decay_factor_applied
            return stats

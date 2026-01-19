from threading import Lock
from collections import OrderedDict
from ..core.config import SEMANTIC_TTL_CACHE_SIZE, ITEM_CLUSTER_CACHE_SIZE
from ..core.db import get_redis_aura
from ..modules.data import get_item_metadata, get_item_embedding, get_milvus

_semantic_ttl_cache = OrderedDict()
_semantic_ttl_cache_lock = Lock()

_item_cluster_cache = {}
_item_cluster_cache_lock = Lock()

_r = None

def get_redis():
    global _r
    if _r is None:
        _r = get_redis_aura()
    return _r

def _get_category_brand_key(category, brand):
    cat_str = str(category) if category else "None"
    brand_str = str(brand) if brand else "None"
    return f"{cat_str}::{brand_str}"

def estimate_ttl_from_neighbors(item_id, k=5):
    try:
        doc = get_item_metadata(item_id, {"category": 1, "brand": 1})
        category = doc.get("category") if doc else None
        brand = doc.get("brand") if doc else None
        
        cache_key = _get_category_brand_key(category, brand)
        with _semantic_ttl_cache_lock:
            if cache_key in _semantic_ttl_cache:
                cached_ttl = _semantic_ttl_cache.pop(cache_key)
                _semantic_ttl_cache[cache_key] = cached_ttl
                return cached_ttl
        
        embedding = get_item_embedding(item_id)
        if embedding is None:
            return None
        
        milvus = get_milvus()
        if not milvus:
            return None

        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        results = milvus.search(
            data=[embedding],
            anns_field="embedding",
            param=search_params,
            limit=k+1,
            output_fields=["item_id"]
        )
        
        if not results or len(results) == 0:
            return None
        
        neighbor_ids = []
        for hit in results[0]:
            neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
            if neighbor_id is None and hasattr(hit, 'id'):
                neighbor_id = hit.id
            if neighbor_id is not None and neighbor_id != item_id:
                neighbor_ids.append(neighbor_id)
        
        if not neighbor_ids:
            return None
        
        r = get_redis()
        neighbor_ttls = []
        for neighbor_id in neighbor_ids:
            neighbor_key = f"item:{neighbor_id}"
            try:
                ttl = r.ttl(neighbor_key)
                if ttl > 0:
                    neighbor_ttls.append(ttl)
            except Exception:
                continue
        
        if neighbor_ttls:
            avg_ttl = int(sum(neighbor_ttls) / len(neighbor_ttls))
            
            with _semantic_ttl_cache_lock:
                if cache_key in _semantic_ttl_cache:
                    _semantic_ttl_cache.pop(cache_key)
                elif len(_semantic_ttl_cache) >= SEMANTIC_TTL_CACHE_SIZE:
                    _semantic_ttl_cache.popitem(last=False)
                _semantic_ttl_cache[cache_key] = avg_ttl
            
            return avg_ttl
    except Exception as e:
        print(f"[WARNING] Failed to estimate TTL from neighbors for item_id={item_id}: {e}")
    return None

def get_item_cluster(item_id):
    with _item_cluster_cache_lock:
        if item_id in _item_cluster_cache:
            return _item_cluster_cache[item_id]

    try:
        embedding = get_item_embedding(item_id)
        if embedding is None:
            try:
                doc = get_item_metadata(item_id, {"category": 1, "brand": 1})
                if doc:
                    cluster_id = _get_category_brand_key(doc.get("category"), doc.get("brand"))
                    with _item_cluster_cache_lock:
                        if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                            keys = list(_item_cluster_cache.keys())
                            if keys:
                                _item_cluster_cache.pop(keys[0], None)
                        _item_cluster_cache[item_id] = cluster_id
                    return cluster_id
            except:
                pass
            cluster_id = f"cluster_{hash(item_id) % 1000}"
            with _item_cluster_cache_lock:
                if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                    keys = list(_item_cluster_cache.keys())
                    if keys:
                        _item_cluster_cache.pop(keys[0], None)
                _item_cluster_cache[item_id] = cluster_id
            return cluster_id
        
        milvus = get_milvus()
        if not milvus:
            try:
                doc = get_item_metadata(item_id, {"category": 1, "brand": 1})
                cluster_id = _get_category_brand_key(doc.get("category") if doc else None, doc.get("brand") if doc else None)
                with _item_cluster_cache_lock:
                    if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                        keys = list(_item_cluster_cache.keys())
                        if keys:
                            _item_cluster_cache.pop(keys[0], None)
                    _item_cluster_cache[item_id] = cluster_id
                return cluster_id
            except:
                return f"cluster_{hash(item_id) % 1000}"

        search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
        results = milvus.search(
            data=[embedding],
            anns_field="embedding",
            param=search_params,
            limit=2,
            output_fields=["item_id"]
        )
        
        if results and len(results) > 0 and len(results[0]) > 0:
            for hit in results[0]:
                neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
                if neighbor_id is None and hasattr(hit, 'id'):
                    neighbor_id = hit.id
                if neighbor_id is not None and neighbor_id != item_id:
                    cluster_id = str(hash(neighbor_id) % 1000)
                    
                    with _item_cluster_cache_lock:
                        if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                            keys = list(_item_cluster_cache.keys())
                            if keys:
                                 _item_cluster_cache.pop(keys[0], None)
                        _item_cluster_cache[item_id] = cluster_id
                    
                    return cluster_id
        
        try:
            doc = get_item_metadata(item_id, {"category": 1, "brand": 1})
            cluster_id = _get_category_brand_key(doc.get("category") if doc else None, doc.get("brand") if doc else None)
            with _item_cluster_cache_lock:
                if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                    keys = list(_item_cluster_cache.keys())
                    if keys:
                        _item_cluster_cache.pop(keys[0], None)
                _item_cluster_cache[item_id] = cluster_id
            return cluster_id
        except:
            pass

        cluster_id = f"cluster_{hash(item_id) % 1000}"
        with _item_cluster_cache_lock:
            if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                keys = list(_item_cluster_cache.keys())
                if keys:
                    _item_cluster_cache.pop(keys[0], None)
            _item_cluster_cache[item_id] = cluster_id
        return cluster_id
        
    except Exception as e:
        cluster_id = f"cluster_{hash(item_id) % 1000}"
        with _item_cluster_cache_lock:
            if len(_item_cluster_cache) >= ITEM_CLUSTER_CACHE_SIZE:
                keys = list(_item_cluster_cache.keys())
                if keys:
                    _item_cluster_cache.pop(keys[0], None)
            _item_cluster_cache[item_id] = cluster_id
        return cluster_id

def compute_semantic_novelty(item_ids, k=5):
    if not item_ids:
        return None
    
    milvus = get_milvus()
    if not milvus:
        return None

    novelty_scores = []
    for item_id in item_ids:
        try:
            embedding = get_item_embedding(item_id)
            if embedding is None:
                continue
            
            search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            results = milvus.search(
                data=[embedding],
                anns_field="embedding",
                param=search_params,
                limit=k+1,
                output_fields=["item_id"]
            )
            
            if not results or len(results) == 0:
                continue
            
            max_similarity = 0.0
            for hit in results[0]:
                neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
                if neighbor_id is None and hasattr(hit, 'id'):
                    neighbor_id = hit.id
                if neighbor_id is not None and neighbor_id != item_id:
                    distance = hit.distance
                    similarity = 1.0 / (1.0 + distance)
                    max_similarity = max(max_similarity, similarity)
                    break
            
            if max_similarity > 0:
                novelty = 1.0 - max_similarity
                novelty_scores.append(novelty)
        except Exception as e:
            print(f"[WARNING] Failed to compute novelty for item_id={item_id}: {e}")
            continue
    
    if novelty_scores:
        return sum(novelty_scores) / len(novelty_scores)
    return None

def compute_semantic_redundancy(item_ids, k=5):
    if not item_ids:
        return None

    milvus = get_milvus()
    if not milvus:
        return None

    redundancy_scores = []
    for item_id in item_ids:
        try:
            embedding = get_item_embedding(item_id)
            if embedding is None:
                continue
            
            search_params = {"metric_type": "L2", "params": {"nprobe": 10}}
            results = milvus.search(
                data=[embedding],
                anns_field="embedding",
                param=search_params,
                limit=k+1,
                output_fields=["item_id"]
            )
            
            if not results or len(results) == 0:
                continue
            
            max_similarity = 0.0
            for hit in results[0]:
                neighbor_id = hit.entity.get("item_id") if hasattr(hit, 'entity') and hit.entity else None
                if neighbor_id is None and hasattr(hit, 'id'):
                    neighbor_id = hit.id
                if neighbor_id != item_id:
                    distance = hit.distance
                    similarity = 1.0 / (1.0 + distance)
                    max_similarity = max(max_similarity, similarity)
                    break
            
            if max_similarity > 0:
                redundancy_scores.append(max_similarity)
        except Exception as e:
            print(f"[WARNING] Failed to compute redundancy for item_id={item_id}: {e}")
            continue
    
    if redundancy_scores:
        return sum(redundancy_scores) / len(redundancy_scores)
    return None

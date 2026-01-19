from threading import Lock
from ..core.db import get_mongo_collection, get_milvus_collection
from ..core.config import ITEM_METADATA_CACHE_SIZE, CATEGORY_CACHE_SIZE

_item_metadata_cache = {}
_item_metadata_cache_lock = Lock()

_category_cache = {}
_category_cache_lock = Lock()

_mongo_collection = None
_milvus_collection = None

def get_mongo():
    global _mongo_collection
    if _mongo_collection is None:
        _mongo_collection = get_mongo_collection()
    return _mongo_collection

def get_milvus():
    global _milvus_collection
    if _milvus_collection is None:
        _milvus_collection = get_milvus_collection()
    return _milvus_collection

def get_item_metadata(item_id, fields=None):
    global _item_metadata_cache
    if fields is None:
        fields = {"category": 1}

    field_keys = tuple(sorted(fields.keys()))
    cache_key = (item_id, field_keys)

    with _item_metadata_cache_lock:
        if cache_key in _item_metadata_cache:
            return _item_metadata_cache[cache_key]

    mongo = get_mongo()
    if mongo is None:
        return {}

    try:
        doc = mongo.find_one({"item_id": item_id}, fields)
        result = doc if doc else {}

        with _item_metadata_cache_lock:
            if len(_item_metadata_cache) >= ITEM_METADATA_CACHE_SIZE:
                first_key = next(iter(_item_metadata_cache))
                del _item_metadata_cache[first_key]

            _item_metadata_cache[cache_key] = result

        if "category" in fields and "category" in result:
            with _category_cache_lock:
                if len(_category_cache) >= CATEGORY_CACHE_SIZE:
                     first_k = next(iter(_category_cache))
                     del _category_cache[first_k]
                _category_cache[item_id] = result.get("category")
        
        return result
    except Exception as e:
        print(f"[WARNING] MongoDB query failed for item_id={item_id}: {e}")
        return {}

def get_item_category_cached(item_id):
    with _category_cache_lock:
        if item_id in _category_cache:
            return _category_cache[item_id]

    doc = get_item_metadata(item_id, {"category": 1})
    category = doc.get("category") if doc else None

    if category:
        with _category_cache_lock:
            if len(_category_cache) >= CATEGORY_CACHE_SIZE:
                first_k = next(iter(_category_cache))
                del _category_cache[first_k]
            _category_cache[item_id] = category
    
    return category

def get_item_embedding(item_id):
    milvus = get_milvus()
    if milvus is None:
        return None
    try:
        results = milvus.query(
            expr=f"item_id == {item_id}",
            output_fields=["embedding"],
            limit=1
        )
        if results and len(results) > 0:
            embedding = results[0].get("embedding")
            if embedding is not None:
                return embedding
    except Exception as e:
        print(f"[WARNING] Failed to get embedding for item_id={item_id}: {e}")
    return None

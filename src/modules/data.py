from threading import Lock
from ..core.db import get_mongo_collection
from ..core.config import ITEM_METADATA_CACHE_SIZE

_item_metadata_cache = {}
_item_metadata_cache_lock = Lock()

_mongo_collection = None

def get_mongo():
    global _mongo_collection
    if _mongo_collection is None:
        _mongo_collection = get_mongo_collection()
    return _mongo_collection


def get_item_metadata(item_id, fields=None):
    global _item_metadata_cache
    if fields is None:
        fields = {}

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
        
        return result
    except Exception as e:
        print(f"[WARNING] MongoDB query failed for item_id={item_id}: {e}")
        return {}


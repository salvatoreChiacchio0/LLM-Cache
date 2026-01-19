import redis
import pymongo
from confluent_kafka import Producer, Consumer
from pymilvus import Collection, connections, MilvusException
from .config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC_PLAN,
    KAFKA_TOPIC_STATS
)
import time

CHARSET = "utf-8"

def get_redis_aura():
    return redis.Redis(host="redis-aura", port=6379, encoding=CHARSET, protocol=2)

def get_redis_lru():
    return redis.Redis(host="redis-lru", port=6380, encoding=CHARSET, protocol=2)

def get_mongo_collection():
    print("[DEBUG] Connecting to MongoDB...")
    try:
        client = pymongo.MongoClient("mongodb://mongo:27017")
        print("[DEBUG] MongoDB connected")
        return client.aura.catalog
    except Exception as e:
        print(f"[ERROR] Failed to connect to MongoDB: {e}")
        return None

def get_mongodb():
    print("[DEBUG] Connecting to MongoDB (DB instance)...")
    try:
        client = pymongo.MongoClient("mongodb://mongo:27017", serverSelectionTimeoutMS=2000)
        client.server_info()
        print("[DEBUG] MongoDB connected")
        return client.aura
    except Exception as e:
        print(f"[ERROR] Failed to connect to MongoDB: {e}")
        return None

def get_milvus_collection(collection_name="catalog"):
    print("[DEBUG] Connecting to Milvus...")
    try:
        connections.connect("default", host="milvus", port=19530)
        print("[DEBUG] Milvus connected")
        coll = Collection(collection_name)
        coll.load()
        print(f"[DEBUG] Milvus collection '{collection_name}' loaded")
        return coll
    except MilvusException as e:
        print(f"[ERROR] Failed to load Milvus collection: {e}")
        return None

def get_kafka_producer(client_id='llm-producer'):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS, 
        'client.id': client_id,
        'queue.buffering.max.messages': 100000,
        'queue.buffering.max.kbytes': 1048576,
        'batch.num.messages': 100,
        'linger.ms': 10,
        'compression.type': 'none',
        'acks': 'all',
        'retries': 3,
        'max.in.flight.requests.per.connection': 1,
        'enable.idempotence': True,
    }
    return Producer(conf)

def get_kafka_consumer(group_id, topics, auto_offset_reset='latest'):
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset,
    }
    consumer = Consumer(conf)
    consumer.subscribe(topics)
    return consumer

import csv, pymongo, pymilvus, time
from sentence_transformers import SentenceTransformer
from pymilvus import Collection, FieldSchema, CollectionSchema, DataType, connections, utility
from pymilvus.exceptions import MilvusException


DELIMITER = chr(1) 
FILE_ENCODING = 'utf-8'
BATCH_SIZE = 20000 
CAT_FILE = "../data/rec_tmall_product.txt"
total_items_target = 8133507 

TEST_LIMIT  = 813350
print("Connessione ai servizi Docker...")
try:
  connections.connect("default", host="localhost", port="19530")
  mongo = pymongo.MongoClient("mongodb://localhost:27017").aura
except Exception as e:
  print(f"ERRORE CRITICO DI CONNESSIONE: {e}. I container sono attivi?")
  exit()

model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu') 

print("Configurazione MongoDB e Milvus Catalog...")
mongo.catalog.drop() 
mongo.catalog.create_index("item_id", unique=True) 

if utility.has_collection("catalog"): utility.drop_collection("catalog")

fields = [
  FieldSchema(name="item_id", dtype=DataType.INT64, is_primary=True),
  FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=384)
]
coll_schema = CollectionSchema(fields, "Tmall Product Catalog")
coll = Collection("catalog", coll_schema)

print(f"Avvio Ingestione e Embedding di {total_items_target:,} righe. Stima: 15-24 ore.")
t_start = time.time()

print("Apertura file:", CAT_FILE)
with open(CAT_FILE, 'r', encoding=FILE_ENCODING, errors='ignore') as f:
  print("File aperto, leggo riga 1…")
  try:
    print(next(csv.reader(f, delimiter=DELIMITER)))
  except Exception as e:
    print(f"Errore nella lettura della prima riga: {e}. Il file è vuoto o corrotto?")
  
  cat_reader = csv.reader(f, delimiter=DELIMITER) 
  try:
    next(cat_reader) 
  except:
    pass 
  
  batch_mongo = []
  batch_milvus = []
  count = 0
  
  for row in cat_reader:
    if len(row) < 6: continue
    
    item_id, title, _, cat, brand, _ = row
    
    try:
      title = title.strip()
      
      emb = model.encode(title).astype("float32").tolist() 
      item_id_int = int(item_id)
    except Exception:
      continue 

    batch_mongo.append({"_id": item_id_int, "item_id": item_id_int, "title": title, "category": cat, "brand": brand})
    batch_milvus.append([item_id_int, emb])

    count += 1
    
    if TEST_LIMIT > 0 and count >= TEST_LIMIT:
      print(f"\n[ATTENZIONE] Raggiunto limite di test: {TEST_LIMIT} prodotti. Eseguo flush finale.")
      break
    
    if len(batch_mongo) >= BATCH_SIZE:
      try:
        mongo.catalog.insert_many(batch_mongo)

        milvus_data = [
          [item[0] for item in batch_milvus], 
          [item[1] for item in batch_milvus] 
        ]
        coll.insert(milvus_data)

        t_elapsed = time.time() - t_start
        rate = count / t_elapsed
        t_remaining = (total_items_target - count) / rate / 3600
        
        print(f"[{time.strftime('%H:%M:%S')}] Inseriti {len(batch_mongo):,} nuovi. Totale: {count:,} items. Rate: {rate:.1f} i/s. Restano stimati: {t_remaining:.1f} ore.   \r", end="")
        
        batch_mongo = []
        batch_milvus = []
        
      except MilvusException as e:
        print(f"\n[ERRORE MILVUS BATCH]: {type(e).__name__}: {e}. Svuoto il batch per continuare...")
        batch_mongo = []
        batch_milvus = []
      except Exception as e:
        print(f"\n[ERRORE GENERICO DB BATCH]: {type(e).__name__}: {e}. Svuoto il batch per continuare...")
        batch_mongo = []
        batch_milvus = []
        
if batch_mongo: 
    print(f"\nFinal Flush MongoDB: {len(batch_mongo)} items.")
    try:
        mongo.catalog.insert_many(batch_mongo)
    except:
        pass
    
if batch_milvus: 
    print(f"Final Flush Milvus: {len(batch_milvus)} items.")
    final_milvus_data = [
        [item[0] for item in batch_milvus], 
        [item[1] for item in batch_milvus]
    ]
    coll.insert(final_milvus_data)
print("Forzo il flush di Milvus (scrittura definitiva su MinIO)...")
coll.flush(
coll.create_index("embedding", {"index_type":"IVF_SQ8", "metric_type":"L2", "params":{"nlist":1024}})
coll.load()print(f"Totale Entità: {coll.num_entities:,}")

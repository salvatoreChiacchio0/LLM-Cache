
import pymilvus, time
from pymilvus import Collection, connections
from pymilvus.exceptions import MilvusException

print("Connessione a Milvus...")
try:
  connections.connect("default", host="localhost", port="19530")
except Exception as e:
  print(f"ERRORE CRITICO DI CONNESSIONE: {e}. Il container è attivo?")
  exit()

coll = Collection("catalog")

print("\nCreazione/Verifica Indice Milvus...")
try:
    if not coll.has_index():
        coll.create_index("embedding", {"index_type":"IVF_SQ8", "metric_type":"L2", "params":{"nlist":1024}})
        print("Indice creato. Attendo il caricamento...")
    else:
        print("Indice esistente, procedo al caricamento.")

    coll.load()
    print("Caricamento della Collection in memoria completato.")

    coll.compact()
    print(f"Totale Entità: {coll.num_entities:,}")

except MilvusException as e:
    print(f"\n[ERRORE MILVUS]: {e}")
    print("Verifica lo stato del container Milvus e riprova.")
except Exception as e:
    print(f"\n[ERRORE GENERICO]: {e}")

print("=====================================================================")
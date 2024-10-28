import random
import time
from tqdm import tqdm
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')  # Ajusta la URL de conexión si es necesario
db = client['test_orange']
collection = db['dialog']

# Función para medir el tiempo de lectura de documentos en bloque
def leer_documentos_bulk(ids):
    start_time = time.time()
    
    # Usar find() con $in para obtener todos los documentos con esos ids en una sola consulta
    # Aquí no se puede mostrar barra de progreso directamente porque estamos leyendo en bloque
    documentos = list(collection.find({"_id": {"$in": ids}}))
    
    end_time = time.time()
    
    print(f"Documentos encontrados: {len(documentos)}")
    
    return end_time - start_time

# Obtener una lista de 100 _id aleatorios de la base de datos
document_ids = collection.distinct("_id")  # Lista de todos los _id en la colección
random_ids = random.sample(document_ids, 100)  # Seleccionar 100 _id aleatorios

# Medir el tiempo de lectura de los documentos en bloque con barra de progreso
with tqdm(total=1, desc="Leyendo documentos en bloque", unit="consulta") as pbar:
    tiempo_lectura_bulk = leer_documentos_bulk(random_ids)
    pbar.update(1)  # Actualizar la barra de progreso

# Resultados
print(f"Tiempo total de lectura de 100 documentos en bloque: {tiempo_lectura_bulk:.4f} segundos")

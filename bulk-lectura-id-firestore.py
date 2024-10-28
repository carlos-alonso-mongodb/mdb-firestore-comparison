import random
import time
from google.cloud import firestore
from tqdm import tqdm

# Inicializar Firestore
db = firestore.Client()

# Función para medir el tiempo de lectura de documentos en bloque
def leer_documentos_bulk(ids):
    start_time = time.time()
    
    # Crear una lista para almacenar los documentos leídos
    documentos = []
    
    # Leer cada documento por su ID
    for doc_id in tqdm(ids, desc="Leyendo documentos"):
        doc_ref = db.collection('dialog').document(doc_id)
        doc = doc_ref.get()
        if doc.exists:
            documentos.append(doc.to_dict())
    
    end_time = time.time()
    
    print(f"Documentos encontrados: {len(documentos)}")
    
    return end_time - start_time

# Obtener una lista de 100 _id aleatorios de la base de datos
# Suponiendo que ya tienes una lista de IDs disponibles
# Por ejemplo, usando 'distinct' como en MongoDB
# Aquí deberías implementar la lógica para obtener los IDs de Firestore si es necesario
# For example, replace this line with the logic to fetch document IDs from Firestore
document_ids = [doc.id for doc in db.collection('dialog').stream()]  # Obtener todos los IDs
random_ids = random.sample(document_ids, 100)  # Seleccionar 100 _id aleatorios

# Medir el tiempo de lectura de los documentos en bloque
tiempo_lectura_bulk = leer_documentos_bulk(random_ids)

# Resultados
print(f"Tiempo total de lectura de 100 documentos en bloque: {tiempo_lectura_bulk:.4f} segundos")

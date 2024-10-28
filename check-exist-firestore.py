import time
from google.cloud import firestore

# Inicializar Firestore
db = firestore.Client()

# Iniciar el cronómetro
start_time = time.time()

# Hacer una consulta para obtener todos los documentos en la colección 'dialog'
docs = db.collection('dialog').stream()

# Contar los documentos que tienen el campo 'coordenadas' en 'clientdata'
count = sum(1 for doc in docs if 'coordenadas' in doc.to_dict().get('clientdata', {}))

# Calcular el tiempo transcurrido
elapsed_time = time.time() - start_time

print(f"Número de documentos que contienen el campo 'coordenadas' en 'clientdata': {count}")
print(f"Tiempo para contar documentos en Firestore: {elapsed_time:.6f} segundos")

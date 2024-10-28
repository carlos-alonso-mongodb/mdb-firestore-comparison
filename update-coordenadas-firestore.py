import random
import time
from google.cloud import firestore

# Inicializar Firestore
db = firestore.Client()

# Generar coordenadas aleatorias dentro de España
def generar_coordenadas_en_espana():
    latitud = round(random.uniform(36.0, 43.0), 6)   # Latitud entre 36.0 y 43.0
    longitud = round(random.uniform(-9.5, 3.5), 6)  # Longitud entre -9.5 y 3.5
    return {'latitud': latitud, 'longitud': longitud}

# Medir el tiempo total de la operación
start_time = time.time()

# Recorrer los primeros 10,000 documentos y añadir el campo 'coordenadas'
docs = db.collection('dialog').limit(10000).stream()  # Recupera los primeros 10,000 documentos

# Acumular operaciones de actualización
batch = db.batch()  # Crear un batch para operaciones
batch_size = 1000  # Tamaño del batch
operation_count = 0  # Contador de operaciones

for doc in docs:
    coordenadas = generar_coordenadas_en_espana()
    
    # Preparar la operación de actualización
    batch.update(doc.reference, {
        'clientdata.coordenadas': coordenadas
    })
    operation_count += 1  # Incrementar el contador

    # Ejecutar el batch cada 500 operaciones
    if operation_count % batch_size == 0:
        batch.commit()  # Ejecuta las operaciones en batch
        print(f'Actualizados {operation_count} documentos con coordenadas aleatorias dentro de España.')
        batch = db.batch()  # Reiniciar el batch

# Ejecutar cualquier operación restante
if operation_count % batch_size > 0:
    batch.commit()
    print(f'Actualizados {operation_count} documentos con coordenadas aleatorias dentro de España.')

# Medir el tiempo total de la operación
end_time = time.time()
elapsed_time = end_time - start_time

print(f'Proceso completado. Se han actualizado {operation_count} documentos con coordenadas en España.')
print(f'Tiempo total de la operación: {elapsed_time:.4f} segundos.')

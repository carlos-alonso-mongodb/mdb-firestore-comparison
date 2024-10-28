from google.cloud import firestore
import time

# Inicializar Firestore
db = firestore.Client()

# Medir el tiempo de borrado
start_time = time.time()

# Crear un batch para las operaciones de borrado
batch = db.batch()
count = 0

# Consultar documentos con status='closed'
docs_to_delete = db.collection('dialog').where('status', '==', 'closed').stream()

for doc in docs_to_delete:
    batch.delete(doc.reference)
    count += 1

    # Ejecutar el lote si hemos alcanzado el límite de 500 operaciones
    if count % 500 == 0:
        batch.commit()
        batch = db.batch()  # Reiniciar el lote

# Ejecutar el último lote si hay operaciones pendientes
if count % 500 != 0:
    batch.commit()

end_time = time.time()

# Mostrar resultados
print(f"Documentos con status 'closed' han sido borrados: {count}")
print(f"Tiempo de borrado en Firestore: {end_time - start_time:.6f} segundos")

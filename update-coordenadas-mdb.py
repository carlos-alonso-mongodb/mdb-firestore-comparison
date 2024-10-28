import random
import time
from pymongo import MongoClient, UpdateOne

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')  # Ajusta la URL de conexión si es necesario
db = client['test_orange']
collection = db['dialog']

# Generar coordenadas aleatorias dentro de España
def generar_coordenadas_en_espana():
    latitud = round(random.uniform(36.0, 43.0), 6)   # Latitud entre 36.0 y 43.0
    longitud = round(random.uniform(-9.5, 3.5), 6)  # Longitud entre -9.5 y 3.5
    return {'latitud': latitud, 'longitud': longitud}

# Acumular operaciones de actualización
operaciones = []

# Medir el tiempo total de la operación
start_time = time.time()

# Recorrer los primeros 10,000 documentos y añadir el campo 'coordenadas'
for doc in collection.find({}, {"_id": 1}).limit(10000):  # Solo obtener el _id para minimizar el uso de memoria
    coordenadas = generar_coordenadas_en_espana()
    
    # Preparar la operación de actualización en lugar de ejecutarla inmediatamente
    operaciones.append(UpdateOne(
        {'_id': doc['_id']},
        {'$set': {'clientdata.coordenadas': coordenadas}}
    ))
    
    # Ejecutar el batch cada 1000 operaciones
    if len(operaciones) >= 1000:
        collection.bulk_write(operaciones)
        print(f'Actualizados {len(operaciones)} documentos con coordenadas aleatorias dentro de España.')
        operaciones = []  # Reiniciar la lista de operaciones

# Ejecutar cualquier operación restante
if operaciones:
    collection.bulk_write(operaciones)
    print(f'Actualizados {len(operaciones)} documentos con coordenadas aleatorias dentro de España.')

# Medir el tiempo total de la operación
end_time = time.time()
elapsed_time = end_time - start_time

print(f'Proceso completado. Se han actualizado 10,000 documentos con coordenadas en España.')
print(f'Tiempo total de la operación: {elapsed_time:.4f} segundos.')

from pymongo import MongoClient
from bson import ObjectId
import time

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')

# Definir el ID del documento y la acción que buscamos
document_id = ObjectId('6718c76739ab0dc7ed875de5')
action_name = 'bloquear.tarjeta'

# Filtro para buscar el documento
filter = {
    '_id': document_id, 
    'topics.messages.output.actions.name': action_name
}

# Proyección para limitar los campos devueltos
project = {
    'topics.messages.output.actions.name': 1,
    'topics.topic': 1,
    'topics.messages.id': 1  # Solo queremos los mensajes
}

# Obtener el documento
start_time = time.time()
result = client['test_orange']['dialog'].find(filter=filter, projection=project)
end_time = time.time()
print(f"Tiempo para la búsqueda: {end_time - start_time:.6f} segundos")

print(result)

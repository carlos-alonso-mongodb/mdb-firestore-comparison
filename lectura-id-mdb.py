import time
from pymongo import MongoClient
from bson.objectid import ObjectId

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')
db = client['test_orange']
collection = db['dialog']

# ID del documento que queremos buscar
document_id = "6718c76739ab0dc7ed875de4"  # Reemplaza por el _id que quieras buscar
document_id2 = "6718c76739ab0dc7ed875de7"  # Reemplaza por el _id que quieras buscar

doc2 = collection.find_one({'_id': ObjectId(document_id2)})  # Buscar por _id usando find_one
doc2
# Medir el tiempo de búsqueda
start_time = time.time()
doc = collection.find_one({'_id': ObjectId(document_id)},{'conversation':1})  # Buscar por _id usando find_one
end_time = time.time()

if doc:
    # Obtener el id del documento y el id de la conversación (si existe)
    document_id = doc['_id']
    conversation_id = doc.get('conversation', 'Sin conversación')
    
    # Mostrar los ids
    print(f"Document ID: {document_id}")
    print(f"Conversation ID: {conversation_id}")
else:
    print(f"No se encontró el documento con _id: {document_id}")

# Imprimir el tiempo que tomó la búsqueda
print(f"Tiempo para la consulta por _id: {end_time - start_time:.6f} segundos")
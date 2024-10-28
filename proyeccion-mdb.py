import time
from pymongo import MongoClient

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')  # Ajusta la URL de conexión si es necesario
db = client['test_orange']
collection = db['dialog']   # Cambia por el nombre de tu colección

# Iniciar el cronómetro
start_time = time.time()

# Consulta para obtener los documentos que tienen acciones en los mensajes
results = collection.find({"topics.messages.output.actions": {"$exists": True}}, 
                          {"_id": 1, "topics.messages.output.actions": 1})

# Mostrar los resultados
for doc in results:
    print(f"Document ID: {doc['_id']}, Actions: {doc['topics'][0]['messages'][0]['output']['actions']}")

# Calcular el tiempo transcurrido
elapsed_time = time.time() - start_time
print(f"Tiempo para realizar la consulta en MongoDB: {elapsed_time:.6f} segundos")

from pymongo import MongoClient
import time

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')  # Ajusta la URL de conexión si es necesario
db = client['test_orange']
collection = db['dialog']  # Cambia por el nombre de tu colección

# Iniciar el cronómetro
start_time = time.time()

# Contar documentos que contienen el campo 'coordenadas'
count = collection.count_documents({'clientdata.coordenadas': {'$exists': True}})

# Calcular el tiempo transcurrido
elapsed_time = time.time() - start_time

print(f"Número de documentos que contienen el campo 'coordenadas' en MongoDB: {count}")
print(f"Tiempo para contar documentos en MongoDB: {elapsed_time:.6f} segundos")


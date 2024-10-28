from pymongo import MongoClient
import time

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')
db = client['test_orange']
collection = db['dialog']

# Criterio de borrado (ajusta según tus necesidades)
filter_criteria = {'status': 'closed'}

# Medir el tiempo de borrado
start_time = time.time()

# Realizar el borrado
result = collection.delete_many(filter_criteria)

end_time = time.time()

# Mostrar resultados
print(f"Documentos borrados: {result.deleted_count}")
print(f"Tiempo de borrado en MongoDB: {end_time - start_time:.6f} segundos")

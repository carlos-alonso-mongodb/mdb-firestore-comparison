from pymongo import MongoClient
from bson import ObjectId
from pprint import pprint

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')

# Definir el ID del documento y la acción que buscamos
document_id = ObjectId('67182b2bfd3fd1b160e45d17')
action_name = 'bloquear.tarjeta'

# Pipeline de agregación
pipeline = [
    {
        '$match': {'_id': document_id}  # Filtrar por ID del documento
    },
    {
        '$unwind': '$topics'  # Descomponer el array de topics
    },
    {
        '$unwind': '$topics.messages'  # Descomponer el array de messages
    },
    {
        '$unwind': '$topics.messages.output.actions'  # Descomponer el array de actions dentro de messages
    },
    {
        '$match': {
            'topics.messages.output.actions.name': action_name  # Filtrar por la acción 'bloquear.tarjeta'
        }
    },
    {
        '$project': {
            '_id': 0,  # No mostrar el ID del documento
            'topic': '$topics.topic',  # Mostrar solo el campo 'topic' de 'topics'
            'message_id': '$topics.messages.id',  # Mostrar solo el campo 'id' de 'messages'
            'action': '$topics.messages.output.actions'  # Mostrar toda la información de la acción
        }
    }
]

# Realizar la agregación
result = list(client['test_orange']['dialog'].aggregate(pipeline))

# Mostrar el resultado con pprint
if result:
    print("Datos filtrados (topic, message_id, action):")
    pprint(result)
else:
    print("No se encontraron mensajes con la acción 'bloquear.tarjeta'.")

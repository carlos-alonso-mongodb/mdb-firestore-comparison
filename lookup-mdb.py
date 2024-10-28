from pymongo import MongoClient
from pprint import pprint

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')

# Seleccionamos la base de datos y la colección dialog
db = client['test_orange']
dialog_collection = db['dialog']

# Agregación usando $lookup para unir las colecciones
pipeline = [
    {
        '$unwind': '$topics'  # Descomponer el array 'topics'
    },
    {
        '$unwind': '$topics.messages'  # Descomponer el array 'messages' dentro de 'topics'
    },
    {
        '$lookup': {
            'from': 'token_count',  # La colección a unir
            'localField': 'topics.messages.id',  # Campo local en dialog
            'foreignField': 'message_id',  # Campo en token_count
            'as': 'token_data'  # Nombre del nuevo campo que contendrá los datos de token_count
        }
    },
    {
        '$unwind': {
            'path': '$token_data',  # Descomponer el array 'token_data'
            'preserveNullAndEmptyArrays': True  # Mantener el documento si no hay coincidencias
        }
    },
    {
        '$project': {
            'message_id': '$topics.messages.id',
            'message_content': '$topics.messages.output.actions',  # Incluye el contenido del mensaje
            'timestamp': '$token_data.timestamp',
            'groupId': '$token_data.groupId',
            'env': '$token_data.env',
            'system': '$token_data.system',
            'type': '$token_data.type',
            'client': '$token_data.client',
            'initiator': '$token_data.initiator',
            'total_tokens': '$token_data.total_tokens',
            'prompt_tokens': '$token_data.prompt_tokens',
            'completion_tokens': '$token_data.completion_tokens'
        }
    },
    {
        '$limit': 10  # Limitar los resultados a 10 documentos
    }
]

# Ejecutar la agregación
result = dialog_collection.aggregate(pipeline)

# Imprimir los resultados de forma bonita
for doc in result:
    # Crear una salida estructurada para mostrar los datos de manera clara
    output = {
        "Message ID": doc['message_id'],
        "Message Content": doc['message_content'],
        "Token Data": {
            "Timestamp": doc['timestamp'],
            "Group ID": doc['groupId'],
            "Environment": doc['env'],
            "System": doc['system'],
            "Type": doc['type'],
            "Client": doc['client'],
            "Initiator": doc['initiator'],
            "Total Tokens": doc['total_tokens'],
            "Prompt Tokens": doc['prompt_tokens'],
            "Completion Tokens": doc['completion_tokens']
        }
    }
    pprint(output)  # Utilizar pprint para una salida más legible

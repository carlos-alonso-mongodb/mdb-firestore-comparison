import random
import time
from pymongo import MongoClient
from faker import Faker
from multiprocessing import Pool, cpu_count
from tqdm import tqdm

# Conexión a MongoDB
client = MongoClient('mongodb+srv://carlosalonso:BIOPB3nF8riaxlBa@cluster-demo.rcm35.mongodb.net/?retryWrites=true&w=majority')  # Ajusta la URL de conexión si es necesario
db = client['test_orange']
collection = db['dialog']

# Inicialización de Faker para datos falsos en español
faker = Faker('es_ES')

# Lista de posibles valores para algunos campos
channels = ['wsp', 'sms', 'email', 'callcenter']
brands = ['orange', 'vodafone', 'movistar']
segments = ['res', 'soho', 'pyme']
intentions = ['CU016', 'CU013', 'CU039', 'CU010', 'CU020']
skills = ['WhatsappAgente', 'CallCenterAgent', 'EmailSupportAgent']

# Función para generar un documento
def generar_documento():
    timestamp_actual = int(time.time() * 1000)
    msisdn = faker.phone_number()
    
    # Definir listas de mensajes de entrada y salida para la simulación de la conversación
    input_texts = [
        "Hola, necesito consultar mi saldo.",
        "Quiero activar el roaming para mi viaje.",
        "Me gustaría bloquear mi tarjeta SIM.",
        "No puedo acceder a mi cuenta, ¿pueden ayudarme?",
        "¿Cuáles son los planes que ofrecen?"
    ]

    output_texts = [
        "Claro, puedo ayudarte con eso. Tu saldo actual es de 20 euros.",
        "Activaré el roaming para ti. Un momento, por favor.",
        "He bloqueado tu tarjeta SIM como pediste. ¿Necesitas algo más?",
        "Entiendo, puedo restablecer tu contraseña. ¿Cuál es tu número de teléfono?",
        "Actualmente ofrecemos varios planes. Te enviaré la información a tu correo."
    ]

    # Definir acciones posibles
    actions_options = [
        {"name": "consultar.saldo", "status": 200},
        {"name": "activar.roaming", "status": 200},
        {"name": "bloquear.tarjeta", "status": 200},
        {"name": "restablecer.contraseña", "status": 200},
        {"name": "enviar.informacion.plans", "status": 200}
    ]

    documento = {
        "conversation": faker.uuid4(),
        "initiator": random.choice(["user", "agent"]),
        "status": random.choice(["open", "closed"]),
        "open_ts": timestamp_actual,
        "msisdn": msisdn,
        "origin": {
            "channel": random.choice(channels),
            "brand": random.choice(brands),
            "segment": random.sample(segments, k=random.randint(1, 3)),
            "number": faker.phone_number(),
            "currentskill": random.choice(skills)
        },
        "clientdata": {
            "nombre": faker.first_name(),
            "apellidos": faker.last_name(),
            "brand": random.choice(brands),
            "segmentoCliente": random.choice(segments),
            "numDocumento": faker.ssn(),
            "esVIP": random.choice(["Sí", "No"]),
            "CCGG_WSP": random.choice(["Aceptada", "Rechazada"]),
            "codigoPostal": faker.postcode(),
            "esCliente": random.choice([True, False]),
            "msisdn": msisdn,
            "numAbonado": faker.msisdn(),
            "codPais": "34"
        },
        "intentionDetector": {
            "id": faker.uuid4(),
            "params": {}
        },
        "topics": [
            {
                "topic": faker.uuid4(),
                "status": random.choice(["open", "closed"]),
                "open_ts": timestamp_actual,
                "close_ts": timestamp_actual + random.randint(10000, 100000),
                "intention": random.choice(intentions),
                "CdU": random.choice(intentions) + "." + faker.numerify("#.##"),
                "intentionDetectorResp": {
                    "result": "CdU",
                    "candidates": [
                        {"CdU": random.choice(intentions), "intention": random.choice(intentions), "chunk": "Chunk1", "confidence": round(random.uniform(0.8, 1.0), 3)},
                        {"CdU": random.choice(intentions), "intention": random.choice(intentions), "chunk": "Chunk2", "confidence": round(random.uniform(0.7, 0.9), 3)}
                    ],
                    "mainIntention": random.choice(intentions),
                    "mainCdU": random.choice(intentions) + "." + faker.numerify("##"),
                    "inputEmbedding": [random.uniform(-0.05, 0.05) for _ in range(10)]
                },
                "assistant": {
                    "mainIntention": random.choice(intentions),
                    "mainCdU": random.choice(intentions) + "." + faker.numerify("#.#"),
                    "dialogAssistant": {
                        "id": random.choice(["DialogFlow", "Monyca"]),
                        "params": {}
                    }
                },
                "messages": []
            }
        ]
    }

    # Generar tres mensajes y agregarlos al documento
    for _ in range(3):
        input_text = random.choice(input_texts)
        output_text = random.choice(output_texts)
        action = random.choice(actions_options)  # Elegir una acción aleatoria

        mensaje = {
            "id": faker.uuid4(),
            "input": {
                "ts": timestamp_actual,
                "text": input_text,
                "text_esp": input_text,
                "lang": "esp",
                "files": [
                    {"path": f"gs://ruta/al/fichero/{faker.uuid4()}.opus", "contentType": "audio/opus"}
                ]
            },
            "output": {
                "ts": timestamp_actual + random.randint(1000, 5000),
                "type": "response",
                "output": [{"type": "text", "text": output_text}],
                "output_esp": [{"type": "text", "text": output_text}],
                "actions": [action],
                "typification": {
                    "intention": random.choice(intentions),
                    "CdU": random.choice(intentions) + "." + faker.numerify("##.##"),
                    "idFaq": faker.word(),
                    "R1": faker.word(),
                    "R2": faker.word(),
                    "R3": faker.word()
                }
            }
        }
        documento["topics"][0]["messages"].append(mensaje)

    return documento

# Función para insertar un lote de documentos
def insertar_lote(lote):
    collection.insert_many(lote)

# Función que se ejecuta en cada proceso para generar documentos y hacer la inserción
def proceso_insercion(n_documentos):
    lote_documentos = [generar_documento() for _ in range(n_documentos)]
    insertar_lote(lote_documentos)

# Número total de documentos a generar y tamaño del lote
total_docs = 1000
batch_size = 100

# Número de procesos a usar (equivalente al número de CPUs o menos)
num_procesos = cpu_count()

if __name__ == '__main__':
    start_time = time.time()

    # Usar Pool para paralelizar la generación e inserción
    with Pool(processes=num_procesos) as pool:
        # Dividir los documentos entre los procesos
        resultados = list(tqdm(pool.imap_unordered(proceso_insercion, [batch_size] * (total_docs // batch_size)), total=total_docs // batch_size))

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Documentos generados e insertados en {elapsed_time:.2f} segundos.")

import random
import time
from google.cloud import firestore

# Inicializar Firestore
db = firestore.Client()

# Referencia al documento que deseas leer
doc_ref = db.collection('dialog').document('02zQigGCyVjQcoTFhuUo')
doc_ref2 = db.collection('dialog').document('01BYrsUsXMeNYck7GtfR')

# Medir el tiempo de lectura del documento
doc2 = doc_ref2.get()
start_time = time.time()
doc = doc_ref.get()
end_time = time.time()

# Verificar si el documento existe
if doc.exists:
    # Obtener el id del documento y el id de la conversación
    document_id = doc.id
    conversation_id = doc.to_dict().get('conversation', 'Sin conversación')
    
    # Mostrar los ids
    print(f"Document ID: {document_id}")
    print(f"Conversation ID: {conversation_id}")
else:
    print("No se encontró el documento con el ID proporcionado.")

# Imprimir el tiempo que tomó la lectura
print(f"Tiempo para la primera lectura: {end_time - start_time:.6f} segundos")

import os
import requests
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pathlib import Path

# Carrega o .env da raiz do projeto
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

def connect_mongo(uri): 
    try:
        client = MongoClient(uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(f"Erro ao conectar ao MongoDB: {e}")
        return None

def create_connect_db(client, db_name):
    return client[db_name]

def create_connect_collection(db, collection_name):
    return db[collection_name]

def extract_api_data(url):
    return requests.get(url).json()
    
def insert_data(col, data):
    result = col.insert_many(data)
    return len(result.inserted_ids)

if __name__ == "__main__":
    # 1. Busca a URI do .env
    mongo_uri = os.getenv("MONGODB_URI_EXTRACT")
    
    # 2. Tenta conectar
    client = connect_mongo(mongo_uri)
    
    # 3. Só prossegue se o client for válido (evita o erro que você teve)
    if client:
        db = create_connect_db(client, "db_produtos_desafio")
        col = create_connect_collection(db, "produtos")

        data = extract_api_data("https://labdados.com/produtos")
        print(f"\nQuantidade de dados extraídos: {len(data)}")

        n_docs = insert_data(col, data)
        print(f"\nDocumentos inseridos na coleção: {n_docs}")

        client.close()
    else:
        print("Não foi possível prosseguir com a extração: Erro de conexão.")
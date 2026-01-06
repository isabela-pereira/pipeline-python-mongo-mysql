import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv

# --- FUNÇÕES DE CONEXÃO E MANIPULAÇÃO ---

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))
    try:
        client.admin.command('ping')
        print("Conectado ao MongoDB com sucesso!")
        return client
    except Exception as e:
        print(f"Erro na conexão: {e}")
        return None

def rename_collection_fields(col):
    """Traduz os campos do MongoDB, ignorando se já estiverem renomeados."""
    try:
        # Verificamos se há pelo menos um documento com o campo antigo 'title'
        if col.find_one({"title": {"$exists": True}}):
            col.update_many({}, {
                '$rename': {
                    'title': 'Título', 
                    'price': 'Preço', 
                    'description': 'Descrição', 
                    'category': 'Categoria', 
                    'image': 'Imagem', 
                    'rating': 'Avaliação'
                }
            })
            print("Campos renomeados no MongoDB com sucesso.")
        else:
            print("Os campos já estão renomeados ou a coleção está vazia.")
    except Exception as e:
        print(f"Aviso ao renomear: {e}")

def transform_rating(df):
    """Separa a coluna 'Avaliação' em 'Nota' e 'Total_Avaliacoes'."""
    if 'Avaliação' in df.columns and not df.empty:
        # 1. Transforma o dicionário em colunas
        df_rating = df['Avaliação'].apply(pd.Series)
        # 2. Renomeia as novas colunas
        df_rating = df_rating.rename(columns={'rate': 'Nota', 'count': 'Total_Avaliacoes'})
        # 3. Concatena e remove a antiga
        df = pd.concat([df.drop(columns=['Avaliação']), df_rating], axis=1)
    return df

def save_csv(df, caminho_completo):
    """Cria a pasta e salva o arquivo."""
    diretorio = os.path.dirname(caminho_completo)
    if diretorio and not os.path.exists(diretorio):
        os.makedirs(diretorio)
    df.to_csv(caminho_completo, index=False)
    print(f"Arquivo salvo em: {caminho_completo}")


load_dotenv() # Carrega o .env

# ... (funções anteriores permanecem iguais)

if __name__ == "__main__":
    uri = os.getenv("MONGODB_URI_TRANSFORM")
    client = connect_mongo(uri)
    
    if client:
        # A variável 'collection' é definida aqui dentro
        db = client["db_produtos"]
        collection = db["produtos"]

        # A chamada deve estar recuada (dentro do if)
        rename_collection_fields(collection)

        query_produtos = {"Avaliação.rate": {"$gte": 4.0, "$lt": 5.0}}
        lista_produtos = list(collection.find(query_produtos))
        
        df_produtos = pd.DataFrame(lista_produtos)

        if not df_produtos.empty:
            df_final = transform_rating(df_produtos)
            save_csv(df_final, '../avaliacao/tb_4_5_em_diante.csv')
        else:
            print("Nenhum produto encontrado com os critérios de filtro.")

        client.close()
    else:
        print("Erro: Não foi possível definir 'collection' porque a conexão falhou.")
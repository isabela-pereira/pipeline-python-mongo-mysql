import os
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
from pathlib import Path
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)
print(f"Usuário carregado: {os.getenv('DB_USER')}") # Deve imprimir 'isapereira' ou o que estiver no .env

def get_mysql_connection():
    """Estabelece a conexão com o servidor MySQL."""
    return mysql.connector.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

def setup_database(cursor):
    """Cria o banco de dados e a tabela se não existirem."""
    cursor.execute("CREATE DATABASE IF NOT EXISTS dbprodutos;")
    cursor.execute("USE dbprodutos;")
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS tb_eletronicos(
            id VARCHAR(100),
            Titulo VARCHAR(255),
            Preco FLOAT(10,2),
            Nota FLOAT(10,2),
            Total_Avaliacoes INT,
            PRIMARY KEY (id)
        );
    """)
    print("Banco de dados e tabela verificados/criados com sucesso.")

def process_and_insert_data(cursor, cnx):
    """Lê o CSV, limpa os dados e insere no MySQL."""
    path = os.getenv("CSV_PATH")
    
    if not os.path.exists(path):
        print(f"Erro: Arquivo CSV não encontrado em: {path}")
        return

    # Leitura e limpeza
    df = pd.read_csv(path)
    df = df.replace({np.nan: None})

    comando_insert = """
        INSERT INTO dbprodutos.tb_eletronicos (id, Titulo, Preco, Nota, Total_Avaliacoes)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE 
        Titulo=VALUES(Titulo), Preco=VALUES(Preco), Nota=VALUES(Nota), Total_Avaliacoes=VALUES(Total_Avaliacoes);
    """

    print(f"Iniciando a inserção de {len(df)} linhas...")

    for _, linha in df.iterrows():
        dados = (
            str(linha['id']), 
            str(linha['Título']), 
            float(linha['Preço']) if linha['Preço'] is not None else None,
            float(linha['Nota']) if linha['Nota'] is not None else None,
            int(linha['Total_Avaliacoes']) if linha['Total_Avaliacoes'] is not None else 0
        )
        cursor.execute(comando_insert, dados)

    cnx.commit()
    
    # Verificação
    cursor.execute("SELECT COUNT(*) FROM dbprodutos.tb_eletronicos;")
    total = cursor.fetchone()[0]
    print(f"Sucesso! Total de registros no banco: {total}")

def main():
    """Função principal para execução do pipeline."""
    cnx = None
    try:
        cnx = get_mysql_connection()
        if cnx.is_connected():
            cursor = cnx.cursor()
            
            setup_database(cursor)
            process_and_insert_data(cursor, cnx)
            
            cursor.close()

    except Error as e:
        print(f"Erro ao processar dados no MySQL: {e}")
    
    finally:
        if cnx and cnx.is_connected():
            cnx.close()
            print("Conexão com o MySQL encerrada.")

if __name__ == "__main__":
    main()
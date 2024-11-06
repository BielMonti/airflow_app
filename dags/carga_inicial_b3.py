from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

# Configuração do caminho e conexão com o banco de dados
DATA_PATH = '/opt/airflow/dados'
DB_CONFIG = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}

# Funções ETL
def extract_data(file_path):
    # Extrai os dados de cada arquivo específico
    # Especifica larguras de colunas conforme layout do arquivo B3
    colspecs = [
        (0, 2), (2, 10), (10, 12), (12, 24), (27, 39), 
        (56, 69), (69, 82), (82, 95), (108, 121), (170, 188)
    ]
    df = pd.read_fwf(file_path, colspecs=colspecs, encoding='latin1')
    df.columns = ['tipo_registro', 'data', 'codigo_bdi', 'ticker', 'nome_empresa',
                  'preco_abertura', 'maxima', 'minima', 'preco_fechamento', 'volume']
    return df

def transform_data(df):
    # Realiza transformações nas dimensões e tabela fato
    df['data'] = pd.to_datetime(df['data'], format='%Y%m%d', errors='coerce')
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    df['trimestre'] = df['data'].dt.quarter
    
    # Cria as dimensões e tabela fato separadamente
    dim_empresas = df[['ticker', 'nome_empresa']].drop_duplicates().rename(columns={'ticker': 'id_empresa'})
    dim_datas = df[['data', 'ano', 'mes', 'trimestre']].drop_duplicates().rename(columns={'data': 'id_data'})
    fato_cotacoes = df[['ticker', 'data', 'preco_abertura', 'preco_fechamento', 'maxima', 'minima', 'volume']]
    
    return dim_empresas, dim_datas, fato_cotacoes

def load_data(dim_empresas, dim_datas, fato_cotacoes):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    try:
        # Insere dados em `dim_empresa`
        for _, row in dim_empresas.iterrows():
            cursor.execute("""
                INSERT INTO dim_empresa (ticker, nome_empresa)
                VALUES (%s, %s)
                ON CONFLICT (ticker) DO NOTHING;
            """, (row['ticker'], row['nome_empresa']))
        
         # Insere dados em `dim_data`
        for _, row in dim_datas.iterrows():
            cursor.execute("""
                INSERT INTO dim_data (data, ano, mes, trimestre)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (data) DO NOTHING;
            """, (row['id_data'], row['ano'], row['mes'], row['trimestre']))
        
        # Insere dados em `fato_cotacoes`
        for _, row in fato_cotacoes.iterrows():
            cursor.execute("""
                INSERT INTO fato_cotacoes (
                    id_empresa, id_data, preco_abertura, preco_fechamento, maxima, minima, volume
                ) VALUES (
                    (SELECT id_empresa FROM dim_empresa WHERE ticker = %s),
                    (SELECT id_data FROM dim_data WHERE data = %s),
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT ON CONSTRAINT unique_cotacao DO NOTHING;
            """, (
                row['ticker'], row['data'], row['preco_abertura'], row['preco_fechamento'],
                row['maxima'], row['minima'], row['volume']
            ))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'carga_inicial_b3',
    default_args=default_args,
    description='Carga inicial para as tabelas com dados de 2022, 2023 e 2024',
    schedule_interval='@once',
    catchup=False,
    concurrency=1,
    max_active_runs=1
) as dag:
    
    def process_initial_files():
        files = ["COTAHIST_A2022.TXT", "COTAHIST_A2023.TXT", "COTAHIST_A2024.TXT"]
        
        for file in files:
            file_path = os.path.join(DATA_PATH, file)     
            df = extract_data(file_path)
            dim_empresas, dim_datas, fato_cotacoes = transform_data(df)
            load_data(dim_empresas, dim_datas, fato_cotacoes)
    
   # Tarefas para cada arquivo
    def process_file_2022():
        file_path = os.path.join(DATA_PATH, "COTAHIST_A2022.TXT")
        df = extract_data(file_path)
        dim_empresas, dim_datas, fato_cotacoes = transform_data(df)
        load_data(dim_empresas, dim_datas, fato_cotacoes)

    def process_file_2023():
        file_path = os.path.join(DATA_PATH, "COTAHIST_A2023.TXT")
        df = extract_data(file_path)
        dim_empresas, dim_datas, fato_cotacoes = transform_data(df)
        load_data(dim_empresas, dim_datas, fato_cotacoes)

    def process_file_2024():
        file_path = os.path.join(DATA_PATH, "COTAHIST_A2024.TXT")
        df = extract_data(file_path)
        dim_empresas, dim_datas, fato_cotacoes = transform_data(df)
        load_data(dim_empresas, dim_datas, fato_cotacoes)

    # Tasks da DAG
    process_2022 = PythonOperator(
        task_id='process_file_2022',
        python_callable=process_file_2022
    )

    process_2023 = PythonOperator(
        task_id='process_file_2023',
        python_callable=process_file_2023
    )

    process_2024 = PythonOperator(
        task_id='process_file_2024',
        python_callable=process_file_2024
    )

    process_2022 >> process_2023 >> process_2024

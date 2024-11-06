from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os

# Caminho e configuração do banco de dados para a DAG diária
DB_CONFIG = {
    'dbname': 'airflow',
    'user': 'airflow',
    'password': 'airflow',
    'host': 'postgres',
    'port': '5432'
}
DAILY_FILE_PATH = r'D:\Trabalhos\airflow_app\dados\COTAHIST_A2024.TXT'

# Funções ETL
def daily_extract():
    # Extração diária do arquivo de 2024
    colspecs = [
        (0, 2),   # Tipo de registro
        (2, 10),  # Data do pregão
        (12, 24), # Código de negociação (ticker)
        (27, 39), # Nome da empresa
        (56, 69), # Preço de abertura
        (69, 82), # Preço máximo
        (82, 95), # Preço mínimo
        (108, 121), # Preço de fechamento
        (170, 188) # Volume de negócios
    ]
    df = pd.read_fwf(DAILY_FILE_PATH, colspecs=colspecs, encoding='latin1')
    return df

def daily_transform(df):
    # Transformações para a carga diária
    df.columns = ['tipo_registro', 'data', 'ticker', 'nome_empresa', 'preco_abertura', 'maxima', 'minima', 'preco_fechamento', 'volume']
    df['data'] = pd.to_datetime(df['data'], format='%Y%m%d', errors='coerce')
    df['ano'] = df['data'].dt.year
    df['mes'] = df['data'].dt.month
    df['trimestre'] = df['data'].dt.quarter
    
   # Criação das dimensões e tabela fato para carga diária
    dim_empresas = df[['ticker', 'nome_empresa']].drop_duplicates()
    dim_datas = df[['data', 'ano', 'mes', 'trimestre']].drop_duplicates()
    fato_cotacoes = df[['ticker', 'data', 'preco_abertura', 'preco_fechamento', 'maxima', 'minima', 'volume']]
    
    return dim_empresas, dim_datas, fato_cotacoes

def daily_load(dim_empresas, dim_datas, fato_cotacoes):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cursor = conn.cursor()

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
    except Exception as e:
        logging.error("Erro durante o carregamento diário: %s", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

# Configuração da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 2),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'carga_diaria_b3',
    default_args=default_args,
    description='Carga diária com dados do arquivo de 2024',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    def process_daily():
        logging.info("Iniciando extração diária...")
        df = daily_extract()
        logging.info("Extração concluída. Transformando dados...")
        dim_empresas, dim_datas, fato_cotacoes = daily_transform(df)
        logging.info("Transformação concluída. Carregando dados...")
        daily_load(dim_empresas, dim_datas, fato_cotacoes)
        logging.info("Carga diária concluída.")
    
    daily_update_task = PythonOperator(
        task_id='daily_update_task',
        python_callable=process_daily
    )

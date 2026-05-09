import pandas as pd
import psycopg2
import time
import logging
from dotenv import load_dotenv
import os
import numpy as np

load_dotenv()

def connect_db():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=os.getenv("DB_PORT")
    )

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def connect_db():
    try:
        return psycopg2.connect(
            host=os.getenv("DB_HOST"),
            database=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            port=os.getenv("DB_PORT")
        )
    except Exception as e:
        logger.error(f"Erro ao conectar no banco: {e}")
        raise

def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.trips (
                id SERIAL PRIMARY KEY,
                vendor_id INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
            CREATE TABLE IF NOT EXISTS public.trips (
                id SERIAL PRIMARY KEY,
                vendor_id INTEGER,
                tpep_pickup_datetime TIMESTAMP,
                tpep_dropoff_datetime TIMESTAMP,
                passenger_count FLOAT,
                trip_distance FLOAT,
                payment_type FLOAT,
                fare_amount FLOAT,
                total_amount FLOAT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
    conn.commit()
    logger.info("Tabela 'trips' criada no banco de dados.")

def stream_data():
    logger.info("Carregando o dataset de amostra...")
    
    df = pd.read_parquet('data/yellow_tripdata_sample.parquet')
    
    df = df.replace({np.nan: None})
    
    conn = connect_db()
    create_table(conn)
    
    logger.info("Iniciando INSERTs em tempo real...")
    
    try:
        with conn.cursor() as cur:
            for index, row in df.iterrows():
                query = """
                    INSERT INTO public.trips (
                        vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, 
                        passenger_count, trip_distance, payment_type, 
                        fare_amount, total_amount
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    row['VendorID'], 
                    row['tpep_pickup_datetime'], 
                    row['tpep_dropoff_datetime'], 
                    row['passenger_count'],
                    row['trip_distance'], 
                    row['payment_type'],
                    row['fare_amount'],
                    row['total_amount']
                )
                
                cur.execute(query, values)
                conn.commit()
                
                logger.info(f"[{index + 1}] Viagem inserida: Distância {row['trip_distance']} mi, Valor ${row['total_amount']}")
                
                time.sleep(0.5) 
                
    except Exception as e:
        logger.error(f"Erro na simulação: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    stream_data()
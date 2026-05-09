import json
import duckdb
from kafka import KafkaConsumer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db = duckdb.connect("data/trips_data.duckdb")

def setup_sink():
    db.execute("""
        CREATE TABLE IF NOT EXISTS trips_analytics (
            id INTEGER,
            vendor_id INTEGER,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            total_amount DOUBLE,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

def run_consumer():
    setup_sink()
    
    consumer = KafkaConsumer(
        'cdc.public.trips', # O tópico que o Debezium criou
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    logger.info("Consumer iniciado! Aguardando mensagens do Kafka...")

    try:
        for message in consumer:
            payload = message.value.get('payload', {})
            
            data = payload.get('after')
            operation = payload.get('op')

            if data and operation == 'c':
                values = (
                    data['id'],
                    data['vendor_id'],
                    data['trip_distance'],
                    data['fare_amount'],
                    data['total_amount']
                )

                db.execute("""
                    INSERT INTO trips_analytics (id, vendor_id, trip_distance, fare_amount, total_amount)
                    VALUES (?, ?, ?, ?, ?)
                """, values)

                logger.info(f"Evento CDC processado: ID {data['id']} salvo no DuckDB!")
            
            elif operation == 'd':
                logger.warning(f"Registro deletado no Postgres! ID: {payload.get('before')['id']}")

    except KeyboardInterrupt:
        logger.info("Parando consumer...")
    finally:
        consumer.close()
        db.close()

if __name__ == "__main__":
    run_consumer()
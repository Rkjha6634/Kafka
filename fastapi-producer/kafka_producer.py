from kafka import KafkaProducer
from fastapi import  HTTPException
from produce_schema import ProduceMessage
import json  


#Constant value 

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
PRODUCER_CLIENT_ID = 'fastapi-producer'

def serializer(message):
    return json.dumps(message).encode() # default utf-8 encoding

producer = KafkaProducer(
    api_version=(0, 8, 0),
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=serializer,
    client_id=PRODUCER_CLIENT_ID
)

def produce_kafka_message(message: ProduceMessage):
    try:
        producer.send(KAFKA_TOPIC, message.dict())
        producer.flush() # ensures all messages are sent
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce message: {str(e)}")
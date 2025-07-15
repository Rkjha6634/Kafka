from fastapi import FastAPI
import asyncio
from kafka import KafkaConsumer
import json

# constant section 

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_CONSUMER_ID = 'fastapi_consumer'

stop_polling_event = asyncio.Event()

app = FastAPI()

def json_deserializer(value):
    if value is None:
        return 
    try:
        return json.loads(value.decode('utf-8'))
    except json.JSONDecodeError:
        print('Failed to decode JSON')
        return None 


def create_kafka_consumer():

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset ='earliest',
        enable_auto_commit=True,
        group_id=KAFKA_CONSUMER_ID,
        value_deserializer=json_deserializer
    )
    return consumer


async def poll_consumer(consumer : KafkaConsumer):
    try :
        while not stop_polling_event.is_set():
            print('Polling for messages...')
            records =consumer.poll(5000,250)
            if records:
                for records in records.values():
                    for message in records:
                        m = json.loads(message.value).get('message')
                        print(f"Received message: {m}")
            await asyncio.sleep(3)
    except Exception as e:
        print(f"Error while polling messages: {e}")
    finally:
        consumer.close()
        print("Consumer closed.")

polling_tasks = []
@app.get("/trigger")
async def trigger_polling():
    if not polling_tasks:
        stop_polling_event.clear()
        consumer = create_kafka_consumer()
        task = asyncio.create_task(poll_consumer(consumer=consumer))
        polling_tasks.append(task)

        return {"message": "Kafka Polling started."}
    return {"message": "Kafka Polling already running."}

@app.get("/stop-trigger")
async def stop_trigger():
    stop_polling_event.set()
    if polling_tasks:
        polling_tasks.pop()
    return {"message": "Kafka Polling stopped."}

        

    


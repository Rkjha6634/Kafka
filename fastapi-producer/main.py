from fastapi import FastAPI , BackgroundTasks
from kafka_producer import produce_kafka_message
from kafka.admin import KafkaAdminClient, NewTopic
from contextlib import asynccontextmanager
from produce_schema import ProduceMessage

# constants 

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'fastapi-topic'
KAFKA_ADMIN_CLIENT_ID = 'fastapi-admin'


@asynccontextmanager
async def lifespan(app: FastAPI):
    
    admin_client = KafkaAdminClient(
        bootstrap_servers=KAFKA_BROKER,
        client_id=KAFKA_ADMIN_CLIENT_ID, 

    )
    if not KAFKA_TOPIC in admin_client.list_topics():
        admin_client.create_topics(
            new_topics = [
                NewTopic(name=KAFKA_TOPIC,
                num_partitions=1, 
                replication_factor=1)
            ],
            validate_only=False
        )
        #admin_client.delete_topics([KAFKA_TOPIC]) -- to delete topics 
    yield # sepreation point (cleanaup)

app = FastAPI(lifespan=lifespan)

@app.post("/produce/message/" , tags=["Produce message"])
async def produce_message(messgaeRequest: ProduceMessage,background_tasks: BackgroundTasks):
    """
    Endpoint to produce a message to Kafka topic
    """
    background_tasks.add_task(produce_kafka_message, messgaeRequest)
    return {"message": "Message is being produced to Kafka topic."}
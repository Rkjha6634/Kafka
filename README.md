# FastAPI Kafka Consumer

This project demonstrates a simple Kafka consumer using FastAPI. It connects to a Kafka broker, consumes messages from a topic, and exposes endpoints to start and stop polling.

## Requirements

- Python 3.8+
- FastAPI
- kafka-python

## Docker 
    ```
    docker compose up --build 

    ```
## Setup

1. Install dependencies:
   ```bash
   pip install fastapi[standard] kafka-python-ng
   ```

2. Start your Kafka broker and ensure the topic `fastapi-topic` exists.

## Running the Application

Start the FastAPI server:
```bash
Fastapi dev main.py --port 8001 #- producer
Fastapi dev main.py --port 8002 #- Consumer 
```

## API Endpoints

- `GET /trigger` - Start polling Kafka for messages.
- `GET /stop-trigger` - Stop polling Kafka.

## Notes

- Update `KAFKA_BROKER`, `KAFKA_TOPIC`, and `KAFKA_CONSUMER_ID` in `mai.py` as needed.
- Make sure Kafka is running locally or update the broker address.

---

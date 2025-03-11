from fastapi import FastAPI

app = FastAPI(
    title="Confluent Kafka Tech Adapter",
    description="Tech Adapter for Confluent Kafka",  # noqa: E501
    version="2.2.0",
)

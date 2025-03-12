from typing import Any, Literal

from pydantic import BaseModel, Field

from src.models.data_product_descriptor import OutputPort


class KafkaPermission(BaseModel):
    resourceType: Literal["TOPIC", "GROUP"]
    resourceName: str
    resourcePatternType: str
    operation: str
    permissionType: str


class KafkaSchema(BaseModel):
    type: Literal["JSON", "AVRO", "PROTOBUF"]
    definition: str


class KafkaTopic(BaseModel):
    name: str
    numPartitions: int = Field(gt=0)
    replicationFactor: int = Field(gt=0)
    config: dict[str, Any]
    valueSchema: KafkaSchema | None = None


class KafkaSpecific(BaseModel):
    topic: KafkaTopic
    ownerPermissions: list[KafkaPermission]


class KafkaOutputPort(OutputPort):
    platform: Literal["Confluent"]
    technology: Literal["Kafka"]
    specific: KafkaSpecific

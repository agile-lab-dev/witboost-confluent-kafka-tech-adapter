from confluent_kafka.schema_registry import SchemaRegistryError
from confluent_kafka.schema_registry.schema_registry_client import (
    Schema,
    SchemaRegistryClient,
)

from src.models.service_error import ServiceError
from src.settings.kafka_settings import KafkaSettings
from src.utility.logger import get_logger


class SchemaRegistryServiceError(ServiceError):
    pass


class SchemaRegistryService:
    def __init__(
        self,
        kafka_settings: KafkaSettings,
    ):
        self.kafka_settings = kafka_settings
        self.schema_registry_client = SchemaRegistryClient(
            conf=kafka_settings.schema_registry_client_config
        )
        self.logger = get_logger(__name__)

    def register_schema(
        self,
        subject_name: str,
        schema_type: str,
        schema_str: str,
    ) -> int:
        """Registers a schema in the Schema Registry.

        Args:
            subject_name (str): The name of the schema subject.
            schema_type (str): The type of the schema (e.g., "AVRO", "JSON", "PROTOBUF").
            schema_str (str): The schema definition as a string.

        Returns:
            int: The schema ID assigned by the Schema Registry.

        Raises:
            SchemaRegistryServiceError: If schema registration fails.
        """
        try:
            schema = Schema(schema_str=schema_str, schema_type=schema_type)
            return self.schema_registry_client.register_schema(subject_name, schema)
        except SchemaRegistryError as sre:
            error_message = f"Failed to register schema for subject {subject_name}. Details: {sre.error_message}. Code: {sre.error_code}"  # noqa: E501
            self.logger.exception(error_message)
            raise SchemaRegistryServiceError(error_message)
        except Exception as e:
            error_message = f"Failed to register schema for subject {subject_name}. Details: {str(e)}"
            self.logger.exception(error_message)
            raise SchemaRegistryServiceError(error_message)

    def delete_subject(
        self,
        subject_name: str,
    ) -> None:
        """Deletes a schema subject from the Schema Registry.

        This method performs both a soft delete and a hard delete.

        Args:
            subject_name (str): The name of the schema subject to delete.

        Raises:
            SchemaRegistryServiceError: If the subject deletion fails.
        """
        try:
            self._soft_delete(subject_name)
            self._hard_delete(subject_name)
            return None
        except SchemaRegistryError as sre:
            error_message = f"Failed to delete subject {subject_name}. Details: {sre.error_message}. Code: {sre.error_code}"  # noqa: E501
            self.logger.exception(error_message)
            raise SchemaRegistryServiceError(error_message)
        except Exception as e:
            error_message = (
                f"Failed to delete subject {subject_name}. Details: {str(e)}"
            )
            self.logger.exception(error_message)
            raise SchemaRegistryServiceError(error_message)

    def _soft_delete(self, subject_name: str):
        try:
            self.schema_registry_client.delete_subject(subject_name)
            return None
        except SchemaRegistryError as sre:
            # 40404 means soft deleted
            # 40401 means not found
            if sre.error_code == 40404 or sre.error_code == 40401:
                return None
            else:
                raise

    def _hard_delete(self, subject_name: str):
        try:
            self.schema_registry_client.delete_subject(subject_name, permanent=True)
            return None
        except SchemaRegistryError as sre:
            # 40401 means not found
            if sre.error_code == 40401:
                return None
            else:
                raise

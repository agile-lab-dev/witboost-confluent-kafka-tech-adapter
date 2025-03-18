from unittest import mock

import pytest
from confluent_kafka.schema_registry import SchemaRegistryError

from src.services.schema_registry_service import (
    SchemaRegistryService,
    SchemaRegistryServiceError,
)
from src.settings.kafka_settings import KafkaSettings

kafka_settings = KafkaSettings(
    admin_client_config=dict(), schema_registry_client_config=dict()
)
subject_name = "subject_name"


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_register_schema_ok(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.register_schema.return_value = 1

    res = schema_registry_service.register_schema(subject_name, "JSON", "{}")

    assert not isinstance(res, SchemaRegistryServiceError)
    assert res == 1


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_register_schema_registry_error(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.register_schema.side_effect = (
        SchemaRegistryError(401, 401, "Unauthorized")
    )

    with pytest.raises(SchemaRegistryServiceError):
        schema_registry_service.register_schema(subject_name, "JSON", "{}")


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_register_schema_error(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.register_schema.side_effect = ValueError(
        "Error"
    )

    with pytest.raises(SchemaRegistryServiceError):
        schema_registry_service.register_schema(subject_name, "JSON", "{}")


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_delete_subject_ok(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.delete_subject.return_value = [1]

    res = schema_registry_service.delete_subject(subject_name)

    assert not isinstance(res, SchemaRegistryServiceError)
    assert res is None


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_delete_subject_registry_error(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.delete_subject.side_effect = (
        SchemaRegistryError(401, 401, "Unauthorized")
    )

    with pytest.raises(SchemaRegistryServiceError):
        schema_registry_service.delete_subject(subject_name)


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_delete_subject_error(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.delete_subject.side_effect = ValueError(
        "Error"
    )

    with pytest.raises(SchemaRegistryServiceError):
        schema_registry_service.delete_subject(subject_name)


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_delete_subject_already_soft_deleted(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.delete_subject.side_effect = [
        SchemaRegistryError(404, 40404, "Soft deleted"),
        [1],
    ]

    res = schema_registry_service.delete_subject(subject_name)

    assert res is None


@mock.patch("src.services.schema_registry_service.SchemaRegistryClient")
def test_delete_subject_already_hard_deleted(mock_schema_registry_client):
    schema_registry_service = SchemaRegistryService(kafka_settings)
    mock_schema_registry_client.return_value.delete_subject.side_effect = (
        SchemaRegistryError(404, 40401, "Not found")
    )

    res = schema_registry_service.delete_subject(subject_name)

    assert res is None

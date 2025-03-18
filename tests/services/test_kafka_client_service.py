from unittest import mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from src.services.kafka_client_service import (
    KafkaClientService,
    KafkaClientServiceError,
)
from src.settings.kafka_settings import KafkaSettings

kafka_settings = KafkaSettings(
    admin_client_config=dict(), schema_registry_client_config=dict()
)
topic_name = "topic_name"


class FakeTopic:
    def __init__(self, name, partitions=1):
        self.topic = name
        self.partitions = list(range(1, partitions + 1))


class FakeListTopics:
    def __init__(self, name=None, partitions=1):
        self.topics = dict() if name is None else {name: FakeTopic(name, partitions)}


class FakeFutureResultOk:
    def result(self):
        return None


class FakeFutureResultError:
    def __init__(self, error):
        self.error = error

    def result(self):
        raise self.error


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_create_or_update_topic_ok(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.side_effect = [
        FakeListTopics(),
        FakeListTopics(name=topic_name),
    ]
    mock_admin_client.return_value.create_topics.return_value = {
        topic_name: FakeFutureResultOk()
    }
    mock_admin_client.return_value.alter_configs.return_value = {
        topic_name: FakeFutureResultOk()
    }

    res = kafka_client_service.create_or_update_topic(topic_name, 1, 1, dict())

    assert res is None


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_create_or_update_topic_already_exist_increase_partitions(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics(
        name=topic_name
    )
    mock_admin_client.return_value.create_partitions.return_value = {
        topic_name: FakeFutureResultOk()
    }
    mock_admin_client.return_value.alter_configs.return_value = {
        topic_name: FakeFutureResultOk()
    }

    res = kafka_client_service.create_or_update_topic(topic_name, 3, 1, dict())

    assert res is None
    assert not mock_admin_client.return_value.create_topics.called


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_create_or_update_topic_already_exist_decrease_partitions_error(
    mock_admin_client,
):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics(
        name=topic_name, partitions=3
    )

    with pytest.raises(KafkaClientServiceError) as e:
        kafka_client_service.create_or_update_topic(topic_name, 1, 1, dict())

    assert (
        e.value.error_msg
        == "Cannot decrease partitions for topic topic_name. Current partition count: 3, requested: 1"
    )
    assert not mock_admin_client.return_value.create_topics.called
    assert not mock_admin_client.return_value.create_partitions.called


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_create_or_update_topic_error(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics()
    mock_admin_client.return_value.create_topics.return_value = {
        topic_name: FakeFutureResultError(ValueError("error"))
    }

    with pytest.raises(KafkaClientServiceError):
        kafka_client_service.create_or_update_topic(topic_name, 1, 1, dict())


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_create_or_update_topic_kafka_error(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics()
    mock_admin_client.return_value.create_topics.return_value = {
        topic_name: FakeFutureResultError(KafkaException(KafkaError(-1)))
    }

    with pytest.raises(KafkaClientServiceError):
        kafka_client_service.create_or_update_topic(topic_name, 1, 1, dict())


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_delete_topic_not_existing_ok(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics()

    res = kafka_client_service.delete_topic(topic_name)

    assert res is None
    assert not mock_admin_client.return_value.delete_topics.called


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_delete_topic_existing_ok(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics(
        name=topic_name
    )
    mock_admin_client.return_value.delete_topics.return_value = {
        topic_name: FakeFutureResultOk()
    }

    res = kafka_client_service.delete_topic(topic_name)

    assert res is None
    assert mock_admin_client.return_value.delete_topics.called


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_delete_topic_existing_error(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics(
        name=topic_name
    )
    mock_admin_client.return_value.delete_topics.return_value = {
        topic_name: FakeFutureResultError(ValueError("error"))
    }

    with pytest.raises(KafkaClientServiceError):
        kafka_client_service.delete_topic(topic_name)


@mock.patch("src.services.kafka_client_service.AdminClient")
def test_delete_topic_existing_kafka_error(mock_admin_client):
    kafka_client_service = KafkaClientService(kafka_settings)
    mock_admin_client.return_value.list_topics.return_value = FakeListTopics(
        name=topic_name
    )
    mock_admin_client.return_value.delete_topics.return_value = {
        topic_name: FakeFutureResultError(KafkaException(KafkaError(-1)))
    }

    with pytest.raises(KafkaClientServiceError):
        kafka_client_service.delete_topic(topic_name)

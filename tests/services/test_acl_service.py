from unittest import mock

import pytest
from confluent_kafka import KafkaError, KafkaException

from src.models.kafka_models import KafkaPermission
from src.services.acl_service import AclService, AclServiceError
from src.services.principal_mapping_service import KafkaPrincipal
from src.settings.kafka_settings import KafkaSettings

kafka_settings = KafkaSettings(
    admin_client_config=dict(), schema_registry_client_config=dict()
)

topic_name = "topic_name"
acls = [
    KafkaPermission(
        resourceType="TOPIC",
        resourceName=topic_name,
        resourcePatternType="LITERAL",
        operation="READ",
        permissionType="ALLOW",
    )
]
principals = [KafkaPrincipal("User:my_user")]


class FakeFutureResultOk:
    def result(self):
        return None


class FakeFutureResultError:
    def __init__(self, error):
        self.error = error

    def result(self):
        raise self.error


@mock.patch("src.services.acl_service.AdminClient")
def test_apply_acls_to_principals_ok(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.create_acls.return_value = {"": FakeFutureResultOk()}

    res = acl_service.apply_acls_to_principals(acls, principals)

    assert res is None


@mock.patch("src.services.acl_service.AdminClient")
def test_apply_acls_to_principals_error(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.create_acls.return_value = {
        "": FakeFutureResultError(ValueError("error"))
    }

    with pytest.raises(AclServiceError):
        acl_service.apply_acls_to_principals(acls, principals)


@mock.patch("src.services.acl_service.AdminClient")
def test_apply_acls_to_principals_kafka_error(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.create_acls.return_value = {
        "": FakeFutureResultError(KafkaException(KafkaError(-1)))
    }

    with pytest.raises(AclServiceError):
        acl_service.apply_acls_to_principals(acls, principals)


@mock.patch("src.services.acl_service.AdminClient")
def test_remove_all_acls_for_topic_ok(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.delete_acls.return_value = {"": FakeFutureResultOk()}

    res = acl_service.remove_all_acls_for_topic(topic_name)

    assert res is None


@mock.patch("src.services.acl_service.AdminClient")
def test_remove_all_acls_for_topic_error(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.delete_acls.return_value = {
        "": FakeFutureResultError(ValueError("error"))
    }

    with pytest.raises(AclServiceError):
        acl_service.remove_all_acls_for_topic(topic_name)


@mock.patch("src.services.acl_service.AdminClient")
def test_remove_all_acls_for_topic_kafka_error(mock_admin_client):
    acl_service = AclService(kafka_settings)
    mock_admin_client.return_value.delete_acls.return_value = {
        "": FakeFutureResultError(KafkaException(KafkaError(-1)))
    }

    with pytest.raises(AclServiceError):
        acl_service.remove_all_acls_for_topic(topic_name)

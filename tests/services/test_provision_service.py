from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml

from src.models.api_models import ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.services.principal_mapping_service import KafkaPrincipal
from src.services.provision_service import ProvisionService
from src.services.schema_registry_service import SchemaRegistryServiceError
from src.services.validation_service import validate_kafka_output_port
from src.utility.parsing_pydantic_models import parse_yaml_with_model


@pytest.fixture(name="get_descriptor")
def descriptor_str_fixture():
    def get_descriptor(param):
        return Path(f"tests/descriptors/{param}").read_text()

    return get_descriptor


@pytest.fixture(name="unpacked_request")
def unpacked_request_fixture(get_descriptor, request):
    request = yaml.safe_load(get_descriptor(request.param))
    data_product = parse_yaml_with_model(request.get("dataProduct"), DataProduct)
    component_to_provision = request.get("componentIdToProvision")
    return validate_kafka_output_port((data_product, component_to_provision))


@pytest.fixture(name="kafka_client_service")
def kafka_client_service_fixture():
    return Mock()


@pytest.fixture(name="principal_mapping_service")
def principal_mapping_service_fixture():
    return Mock()


@pytest.fixture(name="schema_registry_service")
def schema_registry_service_fixture():
    return Mock()


@pytest.fixture(name="acl_service")
def acl_service_fixture():
    return Mock()


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_provision_ok(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    kafka_client_service.create_or_update_topic.return_value = None
    principal_mapping_service.map_identity.return_value = KafkaPrincipal("User:owner")
    acl_service.apply_acls_to_principals.return_value = None
    schema_registry_service.register_schema.return_value = 1
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.provision(data_product, op)

    kafka_client_service.create_or_update_topic.assert_called_once()
    principal_mapping_service.map_identity.assert_called_once()
    acl_service.apply_acls_to_principals.assert_called_once()
    schema_registry_service.register_schema.assert_called_once()
    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED
    assert provisioning_status.info.publicInfo == {
        "schema_id": {"type": "string", "label": "Registered Schema ID", "value": "1"},
        "topic_name": {
            "type": "string",
            "label": "Topic Name",
            "value": "healthcare_vaccinations_0_kafka-output-port_development",
        },
        "num_partitions": {
            "type": "string",
            "label": "Number of partitions",
            "value": "3",
        },
        "replication_factor": {
            "type": "string",
            "label": "Replication factor",
            "value": "1",
        },
    }


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_valid_no_schema.yaml"], indirect=True
)
def test_provision_no_schema_ok(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    kafka_client_service.create_or_update_topic.return_value = None
    principal_mapping_service.map_identity.return_value = KafkaPrincipal("User:owner")
    schema_registry_service.register_schema.return_value = 1
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.provision(data_product, op)

    kafka_client_service.create_or_update_topic.assert_called_once()
    principal_mapping_service.map_identity.assert_called_once()
    acl_service.apply_acls_to_principals.assert_called_once()
    schema_registry_service.register_schema.assert_not_called()
    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED
    assert provisioning_status.info.publicInfo == {
        "topic_name": {
            "type": "string",
            "label": "Topic Name",
            "value": "healthcare_vaccinations_0_kafka-output-port_development",
        },
        "num_partitions": {
            "type": "string",
            "label": "Number of partitions",
            "value": "3",
        },
        "replication_factor": {
            "type": "string",
            "label": "Replication factor",
            "value": "1",
        },
    }


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_provision_service_error(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    kafka_client_service.create_or_update_topic.return_value = None
    principal_mapping_service.map_identity.return_value = KafkaPrincipal("User:owner")
    acl_service.apply_acls_to_principals.return_value = None
    schema_registry_service.register_schema.side_effect = SchemaRegistryServiceError(
        "Unauthorized"
    )
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.provision(data_product, op)

    kafka_client_service.create_or_update_topic.assert_called_once()
    principal_mapping_service.map_identity.assert_called_once()
    acl_service.apply_acls_to_principals.assert_called_once()
    schema_registry_service.register_schema.assert_called_once()
    assert isinstance(provisioning_status, SystemErr)
    assert provisioning_status.error == "Unauthorized"


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_unprovision_ok_remove_data(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    kafka_client_service.delete_topic.return_value = None
    acl_service.remove_all_acls_for_topic.return_value = None
    schema_registry_service.delete_subject.return_value = None
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.unprovision(data_product, op, True)

    kafka_client_service.delete_topic.assert_called_once()
    acl_service.remove_all_acls_for_topic.assert_called_once()
    schema_registry_service.delete_subject.assert_called_once()
    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_unprovision_ok_no_remove_data(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    acl_service.remove_all_acls_for_topic.return_value = None
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.unprovision(data_product, op, False)

    kafka_client_service.delete_topic.assert_not_called()
    acl_service.remove_all_acls_for_topic.assert_called_once()
    schema_registry_service.delete_subject.assert_not_called()
    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_unprovision_service_error(
    unpacked_request,
    kafka_client_service,
    principal_mapping_service,
    schema_registry_service,
    acl_service,
):
    data_product, op = unpacked_request
    kafka_client_service.delete_topic.return_value = None
    acl_service.remove_all_acls_for_topic.return_value = None
    schema_registry_service.delete_subject.side_effect = SchemaRegistryServiceError(
        "Unauthorized"
    )
    provisioner = ProvisionService(
        kafka_client_service,
        principal_mapping_service,
        schema_registry_service,
        acl_service,
    )

    provisioning_status = provisioner.unprovision(data_product, op, True)

    kafka_client_service.delete_topic.assert_called_once()
    acl_service.remove_all_acls_for_topic.assert_called_once()
    schema_registry_service.delete_subject.assert_called_once()
    assert isinstance(provisioning_status, SystemErr)
    assert provisioning_status.error == "Unauthorized"

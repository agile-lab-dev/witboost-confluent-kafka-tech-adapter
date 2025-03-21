from pathlib import Path
from unittest.mock import Mock, call

import pytest
import yaml

from src.models.api_models import (
    ProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.services.acl_service import AclServiceError
from src.services.principal_mapping_service import KafkaPrincipal
from src.services.update_acl_service import UpdateAclService
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
    return data_product, component_to_provision


@pytest.fixture(name="principal_mapping_service")
def principal_mapping_service_fixture():
    return Mock()


@pytest.fixture(name="acl_service")
def acl_service_fixture():
    return Mock()


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_update_acl_ok(
    unpacked_request,
    principal_mapping_service,
    acl_service,
):
    data_product, component_id = unpacked_request
    principal_mapping_service.map_identity.return_value = KafkaPrincipal("User:user")
    witboost_identities = ["user:user"]
    acl_service.apply_acls_to_principals.return_value = None
    acl_service.remove_all_acls_for_topic.return_value = None
    update_acl_service = UpdateAclService(principal_mapping_service, acl_service)
    map_identity_calls = [call("user:name.surname_agilelab.it"), call("user:user")]

    provisioning_status = update_acl_service.update_acls(
        data_product, component_id, witboost_identities
    )

    acl_service.remove_all_acls_for_topic.assert_called_once()
    principal_mapping_service.map_identity.assert_has_calls(
        map_identity_calls, any_order=True
    )
    assert acl_service.apply_acls_to_principals.call_count == 2
    assert isinstance(provisioning_status, ProvisioningStatus)
    assert provisioning_status.status == Status1.COMPLETED


@pytest.mark.parametrize("unpacked_request", ["descriptor_valid.yaml"], indirect=True)
def test_update_acl_service_error(
    unpacked_request,
    principal_mapping_service,
    acl_service,
):
    data_product, component_id = unpacked_request
    principal_mapping_service.map_identity.return_value = KafkaPrincipal("User:user")
    witboost_identities = ["user:user"]
    acl_service.apply_acls_to_principals.side_effect = [
        None,
        AclServiceError("Unauthorized"),
    ]
    acl_service.remove_all_acls_for_topic.return_value = None
    update_acl_service = UpdateAclService(principal_mapping_service, acl_service)
    map_identity_calls = [call("user:name.surname_agilelab.it"), call("user:user")]

    provisioning_status = update_acl_service.update_acls(
        data_product, component_id, witboost_identities
    )

    acl_service.remove_all_acls_for_topic.assert_called_once()
    principal_mapping_service.map_identity.assert_has_calls(
        map_identity_calls, any_order=True
    )
    assert acl_service.apply_acls_to_principals.call_count == 2
    assert isinstance(provisioning_status, SystemErr)
    assert provisioning_status.error == "Unauthorized"


@pytest.mark.parametrize(
    "unpacked_request", ["descriptor_not_valid.yaml"], indirect=True
)
def test_update_acl_validation_error(
    unpacked_request,
    principal_mapping_service,
    acl_service,
):
    data_product, component_id = unpacked_request
    witboost_identities = ["user:user"]
    update_acl_service = UpdateAclService(principal_mapping_service, acl_service)

    provisioning_status = update_acl_service.update_acls(
        data_product, component_id, witboost_identities
    )

    acl_service.remove_all_acls_for_topic.assert_not_called()
    principal_mapping_service.map_identity.assert_not_called()
    acl_service.apply_acls_to_principals.assert_not_called()
    assert isinstance(provisioning_status, ValidationError)
    assert len(provisioning_status.errors) == 5

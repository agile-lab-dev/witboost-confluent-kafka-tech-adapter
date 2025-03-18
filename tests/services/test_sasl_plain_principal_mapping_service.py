import pytest

from src.services.principal_mapping_service import (
    KafkaPrincipal,
    PrincipalMappingServiceError,
)
from src.services.sasl_plain_principal_mapping_service import (
    SaslPlainPrincipalMappingService,
)

mapping_service = SaslPlainPrincipalMappingService()


def test_map_identity_user():
    witboost_identity_user = "user:name.surname_email.com"
    expected_mapping = "User:name.surname_email.com"

    res = mapping_service.map_identity(witboost_identity_user)

    assert isinstance(res, KafkaPrincipal)
    assert res.principal == expected_mapping


def test_map_identity_group():
    witboost_identity_group = "group:dev"
    expected_error_message = "Groups are not supported by SASL Plain protocol."

    with pytest.raises(PrincipalMappingServiceError) as e:
        mapping_service.map_identity(witboost_identity_group)

    assert e.value.error_msg == expected_error_message


def test_map_identity_not_valid():
    witboost_identity_not_valid = "not_valid"
    expected_error_message = "not_valid is not a valid Witboost identity."

    with pytest.raises(PrincipalMappingServiceError) as e:
        mapping_service.map_identity(witboost_identity_not_valid)

    assert e.value.error_msg == expected_error_message

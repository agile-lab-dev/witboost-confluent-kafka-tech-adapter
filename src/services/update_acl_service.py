import pydantic

from src.models.api_models import (
    ProvisioningStatus,
    Status1,
    SystemErr,
    ValidationError,
)
from src.models.data_product_descriptor import DataProduct
from src.models.kafka_models import KafkaOutputPort, KafkaPermission
from src.models.service_error import ServiceError
from src.services.acl_service import AclService
from src.services.principal_mapping_service import (
    PrincipalMappingService,
)
from src.utility.logger import get_logger


class UpdateAclService:
    def __init__(
        self,
        principal_mapping_service: PrincipalMappingService,
        acl_service: AclService,
    ):
        self.principal_mapping_service = principal_mapping_service
        self.acl_service = acl_service
        self._logger = get_logger(__name__)

    def update_acls(
        self,
        data_product: DataProduct,
        component_id: str,
        witboost_identities: list[str],
    ) -> ProvisioningStatus | ValidationError | SystemErr:
        try:
            component_to_provision: KafkaOutputPort | None = (
                data_product.get_typed_component_by_id(component_id, KafkaOutputPort)
            )
            if component_to_provision is None:
                error_msg = f"Component with ID {component_id} not found in descriptor"
                self._logger.error(error_msg)
                return ValidationError(errors=[error_msg])

            self.acl_service.remove_all_acls_for_topic(
                component_to_provision.specific.topic.name
            )

            self._logger.info("Mapping identity for %s", data_product.dataProductOwner)
            mapped_identity = self.principal_mapping_service.map_identity(
                data_product.dataProductOwner
            )
            self._logger.info("Applying acls to %s", mapped_identity.principal)
            self.acl_service.apply_acls_to_principals(
                component_to_provision.specific.ownerPermissions, [mapped_identity]
            )

            for witboost_identity in witboost_identities:
                self._logger.info("Mapping identity for %s", witboost_identity)
                mapped_identity = self.principal_mapping_service.map_identity(
                    witboost_identity
                )
                self._logger.info("Applying acls to %s", mapped_identity.principal)
                self.acl_service.apply_acls_to_principals(
                    self._generate_acls_for(
                        component_to_provision.specific.topic.name,
                        mapped_identity.principal,
                    ),
                    [mapped_identity],
                )
            return ProvisioningStatus(status=Status1.COMPLETED, result="")
        except pydantic.ValidationError as ve:
            error_msg = (
                f"Failed to parse the component {component_id} as a Kafka OutputPort:"
            )
            self._logger.exception(error_msg)
            combined = [error_msg]
            combined.extend(
                map(
                    str,
                    ve.errors(
                        include_url=False, include_context=False, include_input=False
                    ),
                )
            )
            return ValidationError(errors=combined)
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _generate_acls_for(self, topic: str, principal: str) -> list[KafkaPermission]:
        group_name = f"{topic}_{principal}_consumer_group"
        return [
            KafkaPermission(
                resourceType="TOPIC",
                resourceName=topic,
                resourcePatternType="LITERAL",
                operation="READ",
                permissionType="ALLOW",
            ),
            KafkaPermission(
                resourceType="GROUP",
                resourceName=group_name,
                resourcePatternType="LITERAL",
                operation="READ",
                permissionType="ALLOW",
            ),
        ]

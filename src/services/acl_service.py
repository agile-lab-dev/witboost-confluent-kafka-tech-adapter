from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AclBinding,
    AclBindingFilter,
    AclOperation,
    AclPermissionType,
    AdminClient,
    ResourcePatternType,
    ResourceType,
)

from src.models.kafka_models import KafkaPermission
from src.models.service_error import ServiceError
from src.services.principal_mapping_service import (
    KafkaPrincipal,
)
from src.settings.kafka_settings import KafkaSettings
from src.utility.logger import get_logger


class AclServiceError(ServiceError):
    pass


class AclService:
    def __init__(
        self,
        kafka_settings: KafkaSettings,
    ):
        self._kafka_settings = kafka_settings
        self._admin_client = AdminClient(conf=self._kafka_settings.admin_client_config)
        self._logger = get_logger(__name__)

    def apply_acls_to_principals(
        self, acls: list[KafkaPermission], principals: list[KafkaPrincipal]
    ) -> None:
        """Applies a set of ACLs to the specified Kafka principals.

        This method creates ACL bindings for each combination of ACL and principal
        and applies them to Kafka.

        Args:
            acls (list[KafkaPermission]): List of Kafka ACL permissions to apply.
            principals (list[KafkaPrincipal]): List of Kafka principals to apply ACLs to.

        Raises:
            AclServiceError: If there is a failure in applying ACLs.
        """
        try:
            bindings = [
                AclBinding(
                    restype=ResourceType[acl.resourceType],
                    name=acl.resourceName,
                    resource_pattern_type=ResourcePatternType[acl.resourcePatternType],
                    principal=principal.principal,
                    host="*",
                    operation=AclOperation[acl.operation],
                    permission_type=AclPermissionType[acl.permissionType],
                )
                for acl in acls
                for principal in principals
            ]
            fs = self._admin_client.create_acls(bindings)
            for res, future in fs.items():
                future.result()
                self._logger.info(f"Created acl {res}")
            return None
        except KafkaException as ke:
            details = str(ke) if len(getattr(ke, "args", ())) == 0 else ke.args[0].str()
            error_message = f"Failed to apply acls. Details: {details}"
            self._logger.exception(error_message)
            raise AclServiceError(error_message)
        except Exception as e:
            error_message = f"Failed to apply acls. Details: {str(e)}"
            self._logger.exception(error_message)
            raise AclServiceError(error_message)

    def remove_all_acls_for_topic(self, topic_name: str) -> None:
        """Removes all ACLs associated with a specific Kafka topic.

        This method deletes all ACLs for the given topic using an ACL binding filter.

        Args:
            topic_name (str): Name of the Kafka topic for which ACLs should be removed.

        Raises:
            AclServiceError: If there is a failure in removing ACLs.
        """
        try:
            binding_filter = AclBindingFilter(
                restype=ResourceType.TOPIC,
                name=topic_name,
                resource_pattern_type=ResourcePatternType.LITERAL,
                principal=None,
                host=None,
                operation=AclOperation.ANY,
                permission_type=AclPermissionType.ANY,
            )
            fs = self._admin_client.delete_acls([binding_filter])
            for res, future in fs.items():
                future.result()
                self._logger.info("Deleted acls for topic %s", topic_name)
            return None
        except KafkaException as ke:
            details = str(ke) if len(getattr(ke, "args", ())) == 0 else ke.args[0].str()
            error_message = (
                f"Failed to remove acls for topic {topic_name}. Details: {details}"
            )
            self._logger.exception(error_message)
            raise AclServiceError(error_message)
        except Exception as e:
            error_message = (
                f"Failed to remove acls for topic {topic_name}. Details: {str(e)}"
            )
            self._logger.exception(error_message)
            raise AclServiceError(error_message)

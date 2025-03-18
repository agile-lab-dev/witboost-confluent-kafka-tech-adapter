from src.models.api_models import Info, ProvisioningStatus, Status1, SystemErr
from src.models.data_product_descriptor import DataProduct
from src.models.kafka_models import KafkaOutputPort
from src.models.service_error import ServiceError
from src.services.acl_service import AclService
from src.services.kafka_client_service import (
    KafkaClientService,
)
from src.services.principal_mapping_service import (
    PrincipalMappingService,
)
from src.services.schema_registry_service import (
    SchemaRegistryService,
)
from src.utility.logger import get_logger


class ProvisionService:
    def __init__(
        self,
        kafka_client_service: KafkaClientService,
        principal_mapping_service: PrincipalMappingService,
        schema_registry_service: SchemaRegistryService,
        acl_service: AclService,
    ):
        self.kafka_client_service = kafka_client_service
        self.principal_mapping_service = principal_mapping_service
        self.schema_registry_service = schema_registry_service
        self.acl_service = acl_service
        self.logger = get_logger(__name__)

    def provision(
        self, data_product: DataProduct, op: KafkaOutputPort
    ) -> ProvisioningStatus | SystemErr:
        try:
            self.logger.info("Starting provisioning for component %s", op.id)

            self.logger.info("Managing topic %s", op.specific.topic.name)
            self.kafka_client_service.create_or_update_topic(
                op.specific.topic.name,
                op.specific.topic.numPartitions,
                op.specific.topic.replicationFactor,
                op.specific.topic.config,
            )

            self.logger.info("Mapping identity for %s", data_product.dataProductOwner)
            mapped_identity = self.principal_mapping_service.map_identity(
                data_product.dataProductOwner
            )

            self.logger.info("Applying acls to %s", mapped_identity.principal)
            self.acl_service.apply_acls_to_principals(
                op.specific.ownerPermissions, [mapped_identity]
            )

            schema_res = None
            if op.specific.topic.valueSchema is not None:
                subject_name = f"{op.specific.topic.name}-value"
                self.logger.info("Registering schema for subject %s", subject_name)
                schema_res = self.schema_registry_service.register_schema(
                    subject_name,
                    op.specific.topic.valueSchema.type,
                    op.specific.topic.valueSchema.definition,
                )

            self.logger.info("Successfully provisioned component %s", op.id)
            return ProvisioningStatus(
                status=Status1.COMPLETED,
                result="",
                info=Info(
                    publicInfo=self._get_public_info(op, schema_res),
                    privateInfo=dict(),
                ),
            )
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def unprovision(
        self, data_product: DataProduct, op: KafkaOutputPort, remove_data: bool
    ) -> ProvisioningStatus | SystemErr:
        try:
            self.logger.info("Starting unprovisioning for component %s", op.id)

            self.logger.info("Removing acls for topic %s", op.specific.topic.name)
            self.acl_service.remove_all_acls_for_topic(op.specific.topic.name)

            if remove_data:
                self.logger.info("Deleting topic %s", op.specific.topic.name)
                self.kafka_client_service.delete_topic(op.specific.topic.name)

                subject_name = f"{op.specific.topic.name}-value"
                self.logger.info("Deleting schema for subject %s", subject_name)
                self.schema_registry_service.delete_subject(subject_name)

            self.logger.info("Successfully unprovisioned component %s", op.id)
            return ProvisioningStatus(status=Status1.COMPLETED, result="")
        except ServiceError as se:
            return SystemErr(error=se.error_msg)

    def _get_public_info(self, op: KafkaOutputPort, schema_res: int | None) -> dict:
        public_info = dict()
        if isinstance(schema_res, int):
            public_info["schema_id"] = {
                "type": "string",
                "label": "Registered Schema ID",
                "value": str(schema_res),
            }
        public_info["topic_name"] = {
            "type": "string",
            "label": "Topic Name",
            "value": op.specific.topic.name,
        }
        public_info["num_partitions"] = {
            "type": "string",
            "label": "Number of partitions",
            "value": str(op.specific.topic.numPartitions),
        }
        public_info["replication_factor"] = {
            "type": "string",
            "label": "Replication factor",
            "value": str(op.specific.topic.replicationFactor),
        }
        return public_info

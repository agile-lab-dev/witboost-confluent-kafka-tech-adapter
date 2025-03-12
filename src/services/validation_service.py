from typing import Annotated, Tuple

import pydantic
from fastapi import Depends

from src.dependencies import UnpackedProvisioningRequestDep
from src.models.api_models import ValidationError
from src.models.data_product_descriptor import DataProduct
from src.models.kafka_models import KafkaOutputPort
from src.utility.logger import get_logger

logger = get_logger(__name__)


def validate_kafka_output_port(
    request: UnpackedProvisioningRequestDep,
) -> Tuple[DataProduct, KafkaOutputPort] | ValidationError:
    if isinstance(request, ValidationError):
        return request

    data_product, component_id = request

    try:
        component_to_provision = data_product.get_typed_component_by_id(
            component_id, KafkaOutputPort
        )
    except pydantic.ValidationError as ve:
        error_msg = (
            f"Failed to parse the component {component_id} as a Kafka OutputPort:"
        )
        logger.exception(error_msg)
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

    if component_to_provision is None:
        error_msg = f"Component with ID {component_id} not found in descriptor"
        logger.error(error_msg)
        return ValidationError(errors=[error_msg])

    return data_product, component_to_provision


ValidateKafkaOutputPortDep = Annotated[
    Tuple[DataProduct, KafkaOutputPort] | ValidationError,
    Depends(validate_kafka_output_port),
]

from pathlib import Path

from src.models.api_models import ValidationError
from src.models.data_product_descriptor import DataProduct
from src.services.validation_service import validate_kafka_output_port
from src.utility.parsing_pydantic_models import parse_yaml_with_model


def test_validate_kafka_output_port_valid():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_kafka_op_valid.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)

    actual_res = validate_kafka_output_port(
        (result, "urn:dmb:cmp:healthcare:vaccinations:0:kafka-output-port")
    )

    assert not isinstance(actual_res, ValidationError)


def test_validate_kafka_output_port_is_validation_error():
    result = ValidationError(errors=["error"])

    actual_res = validate_kafka_output_port(result)

    assert isinstance(actual_res, ValidationError)
    assert actual_res == result


def test_validate_kafka_output_port_not_valid():
    descriptor_str = Path(
        "tests/descriptors/data_product_with_kafka_op_not_valid.yaml"
    ).read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)

    actual_res = validate_kafka_output_port(
        (result, "urn:dmb:cmp:healthcare:vaccinations:0:kafka-output-port")
    )

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 4
    assert (
        "Failed to parse the component urn:dmb:cmp:healthcare:vaccinations:0:kafka-output-port as a Kafka OutputPort:"  # noqa: E501
        == actual_res.errors[0]
    )


def test_validate_kafka_output_port_missing():
    descriptor_str = Path("tests/descriptors/data_product_valid.yaml").read_text()
    result = parse_yaml_with_model(descriptor_str, DataProduct)
    assert not isinstance(result, ValidationError)
    assert isinstance(result, DataProduct)
    component_id = "urn:dmb:cmp:marketing:system-with-data-contract:0:kafka-op"

    actual_res = validate_kafka_output_port((result, component_id))

    assert isinstance(actual_res, ValidationError)
    assert len(actual_res.errors) == 1
    assert (
        f"Component with ID {component_id} not found in descriptor"
        == actual_res.errors[0]
    )

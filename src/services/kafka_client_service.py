from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import (
    AdminClient,
    ConfigResource,
    NewPartitions,
    NewTopic,
    ResourceType,
)

from src.models.service_error import ServiceError
from src.settings.kafka_settings import KafkaSettings
from src.utility.logger import get_logger


class KafkaClientServiceError(ServiceError):
    pass


class KafkaClientService:
    def __init__(
        self,
        kafka_settings: KafkaSettings,
    ):
        self.kafka_settings = kafka_settings
        self.admin_client = AdminClient(conf=kafka_settings.admin_client_config)
        self.logger = get_logger(__name__)

    def create_or_update_topic(
        self,
        topic_name: str,
        num_partitions: int,
        replication_factor: int,
        extra_config: dict[str, Any],
    ) -> None:
        """Creates or updates a Kafka topic with the specified settings.

        If the topic does not exist, it is created with the given number of partitions
        and replication factor. If the topic exists, its partition count and configuration
        settings are updated accordingly.

        Args:
            topic_name (str): Name of the Kafka topic.
            num_partitions (int): Number of partitions for the topic.
            replication_factor (int): Replication factor for the topic.
            extra_config (dict[str, Any]): Additional configuration settings for the topic.

        Raises:
            KafkaClientServiceError: If the topic creation or update fails.
        """
        try:
            topic_already_exists = any(
                t.topic == topic_name
                for t in self.admin_client.list_topics().topics.values()
            )
            if not topic_already_exists:
                new_topic = NewTopic(
                    topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication_factor,
                )
                fs = self.admin_client.create_topics([new_topic])
                for topic, future in fs.items():
                    future.result()
                    self.logger.info("Topic %s created", topic)
            self._manage_partitions(topic_name, num_partitions)
            self._manage_extra_config(topic_name, extra_config)
            return None
        except KafkaClientServiceError:
            raise
        except KafkaException as ke:
            details = str(ke) if len(getattr(ke, "args", ())) == 0 else ke.args[0].str()
            error_message = f"Failed to manage topic {topic_name}. Details: {details}"
            self.logger.exception(error_message)
            raise KafkaClientServiceError(error_message)
        except Exception as e:
            error_message = f"Failed to manage topic {topic_name}. Details: {str(e)}"
            self.logger.exception(error_message)
            raise KafkaClientServiceError(error_message)

    def delete_topic(
        self,
        topic_name: str,
    ) -> None:
        """Deletes a Kafka topic if it exists.

        Args:
            topic_name (str): Name of the Kafka topic to delete.

        Raises:
            KafkaClientServiceError: If the topic deletion fails.
        """
        try:
            topic_exists = any(
                t.topic == topic_name
                for t in self.admin_client.list_topics().topics.values()
            )
            if topic_exists:
                fs = self.admin_client.delete_topics([topic_name])
                for topic, f in fs.items():
                    f.result()
                    self.logger.info("Topic %s deleted", topic_name)
            return None
        except KafkaException as ke:
            details = str(ke) if len(getattr(ke, "args", ())) == 0 else ke.args[0].str()
            error_message = f"Failed to delete topic {topic_name}. Details: {details}"
            self.logger.exception(error_message)
            raise KafkaClientServiceError(error_message)
        except Exception as e:
            error_message = f"Failed to delete topic {topic_name}. Details: {str(e)}"
            self.logger.exception(error_message)
            raise KafkaClientServiceError(error_message)

    def _manage_partitions(self, topic_name: str, num_partitions: int) -> None:
        t = next(
            (
                t
                for t in self.admin_client.list_topics().topics.values()
                if t.topic == topic_name
            ),
            None,
        )
        if t is None:
            error_message = f"Topic {topic_name} not found"
            self.logger.error(error_message)
            raise KafkaClientServiceError(error_message)
        current_partition_count = len(t.partitions)
        if num_partitions > current_partition_count:
            new_parts = [NewPartitions(topic_name, num_partitions)]
            fs = self.admin_client.create_partitions(new_parts)
            for topic, future in fs.items():
                future.result()
                self.logger.info(
                    "Additional partitions created for topic %s, new total: %s",
                    topic,
                    num_partitions,
                )
        elif num_partitions < current_partition_count:
            error_message = f"Cannot decrease partitions for topic {topic_name}. Current partition count: {current_partition_count}, requested: {num_partitions}"  # noqa: E501
            self.logger.error(error_message)
            raise KafkaClientServiceError(error_message)
        return None

    def _manage_extra_config(self, topic_name: str, extra_config: dict[str, Any]):
        resource = ConfigResource(ResourceType.TOPIC, topic_name)
        for k, v in extra_config.items():
            resource.set_config(k, v)
        fs = self.admin_client.alter_configs([resource])
        for config_resource, future in fs.items():
            future.result()
            self.logger.info(
                "%s configuration successfully altered for topic %s",
                config_resource,
                topic_name,
            )

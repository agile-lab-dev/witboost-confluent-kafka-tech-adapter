from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaSettings(BaseSettings):
    admin_client_config: dict[str, Any]
    schema_registry_client_config: dict[str, Any]

    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="kafka_", extra="ignore"
    )

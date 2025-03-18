from typing import Protocol

from src.models.service_error import ServiceError


class PrincipalMappingServiceError(ServiceError):
    pass


class KafkaPrincipal:
    def __init__(self, principal: str):
        self.principal = principal


class PrincipalMappingService(Protocol):
    def map_identity(self, witboost_identity: str) -> KafkaPrincipal:
        """Maps a Witboost identity to a Kafka principal.

        Args:
            witboost_identity (str): The Witboost identity string (e.g., "user:john_doe").

        Returns:
            KafkaPrincipal: The corresponding Kafka principal.

        Raises:
            PrincipalMappingServiceError: If the mapping fails.
        """
        pass

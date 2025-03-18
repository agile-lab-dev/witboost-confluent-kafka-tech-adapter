from src.services.principal_mapping_service import (
    KafkaPrincipal,
    PrincipalMappingServiceError,
)
from src.utility.logger import get_logger


class SaslPlainPrincipalMappingService:
    def __init__(self):
        self.logger = get_logger(__name__)

    def map_identity(self, witboost_identity: str) -> KafkaPrincipal:
        if witboost_identity.startswith("user:"):
            _, _, identity = witboost_identity.partition("user:")
            # we assume that an user with the same name is defined in the jaas conf
            return KafkaPrincipal(f"User:{identity}")
        elif witboost_identity.startswith("group:"):
            error_message = "Groups are not supported by SASL Plain protocol."
            self.logger.error(error_message)
            raise PrincipalMappingServiceError(error_message)
        else:
            error_message = f"{witboost_identity} is not a valid Witboost identity."
            self.logger.error(error_message)
            raise PrincipalMappingServiceError(error_message)

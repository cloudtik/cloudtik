import logging
from typing import Any, Dict

from cloudtik.core._private.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class StorageProvider:
    """Interface for creating or deleting cloud storage services for a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom storage providers. It is not allowed to call into
    StorageProvider methods from any package outside.
    """

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str,
            storage_name: str) -> None:
        self.provider_config = provider_config
        self.workspace_name = workspace_name
        self.storage_name = storage_name

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        pass

    def delete(self, config: Dict[str, Any]):
        """Delete an object storage in the workspace based on the config.
        """
        pass

    def get_info(self, config: Dict[str, Any]):
        """Return the object storage information.
        Return None if the object storage doesn't exist
        """
        pass

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        pass

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

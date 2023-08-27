import logging
from typing import Any, Dict

from cloudtik.core.storage_provider import StorageProvider

logger = logging.getLogger(__name__)


class AliyunStorageProvider(StorageProvider):
    """Provider for creating or deleting cloud storage services for Alibaba Cloud.
    """

    def __init__(self, provider_config: Dict[str, Any],
                 storage_name: str) -> None:
        super().__init__(provider_config, storage_name)

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        pass

    def delete(self, config: Dict[str, Any]):
        """Delete a object storage in the workspace based on the config.
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

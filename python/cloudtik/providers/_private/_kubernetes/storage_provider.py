import logging
from typing import Any, Dict

from cloudtik.core.storage_provider import StorageProvider
from cloudtik.providers._private._kubernetes.config import create_cloud_storage_provider

logger = logging.getLogger(__name__)


class KubernetesStorageProvider(StorageProvider):
    """Provider for creating or deleting cloud storage services for Kubernetes.
    """

    def __init__(
            self, provider_config: Dict[str, Any],
            workspace_name: str, storage_name: str) -> None:
        super().__init__(provider_config, workspace_name, storage_name)
        self.cloud_storage_provider = create_cloud_storage_provider(
            provider_config, workspace_name, storage_name)

    def check_cloud_storage_provider(self):
        if self.cloud_storage_provider is None:
            raise RuntimeError(
                "No storage provider available with current configuration.")

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        self.check_cloud_storage_provider()
        self.cloud_storage_provider.create(config)

    def delete(self, config: Dict[str, Any]):
        """Delete an object storage in the workspace based on the config.
        """
        self.check_cloud_storage_provider()
        self.cloud_storage_provider.delete(config)

    def get_info(self, config: Dict[str, Any]):
        """Return the object storage information.
        Return None if the object storage doesn't exist
        """
        self.check_cloud_storage_provider()
        return self.cloud_storage_provider.get_info(config)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        self.check_cloud_storage_provider()
        self.cloud_storage_provider.validate_config(provider_config)

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

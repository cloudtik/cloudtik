import logging
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider
from cloudtik.providers._private._kubernetes.config import create_cloud_database_provider

logger = logging.getLogger(__name__)


class KubernetesDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for Kubernetes.
    """

    def __init__(self, provider_config: Dict[str, Any],
                 workspace_name: str, database_name: str) -> None:
        super().__init__(provider_config, workspace_name, database_name)
        self.cloud_database_provider = create_cloud_database_provider(
            provider_config, workspace_name, database_name)

    def check_cloud_database_provider(self):
        if self.cloud_database_provider is None:
            raise RuntimeError(
                "No database provider available with current configuration.")

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        self.check_cloud_database_provider()
        self.cloud_database_provider.create(config)

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        self.check_cloud_database_provider()
        self.cloud_database_provider.delete(config)

    def get_info(self, config: Dict[str, Any]):
        """Return the database instance information.
        Return None if the database instance doesn't exist
        """
        self.check_cloud_database_provider()
        return self.cloud_database_provider.get_info(config)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        self.check_cloud_database_provider()
        self.cloud_database_provider.validate_config(provider_config)

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

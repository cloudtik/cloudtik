import logging
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider

logger = logging.getLogger(__name__)


class KubernetesDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for Managed Kubernetes."""

    def __init__(self, provider_config: Dict[str, Any],
                 database_name: str) -> None:
        super().__init__(provider_config, database_name)

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        workspace_name = config["workspace_name"]
        pass

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        workspace_name = config["workspace_name"]
        pass

    def get_info(self, config: Dict[str, Any]):
        """Return the database instance information.
        Return None if the database instance doesn't exist
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

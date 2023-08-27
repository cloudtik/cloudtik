import logging
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider
from cloudtik.providers._private.gcp.config import _delete_managed_database_instance, \
    _create_managed_database_instance_in_workspace

logger = logging.getLogger(__name__)


class GCPDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for GCP."""

    def __init__(self, provider_config: Dict[str, Any],
                 database_name: str) -> None:
        super().__init__(provider_config, database_name)

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        workspace_name = config["workspace_name"]
        _create_managed_database_instance_in_workspace(
            self.provider_config, workspace_name, self.database_name)

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        workspace_name = config["workspace_name"]
        _delete_managed_database_instance(
            self.provider_config, workspace_name, self.database_name)

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
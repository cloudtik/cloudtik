import logging
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider
from cloudtik.providers._private._azure.config import _delete_managed_database_instance, \
    _create_managed_database_instance_in_workspace, _get_resource_group_name_of

logger = logging.getLogger(__name__)


class AzureDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for Azure."""

    def __init__(self, provider_config: Dict[str, Any],
                 workspace_name: str, database_name: str) -> None:
        super().__init__(provider_config, workspace_name, database_name)

    def get_resource_group_name(self, workspace_name):
        provider_config = self.provider_config
        return _get_resource_group_name_of(provider_config, workspace_name)

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        resource_group_name = self.get_resource_group_name(self.workspace_name)
        _create_managed_database_instance_in_workspace(
            self.provider_config, self.workspace_name,
            resource_group_name, self.database_name)

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        resource_group_name = self.get_resource_group_name(self.workspace_name)
        _delete_managed_database_instance(
            self.provider_config, self.workspace_name,
            resource_group_name, self.database_name)

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

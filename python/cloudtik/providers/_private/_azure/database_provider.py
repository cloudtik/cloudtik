import logging
import re
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider
from cloudtik.providers._private._azure.config import _delete_managed_database_instance, \
    _get_resource_group_name_of, _get_managed_cloud_database_info, \
    _create_managed_cloud_database, get_virtual_network_name

logger = logging.getLogger(__name__)

AZURE_DATABASE_NAME_MIN_LEN = 3
AZURE_DATABASE_NAME_MAX_LEN = 63


def check_database_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class AzureDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for Azure."""

    def __init__(self, provider_config: Dict[str, Any],
                 workspace_name: str, database_name: str) -> None:
        super().__init__(provider_config, workspace_name, database_name)

    def get_resource_group_name(self):
        return _get_resource_group_name_of(
            self.provider_config, self.workspace_name)

    def get_virtual_network_name(self):
        return get_virtual_network_name(
            self.provider_config, self.workspace_name)

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        resource_group_name = self.get_resource_group_name()
        virtual_network_name = self.get_virtual_network_name()
        _create_managed_cloud_database(
            self.provider_config, self.workspace_name,
            resource_group_name, virtual_network_name,
            self.database_name)

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        resource_group_name = self.get_resource_group_name()
        _delete_managed_database_instance(
            self.provider_config, self.workspace_name,
            resource_group_name, self.database_name)

    def get_info(self, config: Dict[str, Any]):
        """Return the database instance information.
        Return None if the database instance doesn't exist
        """
        resource_group_name = self.get_resource_group_name()
        return _get_managed_cloud_database_info(
            self.provider_config, self.workspace_name,
            resource_group_name, self.database_name)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        if (len(self.database_name) < AZURE_DATABASE_NAME_MIN_LEN or
                len(self.database_name) > AZURE_DATABASE_NAME_MAX_LEN or
                not check_database_name_format(self.database_name)):
            raise RuntimeError(
                "{} database instance name is between {} and {} characters, "
                "and can only contain lowercase alphanumeric "
                "characters, and dashes (-)".format(
                    provider_config["type"],
                    AZURE_DATABASE_NAME_MIN_LEN,
                    AZURE_DATABASE_NAME_MAX_LEN))

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

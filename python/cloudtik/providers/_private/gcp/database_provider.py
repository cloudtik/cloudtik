import logging
import re
from typing import Any, Dict

from cloudtik.core.database_provider import DatabaseProvider
from cloudtik.providers._private.gcp.config import _delete_managed_database_instance, \
    _get_managed_cloud_database_info, _create_managed_cloud_database, \
    get_gcp_vpc_name

logger = logging.getLogger(__name__)

GCP_DATABASE_NAME_MIN_LEN = 1
GCP_DATABASE_NAME_MAX_LEN = 63


def check_database_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class GCPDatabaseProvider(DatabaseProvider):
    """Provider for creating or deleting cloud database services for GCP."""

    def __init__(
            self, provider_config: Dict[str, Any],
            workspace_name: str, database_name: str) -> None:
        super().__init__(
            provider_config, workspace_name, database_name)

    def get_vpc_name(self):
        return get_gcp_vpc_name(
            self.provider_config, self.workspace_name)

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        vpc_name = self.get_vpc_name()
        _create_managed_cloud_database(
            self.provider_config, self.workspace_name, vpc_name,
            self.database_name)

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
        _delete_managed_database_instance(
            self.provider_config, self.workspace_name, self.database_name)

    def get_info(self, config: Dict[str, Any]):
        """Return the database instance information.
        Return None if the database instance doesn't exist
        """
        return _get_managed_cloud_database_info(
            self.provider_config, self.workspace_name, self.database_name)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        if (len(self.database_name) < GCP_DATABASE_NAME_MIN_LEN or
                len(self.database_name) > GCP_DATABASE_NAME_MAX_LEN or
                not check_database_name_format(self.database_name)):
            raise RuntimeError(
                "{} database instance name is between {} and {} characters, "
                "and can only contain lowercase alphanumeric "
                "characters, and dashes (-)".format(
                    provider_config["type"],
                    GCP_DATABASE_NAME_MIN_LEN,
                    GCP_DATABASE_NAME_MAX_LEN))

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

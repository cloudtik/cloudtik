import logging
from typing import Any, Dict

from cloudtik.core._private.annotations import DeveloperAPI

logger = logging.getLogger(__name__)


@DeveloperAPI
class DatabaseProvider:
    """Interface for creating or deleting cloud database services for a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom database providers. It is not allowed to call into
    DatabaseProvider methods from any package outside.
    """

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str,
            database_name: str) -> None:
        self.provider_config = provider_config
        self.workspace_name = workspace_name
        self.database_name = database_name

    def create(self, config: Dict[str, Any]):
        """Create the database instance in the workspace based on the config."""
        pass

    def delete(self, config: Dict[str, Any]):
        """Delete a database instance in the workspace based on the config.
        """
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

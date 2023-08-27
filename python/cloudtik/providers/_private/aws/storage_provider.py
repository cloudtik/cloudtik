import logging
import re
from typing import Any, Dict

from cloudtik.core.storage_provider import StorageProvider
from cloudtik.providers._private.aws.config import _create_managed_cloud_storage, _delete_managed_cloud_storage, \
    _get_managed_cloud_storage_info

logger = logging.getLogger(__name__)


AWS_STORAGE_NAME_MIN_LEN = 3
AWS_STORAGE_NAME_MAX_LEN = 63


def check_storage_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-\.]*$", workspace_name))


class AWSStorageProvider(StorageProvider):
    """Provider for creating or deleting cloud storage services for AWS.
    """

    def __init__(self, provider_config: Dict[str, Any],
                 storage_name: str) -> None:
        super().__init__(provider_config, storage_name)

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        workspace_name = config["workspace_name"]
        _create_managed_cloud_storage(
            self.provider_config, workspace_name, self.storage_name)

    def delete(self, config: Dict[str, Any]):
        """Delete a object storage in the workspace based on the config.
        """
        workspace_name = config["workspace_name"]
        _delete_managed_cloud_storage(
            self.provider_config, workspace_name, self.storage_name)

    def get_info(self, config: Dict[str, Any]):
        """Return the object storage information.
        Return None if the object storage doesn't exist
        """
        workspace_name = config["workspace_name"]
        return _get_managed_cloud_storage_info(
            self.provider_config, workspace_name, self.storage_name)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        if (len(self.storage_name) < AWS_STORAGE_NAME_MIN_LEN or
                len(self.storage_name) > AWS_STORAGE_NAME_MAX_LEN or
                not check_storage_name_format(self.storage_name)):
            raise RuntimeError(
                "{} storage name is between {} and {} characters, "
                "and can only contain lowercase alphanumeric "
                "characters, dots and dashes".format(
                    provider_config["type"], AWS_STORAGE_NAME_MIN_LEN, AWS_STORAGE_NAME_MAX_LEN))

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

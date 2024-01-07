import logging
import re
from typing import Any, Dict

from cloudtik.core.storage_provider import StorageProvider
from cloudtik.providers._private.aliyun.config import _create_managed_cloud_storage, _delete_managed_cloud_storage, \
    _get_managed_cloud_storage_info

logger = logging.getLogger(__name__)


ALIYUN_STORAGE_NAME_MIN_LEN = 3
ALIYUN_STORAGE_NAME_MAX_LEN = 63


def check_storage_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class AliyunStorageProvider(StorageProvider):
    """Provider for creating or deleting cloud storage services for Alibaba Cloud.
    """

    def __init__(
            self, provider_config: Dict[str, Any],
            workspace_name: str, storage_name: str) -> None:
        super().__init__(provider_config, workspace_name, storage_name)

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        _create_managed_cloud_storage(
            self.provider_config, self.workspace_name, self.storage_name)

    def delete(self, config: Dict[str, Any]):
        """Delete an object storage in the workspace based on the config.
        """
        _delete_managed_cloud_storage(
            self.provider_config, self.workspace_name, self.storage_name)

    def get_info(self, config: Dict[str, Any]):
        """Return the object storage information.
        Return None if the object storage doesn't exist
        """
        return _get_managed_cloud_storage_info(
            self.provider_config, self.workspace_name, self.storage_name)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        if (len(self.storage_name) < ALIYUN_STORAGE_NAME_MIN_LEN or
                len(self.storage_name) > ALIYUN_STORAGE_NAME_MAX_LEN or
                not check_storage_name_format(self.storage_name)):
            raise RuntimeError(
                "{} storage name is between {} and {} characters, "
                "and can only contain lowercase alphanumeric "
                "characters, and dashes (-)".format(
                    provider_config["type"],
                    ALIYUN_STORAGE_NAME_MIN_LEN,
                    ALIYUN_STORAGE_NAME_MAX_LEN))

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

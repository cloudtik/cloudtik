import logging
import re
from typing import Any, Dict

from cloudtik.core.storage_provider import StorageProvider
from cloudtik.providers._private.huaweicloud.config import \
    _create_managed_cloud_storage, _delete_managed_cloud_storage, \
    _get_managed_cloud_storage_info
from cloudtik.providers._private.huaweicloud.utils import make_obs_client

logger = logging.getLogger(__name__)


HUAWEICLOUD_STORAGE_NAME_MIN_LEN = 3
HUAWEICLOUD_STORAGE_NAME_MAX_LEN = 63


def check_storage_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-\.]*$", workspace_name))


class HUAWEICLOUDStorageProvider(StorageProvider):
    """Provider for creating or deleting cloud storage services for HUAWEI CLOUD.
    """

    def __init__(
            self, provider_config: Dict[str, Any],
            workspace_name: str, storage_name: str) -> None:
        super().__init__(
            provider_config, workspace_name, storage_name)

    def create(self, config: Dict[str, Any]):
        """Create the object storage in the workspace based on the config."""
        obs_client = make_obs_client(self.provider_config)
        _create_managed_cloud_storage(
            obs_client, self.workspace_name, self.storage_name)

    def delete(self, config: Dict[str, Any]):
        """Delete an object storage in the workspace based on the config.
        """
        obs_client = make_obs_client(self.provider_config)
        _delete_managed_cloud_storage(
            obs_client, self.workspace_name, self.storage_name)

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
        if (len(self.storage_name) < HUAWEICLOUD_STORAGE_NAME_MIN_LEN or
                len(self.storage_name) > HUAWEICLOUD_STORAGE_NAME_MAX_LEN or
                not check_storage_name_format(self.storage_name)):
            raise RuntimeError(
                "{} storage name is between {} and {} characters, "
                "and can only contain lowercase alphanumeric "
                "characters, dashes (-) and dots (.)".format(
                    provider_config["type"],
                    HUAWEICLOUD_STORAGE_NAME_MIN_LEN,
                    HUAWEICLOUD_STORAGE_NAME_MAX_LEN))

    @staticmethod
    def bootstrap_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return config

import logging
import json
from typing import Any, Dict

from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.provider_factory import _get_provider_config_object, _import_external

logger = logging.getLogger(__name__)

# For caching storage provider instantiations across API calls of one python session
_storage_provider_instances = ConcurrentObjectCache()


def _import_aws_storage(provider_config):
    from cloudtik.providers._private.aws.storage_provider import AWSStorageProvider
    return AWSStorageProvider


def _import_gcp_storage(provider_config):
    from cloudtik.providers._private.gcp.storage_provider import GCPStorageProvider
    return GCPStorageProvider


def _import_azure_storage(provider_config):
    from cloudtik.providers._private._azure.storage_provider import AzureStorageProvider
    return AzureStorageProvider


def _import_aliyun_storage(provider_config):
    from cloudtik.providers._private.aliyun.storage_provider import AliyunStorageProvider
    return AliyunStorageProvider


def _import_kubernetes_storage(provider_config):
    from cloudtik.providers._private._kubernetes.storage_provider import \
        KubernetesStorageProvider
    return KubernetesStorageProvider


def _import_huaweicloud_storage(provider_config):
    from cloudtik.providers._private.huaweicloud.storage_provider import \
        HUAWEICLOUDStorageProvider
    return HUAWEICLOUDStorageProvider


_STORAGE_PROVIDERS = {
    "aws": _import_aws_storage,
    "gcp": _import_gcp_storage,
    "azure": _import_azure_storage,
    "aliyun": _import_aliyun_storage,
    "kubernetes": _import_kubernetes_storage,
    "huaweicloud": _import_huaweicloud_storage,
    "external": _import_external  # Import an external module
}


def _get_storage_provider_cls(provider_config: Dict[str, Any]):
    """Get the storage provider class for a given provider config.

    Note that this may be used by private storage providers that proxy methods to
    built-in storage providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the storage config.

    Returns:
        StorageProvider class
    """
    importer = _STORAGE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported storage provider: {}".format(
                provider_config["type"]))
    return importer(provider_config)


def _get_storage_provider(
        provider_config: Dict[str, Any],
        workspace_name: str,
        storage_name: str,
        use_cache: bool = True) -> Any:
    """Get the instantiated storage provider for a given provider config.

    Note that this may be used by private storage providers that proxy methods to
    built-in storage providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.
        workspace_name: workspace name from the cluster config.
        storage_name: storage name from the cluster config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        StorageProvider
    """
    def load_storage_provider(
            provider_config: Dict[str, Any], workspace_name: str, storage_name: str):
        provider_cls = _get_storage_provider_cls(provider_config)
        return provider_cls(provider_config, workspace_name, storage_name)

    if not use_cache:
        return load_storage_provider(
            provider_config, workspace_name, storage_name)

    provider_key = (json.dumps(provider_config, sort_keys=True), workspace_name, storage_name)
    return _storage_provider_instances.get(
        provider_key, load_storage_provider,
        provider_config=provider_config,
        workspace_name=workspace_name,
        storage_name=storage_name)


def _clear_storage_provider_cache():
    _storage_provider_instances.clear()


def _get_default_storage_config(provider_config):
    return _get_provider_config_object(provider_config, "storage-defaults")

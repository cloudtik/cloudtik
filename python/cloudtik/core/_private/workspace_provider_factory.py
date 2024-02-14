import logging
import json
from typing import Any, Dict

from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.provider_factory import _get_provider_config_object, _import_external

logger = logging.getLogger(__name__)

# For caching workspace provider instantiations across API calls of one python session
_workspace_provider_instances = ConcurrentObjectCache()


def _import_aws_workspace(provider_config):
    from cloudtik.providers._private.aws.workspace_provider import AWSWorkspaceProvider
    return AWSWorkspaceProvider


def _import_gcp_workspace(provider_config):
    from cloudtik.providers._private.gcp.workspace_provider import GCPWorkspaceProvider
    return GCPWorkspaceProvider


def _import_azure_workspace(provider_config):
    from cloudtik.providers._private._azure.workspace_provider import AzureWorkspaceProvider
    return AzureWorkspaceProvider


def _import_aliyun_workspace(provider_config):
    from cloudtik.providers._private.aliyun.workspace_provider import AliyunWorkspaceProvider
    return AliyunWorkspaceProvider


def _import_onpremise_workspace(provider_config):
    from cloudtik.providers._private.onpremise.workspace_provider import \
        OnPremiseWorkspaceProvider
    return OnPremiseWorkspaceProvider


def _import_local_workspace(provider_config):
    from cloudtik.providers._private.local.workspace_provider import \
        LocalWorkspaceProvider
    return LocalWorkspaceProvider


def _import_virtual_workspace(provider_config):
    from cloudtik.providers._private.virtual.workspace_provider import \
        VirtualWorkspaceProvider
    return VirtualWorkspaceProvider


def _import_kubernetes_workspace(provider_config):
    from cloudtik.providers._private._kubernetes.workspace_provider import \
        KubernetesWorkspaceProvider
    return KubernetesWorkspaceProvider


def _import_huaweicloud_workspace(provider_config):
    from cloudtik.providers._private.huaweicloud.workspace_provider import \
        HUAWEICLOUDWorkspaceProvider
    return HUAWEICLOUDWorkspaceProvider


_WORKSPACE_PROVIDERS = {
    "onpremise": _import_onpremise_workspace,
    "local": _import_local_workspace,
    "virtual": _import_virtual_workspace,
    "aws": _import_aws_workspace,
    "gcp": _import_gcp_workspace,
    "azure": _import_azure_workspace,
    "aliyun": _import_aliyun_workspace,
    "kubernetes": _import_kubernetes_workspace,
    "huaweicloud": _import_huaweicloud_workspace,
    "external": _import_external  # Import an external module
}


def _get_workspace_provider_cls(provider_config: Dict[str, Any]):
    """Get the workspace provider class for a given provider config.

    Note that this may be used by private workspace providers that proxy methods to
    built-in workspace providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the workspace config.

    Returns:
        WorkspaceProvider class
    """
    importer = _WORKSPACE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported workspace provider: {}".format(
                provider_config["type"]))
    return importer(provider_config)


def _get_workspace_provider(
        provider_config: Dict[str, Any],
        workspace_name: str,
        use_cache: bool = True) -> Any:
    """Get the instantiated workspace provider for a given provider config.

    Note that this may be used by private workspace providers that proxy methods to
    built-in workspace providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.
        workspace_name: workspace name from the cluster config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        WorkspaceProvider
    """
    def load_workspace_provider(
            provider_config: Dict[str, Any], workspace_name: str):
        provider_cls = _get_workspace_provider_cls(provider_config)
        return provider_cls(provider_config, workspace_name)

    if not use_cache:
        return load_workspace_provider(provider_config, workspace_name)

    provider_key = (json.dumps(provider_config, sort_keys=True), workspace_name)
    return _workspace_provider_instances.get(
        provider_key, load_workspace_provider,
        provider_config=provider_config, workspace_name=workspace_name)


def _clear_workspace_provider_cache():
    _workspace_provider_instances.clear()


def _get_default_workspace_config(provider_config):
    return _get_provider_config_object(
        provider_config, "workspace-defaults")

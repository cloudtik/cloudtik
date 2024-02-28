import logging
import json
from typing import Any, Dict

from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.provider_factory import _get_provider_config_object, _import_external

logger = logging.getLogger(__name__)

"""Load Balancer provider"""

# For caching load balancer provider instantiations across API calls of one python session
_load_balancer_provider_instances = ConcurrentObjectCache()


def _import_aws_load_balancer(provider_config):
    from cloudtik.providers._private.aws.load_balancer_provider import AWSLoadBalancerProvider
    return AWSLoadBalancerProvider


def _import_azure_load_balancer(provider_config):
    from cloudtik.providers._private._azure.load_balancer_provider import AzureLoadBalancerProvider
    return AzureLoadBalancerProvider


def _import_gcp_load_balancer(provider_config):
    from cloudtik.providers._private.gcp.load_balancer_provider import GCPLoadBalancerProvider
    return GCPLoadBalancerProvider


_LOAD_BALANCER_PROVIDERS = {
    "aws": _import_aws_load_balancer,
    "azure": _import_azure_load_balancer,
    "gcp": _import_gcp_load_balancer,
    "external": _import_external  # Import an external module
}


def _get_load_balancer_provider_cls(provider_config: Dict[str, Any]):
    """Get the load balancer provider class for a given provider config.

    Note that this may be used by private load balancer providers that proxy methods to
    built-in load balancer providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the load balancer config.

    Returns:
        LoadBalancerProvider class
    """
    importer = _LOAD_BALANCER_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported load balancer provider: {}".format(
                provider_config["type"]))
    return importer(provider_config)


def _get_load_balancer_provider(
        provider_config: Dict[str, Any],
        workspace_name: str,
        use_cache: bool = True) -> Any:
    """Get the instantiated load balancer provider for a given provider config.

    Note that this may be used by private load balancer providers that proxy methods to
    built-in load balancer providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.
        workspace_name: workspace name from the cluster config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        LoadBalancerProvider
    """
    def load_load_balancer_provider(
            provider_config: Dict[str, Any], workspace_name: str):
        provider_cls = _get_load_balancer_provider_cls(provider_config)
        return provider_cls(provider_config, workspace_name)

    if not use_cache:
        return load_load_balancer_provider(
            provider_config, workspace_name)

    provider_key = (json.dumps(provider_config, sort_keys=True), workspace_name)
    return _load_balancer_provider_instances.get(
        provider_key, load_load_balancer_provider,
        provider_config=provider_config,
        workspace_name=workspace_name)


def _clear_load_balancer_provider_cache():
    _load_balancer_provider_instances.clear()


def _get_default_load_balancer_config(provider_config):
    return _get_provider_config_object(provider_config, "load-balancer-defaults")

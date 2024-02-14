import logging
import json
import os
from typing import Any, Dict
import inspect
import yaml

from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.util.core_utils import load_class

logger = logging.getLogger(__name__)

"""Node provider"""

# For caching provider instantiations across API calls of one python session
_node_provider_instances = ConcurrentObjectCache()


def _import_aws(provider_config):
    from cloudtik.providers._private.aws.node_provider import AWSNodeProvider
    return AWSNodeProvider


def _import_gcp(provider_config):
    from cloudtik.providers._private.gcp.node_provider import GCPNodeProvider
    return GCPNodeProvider


def _import_azure(provider_config):
    from cloudtik.providers._private._azure.node_provider import AzureNodeProvider
    return AzureNodeProvider


def _import_aliyun(provider_config):
    from cloudtik.providers._private.aliyun.node_provider import AliyunNodeProvider
    return AliyunNodeProvider


def _import_onpremise(provider_config):
    from cloudtik.providers._private.onpremise.node_provider import (
        OnPremiseNodeProvider)
    return OnPremiseNodeProvider


def _import_local(provider_config):
    from cloudtik.providers._private.local.node_provider import (
        LocalNodeProvider)
    return LocalNodeProvider


def _import_virtual(provider_config):
    from cloudtik.providers._private.virtual.node_provider import (
        VirtualNodeProvider)
    return VirtualNodeProvider


def _import_kubernetes(provider_config):
    from cloudtik.providers._private._kubernetes.node_provider import \
        KubernetesNodeProvider
    return KubernetesNodeProvider


def _import_huaweicloud(provider_config):
    from cloudtik.providers._private.huaweicloud.node_provider import \
        HUAWEICLOUDNodeProvider
    return HUAWEICLOUDNodeProvider


def _load_onpremise_provider_home():
    import cloudtik.providers.onpremise as onpremise_provider
    return os.path.dirname(onpremise_provider.__file__)


def _load_local_provider_home():
    import cloudtik.providers.local as local_provider
    return os.path.dirname(local_provider.__file__)


def _load_virtual_provider_home():
    import cloudtik.providers.virtual as virtual_provider
    return os.path.dirname(virtual_provider.__file__)


def _load_kubernetes_provider_home():
    import cloudtik.providers.kubernetes as kubernetes_provider
    return os.path.dirname(kubernetes_provider.__file__)


def _load_aws_provider_home():
    import cloudtik.providers.aws as aws_provider
    return os.path.dirname(aws_provider.__file__)


def _load_gcp_provider_home():
    import cloudtik.providers.gcp as gcp_provider
    return os.path.dirname(gcp_provider.__file__)


def _load_azure_provider_home():
    import cloudtik.providers.azure as azure_provider
    return os.path.dirname(azure_provider.__file__)


def _load_aliyun_provider_home():
    import cloudtik.providers.aliyun as aliyun_provider
    return os.path.dirname(aliyun_provider.__file__)


def _load_huaweicloud_provider_home():
    import cloudtik.providers.huaweicloud as huaweicloud_provider
    return os.path.dirname(huaweicloud_provider.__file__)


def _import_external(provider_config):
    provider_cls = load_class(path=provider_config["provider_class"])
    return provider_cls


_NODE_PROVIDERS = {
    "onpremise": _import_onpremise,  # Run clusters with on-premise nodes using cloud simulating
    "local": _import_local,  # Run a cluster on multiple local nodes
    "virtual": _import_virtual,  # Run virtual clusters with docker containers on single node
    "aws": _import_aws,
    "gcp": _import_gcp,
    "azure": _import_azure,
    "aliyun": _import_aliyun,
    "kubernetes": _import_kubernetes,
    "huaweicloud": _import_huaweicloud,
    "external": _import_external  # Import an external module
}

_PROVIDER_PRETTY_NAMES = {
    "onpremise": "On-Premise",
    "local": "Local",
    "virtual": "Virtual",
    "aws": "AWS",
    "gcp": "GCP",
    "azure": "Azure",
    "aliyun": "Aliyun",
    "kubernetes": "Kubernetes",
    "huaweicloud": "HUAWEI CLOUD",
    "external": "External"
}

# for external providers, we assume the provider home is
# the folder contains the provider class
_PROVIDER_HOMES = {
    "onpremise": _load_onpremise_provider_home,
    "local": _load_local_provider_home,
    "virtual": _load_virtual_provider_home,
    "aws": _load_aws_provider_home,
    "gcp": _load_gcp_provider_home,
    "azure": _load_azure_provider_home,
    "aliyun": _load_aliyun_provider_home,
    "kubernetes": _load_kubernetes_provider_home,
    "huaweicloud": _load_huaweicloud_provider_home,
}


def _get_node_provider_cls(provider_config: Dict[str, Any]):
    """Get the node provider class for a given provider config.

    Note that this may be used by private node providers that proxy methods to
    built-in node providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.

    Returns:
        NodeProvider class
    """
    importer = _NODE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError("Unsupported node provider: {}".format(
            provider_config["type"]))
    return importer(provider_config)


def _get_node_provider(
        provider_config: Dict[str, Any],
        cluster_name: str,
        use_cache: bool = True) -> Any:
    """Get the instantiated node provider for a given provider config.

    Note that this may be used by private node providers that proxy methods to
    built-in node providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.
        cluster_name: cluster name from the cluster config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        NodeProvider
    """
    def load_node_provider(
            provider_config: Dict[str, Any], cluster_name: str):
        provider_cls = _get_node_provider_cls(provider_config)
        return provider_cls(provider_config, cluster_name)

    if not use_cache:
        return load_node_provider(provider_config, cluster_name)

    provider_key = (json.dumps(provider_config, sort_keys=True), cluster_name)
    return _node_provider_instances.get(
        provider_key, load_node_provider,
        provider_config=provider_config, cluster_name=cluster_name)


def _clear_provider_cache():
    _node_provider_instances.clear()


def _get_default_config(provider_config):
    return _get_provider_config_object(provider_config, "defaults")


def _get_default_provider_home(provider_config):
    provider_cls = _get_node_provider_cls(provider_config)
    provider_module = inspect.getmodule(provider_cls)
    return os.path.dirname(provider_module.__file__)


def _get_provider_config_object(provider_config, object_name: str):
    if not object_name.endswith(".yaml"):
        object_name += ".yaml"

    load_config_home = _PROVIDER_HOMES.get(provider_config["type"])
    if load_config_home is None:
        # if there is no home registry, we use the default logic
        path_to_home = _get_default_provider_home(provider_config)
    else:
        path_to_home = load_config_home()
    path_to_config_file = os.path.join(path_to_home, object_name)
    # if the config object file doesn't exist, from global defaults
    if not os.path.exists(path_to_config_file):
        return {"from": object_name}
    else:
        with open(path_to_config_file) as f:
            config_object = yaml.safe_load(f) or {}
        return config_object

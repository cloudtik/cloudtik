from enum import Enum, auto
from typing import Optional, Dict, Any, List, Union

from cloudtik.core._private.util.core_utils import deserialize_config, serialize_config, \
    get_list_for_update, get_config_for_update, get_config_copy

# The standard keys and values used for service discovery
SERVICE_DISCOVERY_SERVICE_TYPE = "service_type"

SERVICE_DISCOVERY_PROTOCOL = "protocol"
SERVICE_DISCOVERY_PROTOCOL_TCP = "tcp"
SERVICE_DISCOVERY_PROTOCOL_HTTP = "http"

SERVICE_DISCOVERY_PORT = "port"

SERVICE_DISCOVERY_NODE_KIND = "node_kind"
SERVICE_DISCOVERY_NODE_KIND_HEAD = "head"
SERVICE_DISCOVERY_NODE_KIND_WORKER = "worker"
SERVICE_DISCOVERY_NODE_KIND_NODE = "node"

SERVICE_DISCOVERY_TAGS = "tags"
SERVICE_DISCOVERY_LABELS = "labels"

SERVICE_DISCOVERY_TAG_CLUSTER_PREFIX = "cloudtik-c-"
SERVICE_DISCOVERY_TAG_FEATURE_PREFIX = "cloudtik-f-"
SERVICE_DISCOVERY_TAG_SYSTEM_PREFIX = "cloudtik-"

SERVICE_DISCOVERY_LABEL_PROTOCOL = "protocol"

SERVICE_DISCOVERY_LABEL_CLUSTER = "cloudtik-cluster"
SERVICE_DISCOVERY_LABEL_SEQ_ID = "cloudtik-seq-id"
SERVICE_DISCOVERY_LABEL_NODE_ID = "cloudtik-node-id"

SERVICE_DISCOVERY_LABEL_RUNTIME = "cloudtik-runtime"
# The service type label
SERVICE_DISCOVERY_LABEL_SERVICE = "cloudtik-service"

SERVICE_DISCOVERY_CHECK_INTERVAL = "check_interval"
SERVICE_DISCOVERY_CHECK_TIMEOUT = "check_timeout"

# A set of predefined features that a service belongs to
# A single service can provide more than one features
SERVICE_DISCOVERY_FEATURES = "features"

SERVICE_DISCOVERY_FEATURE_DATABASE = "database"
SERVICE_DISCOVERY_FEATURE_STORAGE = "storage"
SERVICE_DISCOVERY_FEATURE_ANALYTICS = "analytics"
SERVICE_DISCOVERY_FEATURE_SCHEDULER = "scheduler"
SERVICE_DISCOVERY_FEATURE_DNS = "dns"
SERVICE_DISCOVERY_FEATURE_LOAD_BALANCER = "load-balancer"
SERVICE_DISCOVERY_FEATURE_KEY_VALUE = "key-value"
SERVICE_DISCOVERY_FEATURE_METRICS = "metrics"
SERVICE_DISCOVERY_FEATURE_MESSAGING = "messaging"
SERVICE_DISCOVERY_FEATURE_API_GATEWAY = "api-gateway"

# Standard runtime configurations for service discovery
SERVICE_DISCOVERY_CONFIG_SERVICE_DISCOVERY = "service"
SERVICE_DISCOVERY_CONFIG_PREFIX = "prefix"
SERVICE_DISCOVERY_CONFIG_TAGS = "tags"
SERVICE_DISCOVERY_CONFIG_LABELS = "labels"
SERVICE_DISCOVERY_CONFIG_SERVICE_TYPE = "service_type"
SERVICE_DISCOVERY_CONFIG_PREFER_WORKSPACE = "prefer_workspace"

# The config keys for a standard service selector
SERVICE_SELECTOR_SERVICES = "services"
SERVICE_SELECTOR_SERVICE_TYPES = "service_types"
SERVICE_SELECTOR_TAGS = "tags"
SERVICE_SELECTOR_LABELS = "labels"
SERVICE_SELECTOR_EXCLUDE_LABELS = "exclude_labels"
SERVICE_SELECTOR_EXCLUDE_JOINED_LABELS = "exclude_joined_labels"
SERVICE_SELECTOR_RUNTIMES = "runtimes"
SERVICE_SELECTOR_CLUSTERS = "clusters"


class ServiceAddressType(Enum):
    NODE_IP = auto()
    NODE_FQDN = auto()
    NODE_SQDN = auto()
    SERVICE_FQDN = auto()

    def __str__(self):
        return self.name

    @staticmethod
    def from_str(address_type_str):
        if address_type_str == "NODE_IP":
            return ServiceAddressType.NODE_IP
        elif address_type_str == "NODE_FQDN":
            return ServiceAddressType.NODE_FQDN
        elif address_type_str == "NODE_SQDN":
            return ServiceAddressType.NODE_SQDN
        elif address_type_str == "SERVICE_FQDN":
            return ServiceAddressType.SERVICE_FQDN
        else:
            options = [e.name for e in ServiceAddressType]
            raise ValueError("Unsupported address type: {} (Select from: {})".format(
                address_type_str, options))


class ServiceScope(Enum):
    """The service scope decide how the canonical service name is formed.
    For workspace scoped service, the runtime service name is used directly
    as the service name and the cluster name as a tag.
    For cluster scoped service, the cluster name will be prefixed with the
    runtime service name to form a unique canonical service name.

    """
    WORKSPACE = 1
    CLUSTER = 2


class ServiceRegisterException(RuntimeError):
    pass


def get_service_discovery_config(config):
    return config.get(SERVICE_DISCOVERY_CONFIG_SERVICE_DISCOVERY, {})


def get_service_discovery_config_for_update(config):
    return get_config_for_update(
        config, SERVICE_DISCOVERY_CONFIG_SERVICE_DISCOVERY)


def get_service_discovery_config_copy(config):
    return get_config_copy(config, SERVICE_DISCOVERY_CONFIG_SERVICE_DISCOVERY)


def get_service_type_override(
        service_discovery_config: Optional[Dict[str, Any]]):
    return service_discovery_config.get(
        SERVICE_DISCOVERY_CONFIG_SERVICE_TYPE)


def set_service_type_override(
        service_discovery_config: Dict[str, Any], service_type):
    service_discovery_config[SERVICE_DISCOVERY_CONFIG_SERVICE_TYPE] = service_type


def set_service_discovery_label(
        service_discovery_config: Dict[str, Any], name, value):
    if not name:
        return
    labels = get_config_for_update(
        service_discovery_config, SERVICE_DISCOVERY_CONFIG_LABELS)
    labels[name] = str(value)


def is_prefer_workspace_discovery(
        service_discovery_config: Optional[Dict[str, Any]]):
    return service_discovery_config.get(
        SERVICE_DISCOVERY_CONFIG_PREFER_WORKSPACE, False)


def get_canonical_service_name(
        service_discovery_config: Optional[Dict[str, Any]],
        cluster_name,
        service_type,
        service_scope: ServiceScope = ServiceScope.WORKSPACE):
    prefix = service_discovery_config.get(
        SERVICE_DISCOVERY_CONFIG_PREFIX)
    if prefix:
        # service name with a customized prefix
        return "{}-{}".format(prefix, service_type)
    else:
        if service_scope == ServiceScope.WORKSPACE:
            return service_type
        else:
            # cluster name as prefix of service name
            return "{}-{}".format(cluster_name, service_type)


def define_runtime_service(
        service_type: str,
        service_discovery_config: Optional[Dict[str, Any]],
        service_port,
        node_kind=SERVICE_DISCOVERY_NODE_KIND_NODE,
        protocol: str = None,
        features: List[str] = None):
    if not protocol:
        protocol = SERVICE_DISCOVERY_PROTOCOL_TCP
    service_def = {
        SERVICE_DISCOVERY_SERVICE_TYPE: service_type,
        SERVICE_DISCOVERY_PROTOCOL: protocol,
        SERVICE_DISCOVERY_PORT: service_port,
    }

    if node_kind and node_kind != SERVICE_DISCOVERY_NODE_KIND_NODE:
        service_def[SERVICE_DISCOVERY_NODE_KIND] = node_kind

    tags = service_discovery_config.get(SERVICE_DISCOVERY_CONFIG_TAGS)
    if tags:
        service_def[SERVICE_DISCOVERY_TAGS] = tags
    labels = service_discovery_config.get(SERVICE_DISCOVERY_CONFIG_LABELS)
    if labels:
        service_def[SERVICE_DISCOVERY_LABELS] = labels
    if features:
        service_def[SERVICE_DISCOVERY_FEATURES] = features

    return service_def


def define_runtime_service_on_worker(
        service_type: str,
        service_discovery_config: Optional[Dict[str, Any]],
        service_port,
        protocol: str = None,
        features: List[str] = None):
    return define_runtime_service(
        service_type,
        service_discovery_config,
        service_port,
        node_kind=SERVICE_DISCOVERY_NODE_KIND_WORKER,
        protocol=protocol,
        features=features)


def define_runtime_service_on_head(
        service_type: str,
        service_discovery_config,
        service_port,
        protocol: str = None,
        features: List[str] = None):
    return define_runtime_service(
        service_type,
        service_discovery_config,
        service_port,
        node_kind=SERVICE_DISCOVERY_NODE_KIND_HEAD,
        protocol=protocol,
        features=features)


def define_runtime_service_on_head_or_all(
        service_type: str,
        service_discovery_config,
        service_port, head_or_all,
        protocol: str = None,
        features: List[str] = None):
    node_kind = SERVICE_DISCOVERY_NODE_KIND_NODE \
        if head_or_all else SERVICE_DISCOVERY_NODE_KIND_HEAD
    return define_runtime_service(
        service_type,
        service_discovery_config,
        service_port,
        node_kind=node_kind,
        protocol=protocol,
        features=features)


def match_service_node(runtime_service, head):
    node_kind = runtime_service.get(SERVICE_DISCOVERY_NODE_KIND)
    return match_node_kind(node_kind, head)


def match_node_kind(node_kind, head):
    if not node_kind or node_kind == SERVICE_DISCOVERY_NODE_KIND_NODE:
        return True
    if head:
        if node_kind == SERVICE_DISCOVERY_NODE_KIND_HEAD:
            return True
    else:
        if node_kind == SERVICE_DISCOVERY_NODE_KIND_WORKER:
            return True
    return False


def get_runtime_service_features(runtime_service):
    return runtime_service.get(SERVICE_DISCOVERY_FEATURES)


def has_runtime_service_feature(runtime_service, feature):
    features = get_runtime_service_features(runtime_service)
    return False if not features or feature not in features else True


def serialize_service_selector(service_selector):
    if not service_selector:
        return None
    return serialize_config(service_selector)


def deserialize_service_selector(service_selector_str):
    if not service_selector_str:
        return None
    return deserialize_config(service_selector_str)


def exclude_runtime_of_cluster(
        service_selector, runtime, cluster_name):
    if not (runtime or cluster_name):
        return service_selector
    if service_selector is None:
        service_selector = {}
    exclude_joined_labels = get_list_for_update(
        service_selector, SERVICE_SELECTOR_EXCLUDE_JOINED_LABELS)

    joined_labels = {}
    if runtime:
        joined_labels[SERVICE_DISCOVERY_LABEL_RUNTIME] = runtime
    if cluster_name:
        joined_labels[SERVICE_DISCOVERY_LABEL_CLUSTER] = cluster_name

    exclude_joined_labels.append(joined_labels)
    return service_selector


def get_service_selector_copy(config, config_key):
    return get_config_copy(config, config_key)


def include_list_for_selector(
        service_selector, list_name,
        list_value: Union[str, List[str]], override=False):
    if service_selector is None:
        service_selector = {}
    list_values = get_list_for_update(
        service_selector, list_name)
    if list_values:
        if not override:
            return service_selector
        list_values.clear()

    if isinstance(list_value, str):
        list_values.append(list_value)
    else:
        # list of values
        for item in list_value:
            list_values.append(item)
    return service_selector


def include_cluster_for_selector(
        service_selector,
        cluster_name: Union[str, List[str]], override=False):
    return include_list_for_selector(
        service_selector, SERVICE_SELECTOR_CLUSTERS,
        cluster_name, override)


def include_runtime_for_selector(
        service_selector, runtime: Union[str, List[str]], override=False):
    return include_list_for_selector(
        service_selector, SERVICE_SELECTOR_RUNTIMES,
        runtime, override)


def include_service_type_for_selector(
        service_selector, service_type: Union[str, List[str]], override=False):
    return include_list_for_selector(
        service_selector, SERVICE_SELECTOR_SERVICE_TYPES,
        service_type, override)


def include_service_name_for_selector(
        service_selector, service_name: Union[str, List[str]], override=False):
    return include_list_for_selector(
        service_selector, SERVICE_SELECTOR_SERVICES,
        service_name, override)


def include_feature_for_selector(service_selector, feature):
    tags = get_list_for_update(
        service_selector, SERVICE_SELECTOR_TAGS)
    feature_tag = SERVICE_DISCOVERY_TAG_FEATURE_PREFIX + feature
    if feature_tag not in tags:
        tags.append(feature_tag)
    return service_selector


def include_runtime_service_for_selector(
        service_selector,
        runtime_type: Optional[Union[str, List[str]]] = None,
        service_type: Optional[Union[str, List[str]]] = None):
    if runtime_type:
        # if user provide runtimes in the selector, we don't override it
        # because any runtimes in the list will be selected
        service_selector = include_runtime_for_selector(
            service_selector, runtime_type)
    if service_type:
        service_selector = include_service_type_for_selector(
            service_selector, service_type)
    return service_selector

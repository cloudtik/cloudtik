import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_ELASTICSEARCH
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, define_runtime_service_on_head
from cloudtik.core._private.utils import is_node_seq_id_enabled, enable_node_seq_id

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["elasticsearch", True, "ElasticSearch Server", "node"],
    ]

ELASTICSEARCH_SERVICE_PORT_CONFIG_KEY = "port"
ELASTICSEARCH_TRANSPORT_PORT_CONFIG_KEY = "transport_port"

ELASTICSEARCH_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
ELASTICSEARCH_CLUSTER_MODE_NONE = "none"
# cluster
ELASTICSEARCH_CLUSTER_MODE_CLUSTER = "cluster"

ELASTICSEARCH_PASSWORD_CONFIG_KEY = "password"


ELASTICSEARCH_SERVICE_TYPE = BUILT_IN_RUNTIME_ELASTICSEARCH
ELASTICSEARCH_SERVICE_PORT_DEFAULT = 9200
ELASTICSEARCH_TRANSPORT_PORT_DEFAULT = 9300

ELASTICSEARCH_PASSWORD_DEFAULT = "cloudtik"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_ELASTICSEARCH, {})


def _get_service_port(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_SERVICE_PORT_CONFIG_KEY, ELASTICSEARCH_SERVICE_PORT_DEFAULT)


def _get_transport_port(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_TRANSPORT_PORT_CONFIG_KEY, ELASTICSEARCH_TRANSPORT_PORT_DEFAULT)


def _get_cluster_mode(elasticsearch_config: Dict[str, Any]):
    return elasticsearch_config.get(
        ELASTICSEARCH_CLUSTER_MODE_CONFIG_KEY, ELASTICSEARCH_CLUSTER_MODE_CLUSTER)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_ELASTICSEARCH)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"elasticsearch": logs_dir}


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    elasticsearch_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(elasticsearch_config)
    if cluster_mode != ELASTICSEARCH_CLUSTER_MODE_NONE:
        # We must enable the node seq id (stable seq id is preferred)
        # But we don't enforce it.
        if not is_node_seq_id_enabled(cluster_config):
            enable_node_seq_id(cluster_config)

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    pass


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    elasticsearch_config = _get_config(runtime_config)

    service_port = _get_service_port(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_SERVICE_PORT"] = service_port

    transport_port = _get_service_port(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_TRANSPORT_PORT"] = transport_port

    cluster_mode = _get_cluster_mode(elasticsearch_config)
    runtime_envs["ELASTICSEARCH_CLUSTER_MODE"] = cluster_mode

    password = elasticsearch_config.get(
        ELASTICSEARCH_PASSWORD_CONFIG_KEY, ELASTICSEARCH_PASSWORD_DEFAULT)
    runtime_envs["ELASTICSEARCH_PASSWORD"] = password

    return runtime_envs


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "elasticsearch": {
            "name": "ElasticSearch",
            "url": "{}:{}".format(head_host, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "elasticsearch": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    elasticsearch_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(elasticsearch_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, ELASTICSEARCH_SERVICE_TYPE)
    service_port = _get_service_port(elasticsearch_config)

    def define_elasticsearch_service(define_fn, service_type=None):
        if not service_type:
            service_type = ELASTICSEARCH_SERVICE_TYPE
        return define_fn(
            service_type,
            service_discovery_config, service_port)

    cluster_mode = _get_cluster_mode(elasticsearch_config)
    if cluster_mode == ELASTICSEARCH_CLUSTER_MODE_CLUSTER:
        services = {
            service_name: define_elasticsearch_service(define_runtime_service),
        }
    else:
        # single standalone on head
        services = {
            service_name: define_elasticsearch_service(define_runtime_service_on_head),
        }
    return services

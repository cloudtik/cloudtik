from shlex import quote

from cloudtik.core._private.service_discovery.utils import serialize_service_selector
from cloudtik.core._private.util.core_utils import exec_with_output, JSONSerializableObject
from cloudtik.core._private.util.runtime_utils import \
    get_runtime_config_from_node, get_runtime_cluster_name
from cloudtik.core._private.utils import get_runtime_types
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.loadbalancer.utils import _get_config, _get_backend_config, \
    _get_logs_dir, _get_backend_service_selector, _get_service_identifier

LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)

    # TODO:


def start_controller(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)

    backend_config = _get_backend_config(load_balancer_config)
    cluster_name = get_runtime_cluster_name()
    service_selector = _get_backend_service_selector(
        backend_config, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.loadbalancer.controller.LoadBalancerController"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    cmd += ["interval={}".format(
        LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]

    runtime_types = get_runtime_types(runtime_config)
    if runtime_types:
        runtime_types_str = ",".join(runtime_types)
        cmd += ["runtime_types={}".format(
            quote(runtime_types_str))]
    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_controller():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)


class LoadBalancerBackendService(JSONSerializableObject):
    def __init__(
            self, service_name, backend_servers, frontend_port):
        self.service_name = service_name
        self.backend_servers = backend_servers
        self.frontend_port = frontend_port


def update_backend_configuration(backends):
    # TODO: calling provider API for update the backend
    pass

from shlex import quote

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_LOAD_BALANCER
from cloudtik.core._private.service_discovery.utils import serialize_service_selector
from cloudtik.core._private.util.core_utils import exec_with_output, serialize_config
from cloudtik.core._private.util.runtime_utils import \
    get_runtime_config_from_node, get_runtime_cluster_name, get_runtime_workspace_name
from cloudtik.runtime.common.leader_election.runtime_leader_election import get_runtime_leader_election_url
from cloudtik.runtime.common.utils import stop_pull_service_by_identifier
from cloudtik.runtime.loadbalancer.provider_api import get_load_balancer_manager
from cloudtik.runtime.loadbalancer.utils import _get_config, _get_backend_config, \
    _get_logs_dir, _get_backend_service_selector, _get_service_identifier, _get_provider_config

LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL = 15


###################################
# Calls from node when configuring
###################################


def configure_backend(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)
    provider_config = _get_provider_config(load_balancer_config)

    # TODO: build backends based on static configuration
    backends = {}

    workspace_name = get_runtime_workspace_name()
    load_balancer_manager = get_load_balancer_manager(
        provider_config, workspace_name)
    load_balancer_manager.update(backends)


def start_controller(head):
    runtime_config = get_runtime_config_from_node(head)
    load_balancer_config = _get_config(runtime_config)

    backend_config = _get_backend_config(load_balancer_config)
    cluster_name = get_runtime_cluster_name()
    workspace_name = get_runtime_workspace_name()
    service_selector = _get_backend_service_selector(
        backend_config, cluster_name)
    service_selector_str = serialize_service_selector(service_selector)

    service_identifier = _get_service_identifier()
    logs_dir = _get_logs_dir()

    cmd = ["cloudtik", "node", "service", service_identifier, "start"]
    cmd += ["--service-class=cloudtik.runtime.loadbalancer.controller.LoadBalancerController"]
    cmd += ["--logs-dir={}".format(quote(logs_dir))]

    # job parameters
    coordinator_url = get_runtime_leader_election_url(
        runtime_config, BUILT_IN_RUNTIME_LOAD_BALANCER)
    if coordinator_url:
        cmd += ["coordinator_url={}".format(
            quote(coordinator_url))]
    cmd += ["interval={}".format(
        LOAD_BALANCER_DISCOVER_BACKEND_SERVERS_INTERVAL)]
    if service_selector_str:
        cmd += ["service_selector={}".format(service_selector_str)]

    provider_config = _get_provider_config(load_balancer_config)
    provider_config_str = serialize_config(provider_config) if provider_config else None
    if provider_config_str:
        cmd += ["provider_config={}".format(provider_config_str)]
    if workspace_name:
        cmd += ["workspace_name={}".format(workspace_name)]

    cmd_str = " ".join(cmd)
    exec_with_output(cmd_str)


def stop_controller():
    service_identifier = _get_service_identifier()
    stop_pull_service_by_identifier(service_identifier)

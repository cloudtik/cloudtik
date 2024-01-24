import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_XINETD, _get_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_service_discovery_config, match_node_kind
from cloudtik.core._private.util.core_utils import get_config_for_update
from cloudtik.core._private.utils import get_runtime_config_for_update, get_available_node_types, \
    get_head_node_type, _get_node_type_specific_runtime_config, get_runtime_types
from cloudtik.runtime.common.health_check import HEALTH_CHECK_NODE_KIND, HEALTH_CHECK_PORT

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["xinetd", True, "xinetd", "node"],
    ]


XINETD_SERVICE_TYPE = BUILT_IN_RUNTIME_XINETD

CONFIG_KEY_HEALTH_CHECKS = "health_checks"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_XINETD, {})


def _get_config_for_update(cluster_config):
    runtime_config = get_runtime_config_for_update(cluster_config)
    return get_config_for_update(runtime_config, BUILT_IN_RUNTIME_XINETD)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_XINETD)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_XINETD: logs_dir}


def _get_runtime_health_checks_by_node_type(config: Dict[str, Any]):
    # for all the runtimes, query its services per node type
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)

    health_checks_map = {}
    for node_type in available_node_types:
        head = True if node_type == head_node_type else False
        health_checks_for_node_type = {}

        runtime_config = _get_node_type_specific_runtime_config(config, node_type)
        if runtime_config:
            runtime_types = get_runtime_types(runtime_config)
            for runtime_type in runtime_types:
                if runtime_type == BUILT_IN_RUNTIME_XINETD:
                    continue

                runtime = _get_runtime(runtime_type, runtime_config)
                health_check = runtime.get_health_check(config)
                if not health_check:
                    continue

                port = health_check.get(HEALTH_CHECK_PORT)
                if not port:
                    # no port provided, skip
                    continue

                node_kind = health_check.get(HEALTH_CHECK_NODE_KIND)
                if match_node_kind(node_kind, head):
                    health_checks_for_node_type[runtime_type] = health_check
        if health_checks_for_node_type:
            health_checks_map[node_type] = health_checks_for_node_type
    return health_checks_map


def _bootstrap_runtime_health_checks(config: Dict[str, Any]):
    # for all the runtimes, query its health checks per node type
    health_check_configs = _get_runtime_health_checks_by_node_type(config)
    if health_check_configs:
        xinetd_config = _get_config_for_update(config)
        xinetd_config[CONFIG_KEY_HEALTH_CHECKS] = health_check_configs

    return config


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    return runtime_envs


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    xinetd_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(xinetd_config)
    services = {}
    # TODO: export services that running with xinetd
    return services

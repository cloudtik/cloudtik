import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_XINETD
from cloudtik.core._private.service_discovery.utils import \
    get_service_discovery_config

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["xinetd", True, "xinetd", "node"],
    ]


XINETD_SERVICE_TYPE = BUILT_IN_RUNTIME_XINETD


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_XINETD, {})


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_XINETD)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_XINETD: logs_dir}


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}
    xinetd_config = _get_config(runtime_config)

    return runtime_envs


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    xinetd_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(xinetd_config)

    services = {}
    # TODO: export services that running with xinetd
    return services

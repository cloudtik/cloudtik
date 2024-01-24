import logging
import os
from io import StringIO

from cloudtik.core._private.runtime_factory import _get_runtime_home
from cloudtik.core._private.util.core_utils import get_current_user
from cloudtik.core._private.util.runtime_utils import get_runtime_node_type, get_runtime_config_from_node
from cloudtik.runtime.common.health_check import HEALTH_CHECK_PORT, HEALTH_CHECK_SCRIPT
from cloudtik.runtime.xinetd.utils import _get_home_dir, _get_config, CONFIG_KEY_HEALTH_CHECKS

logger = logging.getLogger(__name__)

SERVICE_NAME_TEMPLATE = "{}-health-check"


###################################
# Calls from node when configuring
###################################


def configure_health_checks(head):
    """This method is called from configure.py script which is running on node.
    """
    runtime_config = get_runtime_config_from_node(head)

    # Configure the health checks
    _configure_health_checks(runtime_config)


def _configure_health_checks(runtime_config):
    node_type = get_runtime_node_type()
    health_checks_config = _get_health_checks_of_node_type(
        runtime_config, node_type)

    home_dir = _get_home_dir()
    config_dir = os.path.join(home_dir, "conf", "xinetd.d")

    # before generate the files, remove all the existing
    _remove_existing_service_files(config_dir)
    _generate_service_files(config_dir, health_checks_config)


def _remove_existing_service_files(config_dir):
    if not os.path.isdir(config_dir):
        return
    for x in os.listdir(config_dir):
        full_path = os.path.join(config_dir, x)
        if os.path.isfile(full_path):
            os.remove(full_path)


def _get_health_checks_of_node_type(runtime_config, node_type):
    xinetd_config = _get_config(runtime_config)
    health_checks_map = xinetd_config.get(CONFIG_KEY_HEALTH_CHECKS)
    if not health_checks_map:
        return None
    return health_checks_map.get(node_type)


def _generate_service_files(config_dir, health_checks_config):
    if not health_checks_config:
        return

    # generate the health checks service configuration files
    os.makedirs(config_dir, exist_ok=True)
    for runtime_type, health_check_config in health_checks_config.items():
        _generate_health_check(
            config_dir, runtime_type, health_check_config)


def _generate_health_check(config_dir, runtime_type, health_check_config):
    service_name = SERVICE_NAME_TEMPLATE.format(runtime_type)
    service_file = os.path.join(config_dir, service_name)
    with open(service_file, "w") as f:
        service_def = _generate_service_def(
            runtime_type, health_check_config)
        f.write(service_def)


def _generate_service_def(runtime_type, health_check_config):
    """
    service runtime_type-health-check
    {
            disable         = no
            flags           = REUSE
            socket_type     = stream
            type            = UNLISTED
            protocol        = tcp
            port            = 8080
            wait            = no
            user            = cloudtik
            server          = /path/to/health/check/script.sh
            log_on_failure  += USERID
            only_from       = 0.0.0.0/0
            per_source      = UNLIMITED
    }
    """
    service_name = SERVICE_NAME_TEMPLATE.format(runtime_type)
    port = health_check_config.get(HEALTH_CHECK_PORT)
    script = health_check_config.get(HEALTH_CHECK_SCRIPT)
    if not script:
        script = os.path.join("scripts", f"{service_name}.sh")
    if os.path.isabs(script):
        full_script_path = script
    else:
        runtime_home_dir = _get_runtime_home(runtime_type)
        full_script_path = os.path.join(runtime_home_dir, script)

    user = get_current_user()

    service_str = StringIO()
    service_str.write(f'service {service_name}\n')
    service_str.write('{\n')
    service_str.write('        disable         = no\n')
    service_str.write('        flags           = REUSE\n')
    service_str.write('        socket_type     = stream\n')
    service_str.write('        type            = UNLISTED\n')
    service_str.write('        protocol        = tcp\n')
    service_str.write(f'        port            = {port}\n')
    service_str.write('        wait            = no\n')
    service_str.write(f'        user            = {user}\n')
    service_str.write(f'        server          = {full_script_path}\n')
    service_str.write('        log_on_failure  += USERID\n')
    service_str.write('        only_from       = 0.0.0.0/0\n')
    service_str.write('        per_source      = UNLIMITED\n')
    service_str.write('}\n')

    return service_str.getvalue()

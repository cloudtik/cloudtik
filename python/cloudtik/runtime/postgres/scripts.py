import logging
import os

import click

from cloudtik.core._private.cli_logger import (add_click_logging_options, cli_logger)
from cloudtik.core._private.cluster.cluster_config import _load_cluster_config
from cloudtik.core._private.cluster.cluster_operator import cli_call_context, _exec_cmd_on_cluster
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_POSTGRES, BUILT_IN_RUNTIME_SSHSERVER
from cloudtik.core._private.util.core_utils import exec_with_output
from cloudtik.core._private.utils import load_head_cluster_config, with_verbose_option, run_system_command, \
    get_runtime_config, is_runtime_enabled
from cloudtik.runtime.postgres.utils import _get_home_dir, _get_config, _get_repmgr_config, _is_repmgr_enabled
from cloudtik.scripts.utils import NaturalOrderGroup

logger = logging.getLogger(__name__)


def _is_switchover_allowed(config):
    runtime_config = get_runtime_config(config)
    if not is_runtime_enabled(
            runtime_config, BUILT_IN_RUNTIME_SSHSERVER):
        return False
    postgres_config = _get_config(runtime_config)
    repmgr_config = _get_repmgr_config(postgres_config)
    repmgr_enabled = _is_repmgr_enabled(repmgr_config)
    if not repmgr_enabled:
        return False
    return True


def _get_repmgr_config_file():
    home_dir = _get_home_dir()
    return os.path.join(
        home_dir, "conf", "repmgr.conf")


def _get_current_node_role():
    rpgmgr_config_file = _get_repmgr_config_file()
    cmds = [
        "repmgr",
        "-f",
        rpgmgr_config_file,
        "node",
        "status"
    ]
    final_cmd = " ".join(cmds)
    output = exec_with_output(
        final_cmd
    ).decode().strip()

    node_role = "unknown"
    if not output:
        return node_role

    lines = output.splitlines()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith("Role:"):
            role_parts = line.split(':')
            if len(role_parts) >= 2:
                node_role = role_parts[1].strip()
            return node_role
    return node_role


def _get_switchover_command():
    # repmgr standby switchover -f repmgr.conf --siblings-follow -force-rewind
    rpgmgr_config_file = _get_repmgr_config_file()
    cmds = [
        "repmgr",
        "-f",
        rpgmgr_config_file,
        "standby",
        "switchover",
        "--siblings-follow",
        "--force-rewind"
    ]
    return " ".join(cmds)


def _check_switchover(config):
    if not _is_switchover_allowed(config):
        cli_logger.abort(
            "Switchover is not allowed for current configuration.\n"
            "1. SSH Server runtime must be configured for passwordless SSH to workers.\n"
            "2. Repmgr must be configured for Postgres.")


def _switchover(config):
    _check_switchover(config)

    call_context = cli_call_context()
    # soft kill, we need to do on head
    cmds = [
        "cloudtik",
        "head",
        BUILT_IN_RUNTIME_POSTGRES,
        "switchover",
    ]
    with_verbose_option(cmds, call_context)
    final_cmd = " ".join(cmds)

    _exec_cmd_on_cluster(
        config,
        call_context=call_context,
        cmd=final_cmd)


def _switchover_on_head(config):
    _check_switchover(config)

    # check whether current head is a standby
    node_role = _get_current_node_role()
    if node_role != "standby":
        cli_logger.abort(
            "Switchover can only be done when head node is standby.\n"
            "Current head node role: {}.".format(
                node_role))

    final_cmd = _get_switchover_command()
    run_system_command(final_cmd)


@click.group(cls=NaturalOrderGroup)
def postgres():
    """
    Commands for Postgres runtime.
    """
    pass


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@add_click_logging_options
def switchover(cluster_config_file, cluster_name):
    """Switchover the head from standby to primary role."""
    config = _load_cluster_config(cluster_config_file, cluster_name)
    _switchover(config)


postgres.add_command(switchover)


@click.group(name=BUILT_IN_RUNTIME_POSTGRES, cls=NaturalOrderGroup)
def postgres_on_head():
    """
    Commands running on head for Postgres runtime.
    """
    pass


@click.command(name='switchover')
@add_click_logging_options
def switchover_on_head():
    """Switchover the head from standby to primary role."""
    config = load_head_cluster_config()
    _switchover_on_head(config)


postgres_on_head.add_command(switchover_on_head)

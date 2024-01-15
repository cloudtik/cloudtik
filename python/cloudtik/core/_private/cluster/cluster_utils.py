from typing import Any, Dict
import subprocess
from types import ModuleType

from cloudtik.core._private.call_context import CallContext
from cloudtik.core._private.node.node_updater import NodeUpdaterThread
from cloudtik.core._private.utils import get_running_head_node, _get_node_specific_runtime_config, \
    _get_node_specific_docker_config, get_runtime_encryption_key, with_runtime_encryption_key, \
    with_head_node_ip_environment_variables, get_node_cluster_ip, get_node_provider_of


def create_node_updater_for_exec(
        config,
        call_context: CallContext,
        node_id,
        provider,
        start_commands,
        is_head_node: bool = False,
        head_node: str = None,
        use_internal_ip: bool = False,
        runtime_config: Dict[str, Any] = None,
        process_runner: ModuleType = subprocess,
        environment_variables=None,
        with_env: bool = False):
    if runtime_config is None:
        runtime_config = _get_node_specific_runtime_config(
            config, provider, node_id)
    docker_config = _get_node_specific_docker_config(
            config, provider, node_id)

    if head_node:
        # if head node is passed, update the is head node based on the fact
        is_head_node = True if node_id == head_node else False

    if with_env and not is_head_node:
        # for workers, head node ip and encryption secrets is needed
        if not head_node:
            head_node = get_running_head_node(
                config, _provider=provider,
                _allow_uninitialized_state=True)
        head_node_ip = get_node_cluster_ip(provider, head_node)
        environment_variables = with_head_node_ip_environment_variables(
            head_node_ip, environment_variables)
        encryption_key = get_runtime_encryption_key(config)
        environment_variables = with_runtime_encryption_key(
            encryption_key, environment_variables)

    updater = NodeUpdaterThread(
        config=config,
        call_context=call_context,
        node_id=node_id,
        provider_config=config["provider"],
        provider=provider,
        auth_config=config["auth"],
        cluster_name=config["cluster_name"],
        file_mounts=config["file_mounts"],
        initialization_commands=[],
        setup_commands=[],
        start_commands=start_commands,
        runtime_hash="",
        file_mounts_contents_hash="",
        is_head_node=is_head_node,
        process_runner=process_runner,
        use_internal_ip=use_internal_ip,
        rsync_options={
            "rsync_exclude": config.get("rsync_exclude"),
            "rsync_filter": config.get("rsync_filter")
        },
        docker_config=docker_config,
        runtime_config=runtime_config,
        environment_variables=environment_variables)
    return updater


def run_on_cluster(
        config: Dict[str, Any],
        call_context: CallContext,
        cmd: str = None,
        run_env: str = "auto",
        with_output: bool = False,
        _allow_uninitialized_state: bool = False,
        with_env: bool = False) -> str:
    head_node = get_running_head_node(
        config,
        _allow_uninitialized_state=_allow_uninitialized_state)

    return run_on_node(
        config=config,
        call_context=call_context,
        node_id=head_node,
        cmd=cmd,
        run_env=run_env,
        with_output=with_output,
        is_head_node=True,
        with_env=with_env
    )


def run_on_node(
        config: Dict[str, Any],
        call_context: CallContext,
        node_id: str,
        cmd: str = None,
        run_env: str = "auto",
        with_output: bool = False,
        is_head_node: bool = False,
        with_env: bool = False) -> str:
    use_internal_ip = config.get("bootstrapped", False)
    provider = get_node_provider_of(config)

    updater = create_node_updater_for_exec(
        config=config,
        call_context=call_context,
        node_id=node_id,
        provider=provider,
        start_commands=[],
        is_head_node=is_head_node,
        use_internal_ip=use_internal_ip,
        with_env=with_env)

    environment_variables = None
    if with_env:
        environment_variables = updater.get_update_environment_variables()

    exec_out = updater.cmd_executor.run(
        cmd,
        with_output=with_output,
        run_env=run_env,
        environment_variables=environment_variables)
    if with_output:
        return exec_out.decode(encoding="utf-8")
    else:
        return exec_out

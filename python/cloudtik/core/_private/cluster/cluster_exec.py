import logging
from typing import Any, Dict

from cloudtik.core._private.call_context import CallContext
from cloudtik.core._private.cluster.cluster_utils import create_node_updater_for_exec
from cloudtik.core._private.utils import get_running_head_node, get_node_provider_of
from cloudtik.core.node_provider import NodeProvider

logger = logging.getLogger(__name__)


def _exec_on_node(
        config: Dict[str, Any],
        call_context: CallContext,
        node_id: str,
        provider: NodeProvider,
        *,
        cmd: str = None,
        run_env: str = "auto",
        with_output: bool = False,
        is_head_node: bool = False,
        use_internal_ip: bool = True,
        with_env: bool = False
) -> str:
    """Runs a command on a node of a cluster
    """
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


def exec_on_head(
        config: Dict[str, Any],
        call_context: CallContext,
        node_id: str,
        *,
        cmd: str = None,
        run_env: str = "auto",
        with_output: bool = False,
        with_env: bool = False) -> str:
    """Runs a command on the head of the cluster.
    """
    provider = get_node_provider_of(config)
    return _exec_on_node(
        config, call_context, node_id, provider,
        cmd=cmd, run_env=run_env, with_output=with_output,
        is_head_node=False, use_internal_ip=True,
        with_env=with_env
    )


def exec_cluster(
        config: Dict[str, Any],
        call_context: CallContext,
        *,
        cmd: str = None,
        run_env: str = "auto",
        with_output: bool = False,
        _allow_uninitialized_state: bool = True,
        with_env: bool = False) -> str:
    """Runs a command on the head of the cluster.
    """
    use_internal_ip = config.get("bootstrapped", False)
    provider = get_node_provider_of(config)
    head_node = get_running_head_node(
        config,
        _provider=provider,
        _allow_uninitialized_state=_allow_uninitialized_state)

    return _exec_on_node(
        config, call_context, head_node, provider,
        cmd=cmd, run_env=run_env, with_output=with_output,
        is_head_node=True, use_internal_ip=use_internal_ip,
        with_env=with_env
    )


def _rsync_with_node(
    config: Dict[str, Any],
    call_context: CallContext,
    node_id,
    provider: NodeProvider,
    source, target,
    down: bool,
    is_head_node=False,
    use_internal_ip: bool = True
):
    if not source or not target:
        raise ValueError("Must specify the source and target to rsync.")

    updater = create_node_updater_for_exec(
        config=config,
        call_context=call_context,
        node_id=node_id,
        provider=provider,
        start_commands=[],
        is_head_node=is_head_node,
        use_internal_ip=use_internal_ip)
    if down:
        rsync = updater.rsync_down
    else:
        rsync = updater.rsync_up

    if call_context.cli_logger.verbosity > 0:
        call_context.set_output_redirected(False)
        call_context.set_rsync_silent(False)
    rsync(source, target, False)


def rsync_on_head(
    config: Dict[str, Any],
    call_context: CallContext,
    node_id,
    source, target,
    down: bool
):
    provider = get_node_provider_of(config)
    _rsync_with_node(
        config, call_context, node_id, provider,
        source, target, down,
        is_head_node=False,
        use_internal_ip=True
    )


def rsync_cluster(
    config: Dict[str, Any],
    call_context: CallContext,
    source, target,
    down: bool
):
    use_internal_ip = config.get("bootstrapped", False)
    provider = get_node_provider_of(config)
    head_node = get_running_head_node(
        config,
        _provider=provider)
    _rsync_with_node(
        config, call_context, head_node, provider,
        source, target, down,
        is_head_node=True,
        use_internal_ip=use_internal_ip
    )

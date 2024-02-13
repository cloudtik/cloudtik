import importlib
import logging
import os
import pkgutil
import shlex
import urllib
import urllib.error
import urllib.parse
from typing import Optional

import click

from cloudtik.core._private import constants
from cloudtik.core._private.util import logging_utils
from cloudtik.core._private.cli_logger import (
    add_click_logging_options, cli_logger, cf)
from cloudtik.core._private.cluster.cluster_config import _load_cluster_config
from cloudtik.core._private.cluster.cluster_operator import (
    attach_cluster, create_or_update_cluster, monitor_cluster,
    teardown_cluster, get_head_node_ip, kill_node_from_head, get_worker_node_ips,
    dump_cluster, RUN_ENV_TYPES,
    show_cluster_status, start_ssh_proxy, stop_ssh_proxy, cluster_debug_status,
    cluster_health_check, cluster_process_status, attach_worker, scale_cluster,
    exec_on_nodes, submit_and_exec, _wait_for_ready, _rsync, cli_call_context, cluster_resource_metrics,
    show_info, _run_script, cluster_logs, get_worker_node_hosts, get_head_node_host)
from cloudtik.core._private.util.core_utils import url_read
from cloudtik.core._private.utils import parse_bundles_json, parse_resources
from cloudtik.scripts.head_scripts import head
from cloudtik.scripts.node_scripts import node
from cloudtik.scripts.runtime_scripts import runtime
from cloudtik.scripts.utils import NaturalOrderGroup, add_command_alias, fail_command
from cloudtik.scripts.workspace import workspace
from cloudtik.scripts.storage import storage
from cloudtik.scripts.database import database

logger = logging.getLogger(__name__)


def _register_runtime_commands():
    try:
        _search_and_register_runtime_commands()
    except Exception:
        # Ignore the errors.
        # This may cause by import some unexpected module
        pass


def _search_and_register_runtime_commands():
    from cloudtik import runtime

    base_dir = os.path.dirname(runtime.__file__)
    for loader, module_name, is_pkg in pkgutil.iter_modules(
            runtime.__path__):
        # walk packages will return global packages not in the current path
        # if the name is also package in the global namespace
        if not is_pkg or "." in module_name:
            continue
        scripts_file = os.path.join(base_dir, module_name, "scripts.py")
        if not os.path.exists(scripts_file):
            continue

        scripts_module_name = runtime.__name__ + '.' + module_name + "." + "scripts"
        _module = importlib.import_module(scripts_module_name)
        if module_name in _module.__dict__:
            runtime_command_group = _module.__dict__[module_name]
            cli.add_command(runtime_command_group)


@click.group(cls=NaturalOrderGroup)
@click.option(
    "--logging-level",
    required=False,
    default=constants.LOGGER_LEVEL_INFO,
    type=str,
    help=constants.LOGGER_LEVEL_HELP)
@click.option(
    "--logging-format",
    required=False,
    default=constants.LOGGER_FORMAT,
    type=str,
    help=constants.LOGGER_FORMAT_HELP)
@click.version_option()
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    logging_utils.setup_logger(level, logging_format)
    cli_logger.set_format(format_tmpl=logging_format)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help="Override the configured min worker node count for the cluster.")
@click.option(
    "--max-workers",
    required=False,
    type=int,
    help="Override the configured max worker node count for the cluster.")
@click.option(
    "--no-restart",
    is_flag=True,
    default=False,
    help=("Whether to skip restarting services during the update. "
          "This avoids interrupting running jobs."))
@click.option(
    "--restart-only",
    is_flag=True,
    default=False,
    help=("Whether to skip running setup commands and only restart. "
          "This cannot be used with 'no-restart'."))
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--workspace-name",
    required=False,
    type=str,
    help="Override the workspace which will provide network service.")
@click.option(
    "--redirect-command-output",
    is_flag=True,
    default=False,
    help="Whether to redirect command output to a file.")
@click.option(
    "--use-login-shells/--use-normal-shells",
    is_flag=True,
    default=True,
    help=("We uses login shells (bash --login -i) to run cluster commands "
          "by default. If your workflow is compatible with normal shells, "
          "this can be disabled for a better user experience."))
@add_click_logging_options
def start(
        cluster_config_file, min_workers, max_workers, no_restart, restart_only,
        yes, cluster_name, workspace_name, redirect_command_output,
        use_login_shells):
    """Start or update a cluster."""
    if restart_only or no_restart:
        cli_logger.doassert(restart_only != no_restart,
                            "`{}` is incompatible with `{}`.",
                            cf.bold("--restart-only"), cf.bold("--no-restart"))
        assert restart_only != no_restart, "Cannot set both 'restart_only' " \
            "and 'no_restart' at the same time!"

    if urllib.parse.urlparse(cluster_config_file).scheme in ("http", "https"):
        try:
            content = url_read(cluster_config_file, timeout=5)
            file_name = cluster_config_file.split("/")[-1]
            with open(file_name, "wb") as f:
                f.write(content)
            cluster_config_file = file_name
        except urllib.error.HTTPError as e:
            cli_logger.warning("{}", str(e))
            cli_logger.warning(
                "Could not download remote cluster configuration file.")
    call_context = cli_call_context()
    create_or_update_cluster(
        config_file=cluster_config_file,
        call_context=call_context,
        override_min_workers=min_workers,
        override_max_workers=max_workers,
        no_restart=no_restart,
        restart_only=restart_only,
        yes=yes,
        override_cluster_name=cluster_name,
        override_workspace_name=workspace_name,
        no_config_cache=True,
        redirect_command_output=redirect_command_output,
        use_login_shells=use_login_shells)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--workers-only",
    is_flag=True,
    default=False,
    help="Only destroy the workers.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--keep-min-workers",
    is_flag=True,
    default=False,
    help="Retain the minimal amount of workers specified in the config.")
@click.option(
    "--hard",
    is_flag=True,
    default=False,
    help="Stop the cluster nodes by without running stop commands.")
@click.option(
    "--deep",
    is_flag=True,
    default=False,
    help="Do deep clean of all the resources such as permanent data volumes.")
@add_click_logging_options
def stop(
        cluster_config_file, yes, workers_only, cluster_name,
        keep_min_workers, hard, deep):
    """Stop a cluster."""
    teardown_cluster(
        cluster_config_file, yes, workers_only,
        cluster_name, keep_min_workers,
        proxy_stop=True,
        hard=hard,
        deep=deep)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--new", "-N", is_flag=True, help="Force creation of a new screen.")
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.option(
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node to attach to")
@click.option(
    "--host",
    is_flag=True,
    default=False,
    help="Attach to the host even running with docker.")
@click.option(
    "--with-env",
    is_flag=True,
    default=False,
    help="Run with updater environment variables.")
@add_click_logging_options
def attach(
        cluster_config_file, screen, tmux, cluster_name,
        no_config_cache, new, port_forward, node_ip, host,
        with_env):
    """Create or attach to SH session to a cluster or a worker node."""
    port_forward = [(port, port) for port in list(port_forward)]
    try:
        if not node_ip:
            # attach to the head
            attach_cluster(
                cluster_config_file,
                screen,
                tmux,
                cluster_name,
                no_config_cache=no_config_cache,
                new=new,
                port_forward=port_forward,
                force_to_host=host,
                with_env=with_env)
        else:
            # attach to the worker node
            attach_worker(
                cluster_config_file,
                node_ip,
                screen,
                tmux,
                cluster_name,
                no_config_cache=no_config_cache,
                new=new,
                port_forward=port_forward,
                force_to_host=host,
                with_env=with_env)
    except RuntimeError as re:
        fail_command("Failed to attach.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("cmd", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--run-env",
    required=False,
    type=click.Choice(RUN_ENV_TYPES),
    default="auto",
    help="Choose whether to execute this command in a container or directly on"
    " the cluster head. Only applies when docker is configured in the YAML.")
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help="Run the command in a screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.")
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--force-update",
    is_flag=True,
    default=False,
    help="Force update the cluster even if the cluster is running.")
@click.option(
    "--wait-for-workers",
    is_flag=True,
    default=False,
    help="Whether wait for minimum number of workers to be ready.")
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help="The minimum number of workers to wait for ready.")
@click.option(
    "--wait-timeout",
    required=False,
    type=int,
    help="The timeout seconds to wait for ready.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.option(
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node to exec command on")
@click.option(
    "--all-nodes/--no-all-nodes",
    is_flag=True,
    default=False,
    help="Whether to execute commands on all nodes.")
@click.option(
    "--parallel/--no-parallel",
    is_flag=True,
    default=True,
    help="Whether the run the commands on nodes in parallel.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--job-waiter",
    required=False,
    type=str,
    help="The job waiter to be used to check the completion of the job.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Do even if the head is not in healthy state.")
@click.option(
    "--with-env",
    is_flag=True,
    default=False,
    help="Run with updater environment variables.")
@add_click_logging_options
def exec(
        cluster_config_file, cmd, cluster_name, run_env, screen, tmux, stop, start,
        force_update, wait_for_workers, min_workers, wait_timeout,
        no_config_cache, port_forward, node_ip, all_nodes, parallel, yes, job_waiter,
        force, with_env):
    """Execute a command via SSH on a cluster or a specified node."""
    port_forward = [(port, port) for port in list(port_forward)]

    try:
        # Don't use config cache so that we will run a full bootstrap needed for start
        if start:
            no_config_cache = True
        config = _load_cluster_config(
            cluster_config_file, cluster_name,
            no_config_cache=no_config_cache)
        exec_on_nodes(
            config,
            call_context=cli_call_context(),
            node_ip=node_ip,
            all_nodes=all_nodes,
            cmd=cmd,
            run_env=run_env,
            screen=screen,
            tmux=tmux,
            stop=stop,
            start=start,
            force_update=force_update,
            wait_for_workers=wait_for_workers,
            min_workers=min_workers,
            wait_timeout=wait_timeout,
            port_forward=port_forward,
            parallel=parallel,
            yes=yes,
            job_waiter_name=job_waiter,
            force=force,
            with_env=with_env)
    except RuntimeError as re:
        fail_command("Failed to exec command.", re)


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help="Run the command in a screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.")
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--force-update",
    is_flag=True,
    default=False,
    help="Force update the cluster even if the cluster is running.")
@click.option(
    "--wait-for-workers",
    is_flag=True,
    default=False,
    help="Whether wait for minimum number of workers to be ready.")
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help="The minimum number of workers to wait for ready.")
@click.option(
    "--wait-timeout",
    required=False,
    type=int,
    help="The timeout seconds to wait for ready.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--job-waiter",
    required=False,
    type=str,
    help="The job waiter to be used to check the completion of the job.")
@click.option(
    "--job-log",
    is_flag=True,
    default=False,
    help="Whether redirect the output of the job to log file in ~/user/logs.")
@click.option(
    "--runtime",
    required=False,
    type=str,
    help="The runtime used to run the job.")
@click.option(
    "--runtime-options",
    required=False,
    type=str,
    default="",
    help="The runtime options of the job.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Do even if the head is not in healthy state.")
@click.option(
    "--with-env",
    is_flag=True,
    default=False,
    help="Run with updater environment variables.")
@click.argument("script", required=True, type=str)
@click.argument("script_args", nargs=-1)
@add_click_logging_options
def submit(
        cluster_config_file, cluster_name, screen, tmux, stop, start,
        force_update, wait_for_workers, min_workers, wait_timeout,
        no_config_cache, port_forward, yes, job_waiter, job_log,
        runtime, runtime_options, force, with_env,
        script, script_args):
    """Uploads and runs a script on the specified cluster.

    The script is automatically synced to the following location:

        os.path.join("~/user/jobs", os.path.basename(script))
    """
    # Don't use config cache so that we will run a full bootstrap needed for start
    if start:
        no_config_cache = True
    config = _load_cluster_config(
        cluster_config_file, cluster_name, no_config_cache=no_config_cache)
    port_forward = [(port, port) for port in list(port_forward)]
    runtime_options = shlex.split(
        runtime_options) if runtime_options is not None else None
    submit_and_exec(
        config,
        call_context=cli_call_context(),
        script=script,
        script_args=script_args,
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=start,
        force_update=force_update,
        wait_for_workers=wait_for_workers,
        min_workers=min_workers,
        wait_timeout=wait_timeout,
        port_forward=port_forward,
        yes=yes,
        job_waiter_name=job_waiter,
        job_log=job_log,
        runtime=runtime,
        runtime_options=runtime_options,
        force=force,
        with_env=with_env,
        )


@cli.command(context_settings={"ignore_unknown_options": True})
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--screen",
    is_flag=True,
    default=False,
    help="Run the command in a screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
@click.option(
    "--stop",
    is_flag=True,
    default=False,
    help="Stop the cluster after the command finishes running.")
@click.option(
    "--start",
    is_flag=True,
    default=False,
    help="Start the cluster if needed.")
@click.option(
    "--force-update",
    is_flag=True,
    default=False,
    help="Force update the cluster even if the cluster is running.")
@click.option(
    "--wait-for-workers",
    is_flag=True,
    default=False,
    help="Whether wait for minimum number of workers to be ready.")
@click.option(
    "--min-workers",
    required=False,
    type=int,
    help="The minimum number of workers to wait for ready.")
@click.option(
    "--wait-timeout",
    required=False,
    type=int,
    help="The timeout seconds to wait for ready.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--job-waiter",
    required=False,
    type=str,
    help="The job waiter to be used to check the completion of the job.")
@click.option(
    "--job-log",
    is_flag=True,
    default=False,
    help="Whether redirect the output of the job to log file in ~/user/logs.")
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Do even if the head is not in healthy state.")
@click.option(
    "--with-env",
    is_flag=True,
    default=False,
    help="Run with updater environment variables.")
@click.argument("script", required=True, type=str)
@click.argument("script_args", nargs=-1)
@add_click_logging_options
def run(
        cluster_config_file, cluster_name, screen, tmux, stop, start,
        force_update, wait_for_workers, min_workers, wait_timeout,
        no_config_cache, port_forward, yes, job_waiter, job_log, force,
        with_env, script, script_args):
    """Runs a built-in script (bash or python or a registered command).

    If you want to execute any commands or user scripts, use exec or submit.
    """
    # Don't use config cache so that we will run a full bootstrap needed for start
    if start:
        no_config_cache = True
    config = _load_cluster_config(
        cluster_config_file, cluster_name, no_config_cache=no_config_cache)
    port_forward = [(port, port) for port in list(port_forward)]
    _run_script(
        config,
        call_context=cli_call_context(),
        script=script,
        script_args=script_args,
        screen=screen,
        tmux=tmux,
        stop=stop,
        start=start,
        force_update=force_update,
        wait_for_workers=wait_for_workers,
        min_workers=min_workers,
        wait_timeout=wait_timeout,
        port_forward=port_forward,
        yes=yes,
        job_waiter_name=job_waiter,
        job_log=job_log,
        force=force,
        with_env=with_env,
    )


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--cpus",
    required=False,
    type=int,
    help="Specify the number of worker cpus of the cluster.")
@click.option(
    "--gpus",
    required=False,
    type=int,
    help="Specify the number of worker gpus of the cluster.")
@click.option(
    "--workers",
    required=False,
    type=int,
    help="Specify the number of workers of the cluster.")
@click.option(
    "--worker-type",
    required=False,
    type=str,
    help="The worker type of the number of workers if there are multiple worker types.")
@click.option(
    "--resources",
    required=False,
    type=str,
    help="The resources to scale for each resource_name:amount separated by comma. "
         "For example, CPU:4,GPU:2")
@click.option(
    "--bundles",
    required=False,
    type=str,
    help="Additional resource bundles to scale in format [{\"resource_name\": amount}, {\"resource_name\": amount}]. "
         "for example, [{\"CPU\": 4, \"GPU\": 1}, {\"CPU\": 8, \"GPU\": 2}]")
@click.option(
    "--up-only",
    is_flag=True,
    default=False,
    help="Scale up if resources is not enough. No scale down.")
@click.option(
    "--override",
    is_flag=True,
    default=False,
    help="Override all the existing resource requests.")
@add_click_logging_options
def scale(
        cluster_config_file, yes, cluster_name,
        cpus, gpus, workers, worker_type,
        resources, bundles, up_only, override):
    """Scale the cluster with a specific number cpus or nodes."""
    if bundles:
        bundles = parse_bundles_json(bundles)
    if resources:
        resources = parse_resources(resources)
    scale_cluster(
        cluster_config_file, yes, cluster_name,
        cpus=cpus, gpus=gpus,
        workers=workers, worker_type=worker_type,
        resources=resources, bundles=bundles,
        up_only=up_only, override=override)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node to upload to")
@click.option(
    "--all-nodes/--no-all-nodes",
    is_flag=True,
    default=False,
    help="Whether to upload to all nodes.")
@add_click_logging_options
def upload(
        cluster_config_file, source, target, cluster_name, node_ip, all_nodes):
    """Upload files to a cluster or a specified node."""

    try:
        config = _load_cluster_config(
            cluster_config_file, cluster_name)
        _rsync(
            config,
            call_context=cli_call_context(),
            source=source,
            target=target,
            down=False,
            node_ip=node_ip,
            all_nodes=all_nodes)
    except RuntimeError as re:
        fail_command("Failed to rsync up.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node from which to download")
@add_click_logging_options
def download(
        cluster_config_file, source, target, cluster_name, node_ip):
    """Download files from a cluster or a specified node."""
    try:
        config = _load_cluster_config(
            cluster_config_file, cluster_name)
        _rsync(
            config,
            call_context=cli_call_context(),
            source=source, target=target,
            down=True, node_ip=node_ip)
    except RuntimeError as re:
        fail_command("Failed to rsync down.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@add_click_logging_options
def status(cluster_config_file, cluster_name):
    """Show cluster summary status."""
    show_cluster_status(
        cluster_config_file,
        cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--worker-cpus",
    is_flag=True,
    default=False,
    help="Get the total number of CPUs for workers.")
@click.option(
    "--worker-gpus",
    is_flag=True,
    default=False,
    help="Get the total number of GPUs for workers.")
@click.option(
    "--worker-memory",
    is_flag=True,
    default=False,
    help="Get the total memory for workers.")
@click.option(
    "--cpus-per-worker",
    is_flag=True,
    default=False,
    help="Get the number of CPUs per worker.")
@click.option(
    "--gpus-per-worker",
    is_flag=True,
    default=False,
    help="Get the number of GPUs per worker.")
@click.option(
    "--memory-per-worker",
    is_flag=True,
    default=False,
    help="Get the size of memory per worker in GB.")
@click.option(
    "--sockets-per-worker",
    is_flag=True,
    default=False,
    help="Get the number of cpu sockets per worker.")
@click.option(
    "--total-workers",
    is_flag=True,
    default=False,
    help="Get the size of updated workers.")
@add_click_logging_options
def info(
        cluster_config_file, cluster_name,
        worker_cpus, worker_gpus, worker_memory,
        cpus_per_worker, gpus_per_worker, memory_per_worker,
        sockets_per_worker, total_workers):
    """Show cluster summary information and useful links to use the cluster."""
    config = _load_cluster_config(cluster_config_file, cluster_name)
    show_info(
        config, cluster_config_file,
        worker_cpus, worker_gpus, worker_memory,
        cpus_per_worker, gpus_per_worker, memory_per_worker,
        sockets_per_worker, total_workers)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--public",
    is_flag=True,
    default=False,
    help="Return public ip if there is one")
@add_click_logging_options
def head_ip(cluster_config_file, cluster_name, public):
    """Return the head node IP of a cluster."""
    try:
        ip = get_head_node_ip(
            cluster_config_file, cluster_name, public)
        click.echo(ip)
    except RuntimeError as re:
        fail_command("Failed to get head IP.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@add_click_logging_options
def head_host(cluster_config_file, cluster_name):
    """Return the head node host of a cluster."""
    try:
        host = get_head_node_host(
            cluster_config_file, cluster_name)
        click.echo(host)
    except RuntimeError as re:
        fail_command("Failed to get head host.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--runtime",
    required=False,
    type=str,
    default=None,
    help="Get the worker ips for specific runtime.")
@click.option(
    "--node-status",
    required=False,
    type=str,
    default=None,
    help="The node status of the workers. Values: setting-up, up-to-date, update-failed."
    " If not specified, return all the workers.")
@click.option(
    "--separator",
    required=False,
    type=str,
    default=None,
    help="The separator between worker ips. Default is change a line.")
@add_click_logging_options
def worker_ips(
        cluster_config_file, cluster_name,
        runtime, node_status, separator):
    """Return the list of worker IPs of a cluster."""
    try:
        ips = get_worker_node_ips(
            cluster_config_file, cluster_name,
            runtime=runtime, node_status=node_status)
        if len(ips) > 0:
            if separator:
                click.echo(separator.join(ips))
            else:
                click.echo("\n".join(ips))
    except RuntimeError as re:
        fail_command("Failed to get worker IPs.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--runtime",
    required=False,
    type=str,
    default=None,
    help="Get the worker hosts for specific runtime.")
@click.option(
    "--node-status",
    required=False,
    type=str,
    default=None,
    help="The node status of the workers. Values: setting-up, up-to-date, update-failed."
    " If not specified, return all the workers.")
@click.option(
    "--separator",
    required=False,
    type=str,
    default=None,
    help="The separator between worker hosts. Default is change a line.")
@add_click_logging_options
def worker_hosts(
        cluster_config_file, cluster_name,
        runtime, node_status, separator):
    """Return the list of worker hosts of a cluster."""
    try:
        hosts = get_worker_node_hosts(
            cluster_config_file, cluster_name,
            runtime=runtime, node_status=node_status)
        if len(hosts) > 0:
            if separator:
                click.echo(separator.join(hosts))
            else:
                click.echo("\n".join(hosts))
    except RuntimeError as re:
        fail_command("Failed to get worker hosts.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--lines",
    required=False,
    default=100,
    type=int,
    help="Number of lines to tail.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--file-type",
    required=False,
    type=str,
    default=None,
    help="The type of information to check: log, out, err")
@add_click_logging_options
def monitor(
        cluster_config_file, lines, cluster_name, file_type):
    """Tails the monitor logs of a cluster."""
    try:
        monitor_cluster(
            cluster_config_file, lines, cluster_name, file_type=file_type)
    except RuntimeError as re:
        fail_command("Failed to monitor cluster.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The list of runtimes to print logs for. If not specified, will all.")
@click.option(
    "--node-types",
    required=False,
    type=str,
    default=None,
    help="The list of node types to print logs for.")
@click.option(
    "--node-ips",
    required=False,
    type=str,
    default=None,
    help="The list of node ips to print logs for.")
@add_click_logging_options
def logs(
        cluster_config_file, cluster_name, no_config_cache,
        runtimes, node_types, node_ips):
    """Print logs of runtimes."""
    try:
        cluster_logs(
            cluster_config_file, cluster_name,
            no_config_cache, runtimes,
            node_types, node_ips)
    except RuntimeError as re:
        fail_command("Failed to print cluster logs.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--bind-address",
    required=False,
    type=str,
    default=None,
    help="The address to bind on local node.")
@add_click_logging_options
def start_proxy(
        cluster_config_file, no_config_cache, cluster_name,
        bind_address):
    """Start the SOCKS5 proxy to the cluster through SSH tunnel forwarding to the head."""
    start_ssh_proxy(
        cluster_config_file,
        override_cluster_name=cluster_name,
        no_config_cache=no_config_cache,
        bind_address=bind_address)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@add_click_logging_options
def stop_proxy(cluster_config_file, cluster_name):
    """Stop the SOCKS5 proxy to the cluster."""
    stop_ssh_proxy(cluster_config_file, cluster_name)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--hard",
    is_flag=True,
    default=False,
    help="Terminates node by directly delete the instances")
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node to kill")
@add_click_logging_options
def kill_node(
        cluster_config_file, yes, hard, cluster_name, node_ip):
    """Kills a specified node or a random node."""
    kill_node_from_head(
        cluster_config_file, yes, hard, cluster_name,
        node_ip)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--min-workers",
    required=False,
    default=None,
    type=int,
    help="The min workers to wait for being ready.")
@click.option(
    "--timeout",
    required=False,
    default=None,
    type=int,
    help="The maximum number of seconds to wait.")
@add_click_logging_options
def wait_for_ready(
        cluster_config_file, cluster_name, no_config_cache,
        min_workers, timeout):
    """Wait for the minimum number of workers to be ready."""
    config = _load_cluster_config(
        cluster_config_file, cluster_name,
        no_config_cache=no_config_cache)
    call_context = cli_call_context()
    _wait_for_ready(config, call_context, min_workers, timeout)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The list of runtimes to show process status for. If not specified, will all.")
@add_click_logging_options
def process_status(
        cluster_config_file, cluster_name, no_config_cache, runtimes):
    """Show process status of cluster nodes."""
    try:
        cluster_process_status(
            cluster_config_file, cluster_name,
            no_config_cache, runtimes)
    except RuntimeError as re:
        fail_command("Failed to show cluster process status.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@add_click_logging_options
def resource_metrics(
        cluster_config_file, cluster_name, no_config_cache):
    """Show cluster resource metrics and the metrics for each node."""
    try:
        cluster_resource_metrics(
            cluster_config_file, cluster_name,
            no_config_cache)
    except RuntimeError as re:
        fail_command("Failed to show cluster resource metrics.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@add_click_logging_options
def debug_status(
        cluster_config_file, cluster_name, no_config_cache):
    """Show debug status of cluster scaling."""
    try:
        cluster_debug_status(
            cluster_config_file, cluster_name,
            no_config_cache,)
    except RuntimeError as re:
        fail_command("Failed to show cluster debug status.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--with-details",
    is_flag=True,
    default=False,
    help="Whether to show detailed information.")
@add_click_logging_options
def health_check(
        cluster_config_file, cluster_name, no_config_cache,
        with_details):
    """Do cluster health check."""
    try:
        cluster_health_check(
            cluster_config_file, cluster_name,
            no_config_cache, with_details)
    except RuntimeError as re:
        fail_command("Failed to do cluster health check.", re)


@cli.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--hosts",
    "-h",
    required=False,
    type=str,
    help="Single or list of hosts, separated by comma.")
@click.option(
    "--head-only",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Decide whether dump the head node only.")
@click.option(
    "--output",
    "-o",
    required=False,
    type=str,
    default=None,
    help="Output file.")
@click.option(
    "--logs/--no-logs",
    is_flag=True,
    default=True,
    help="Collect logs from session dir")
@click.option(
    "--debug-state/--no-debug-state",
    is_flag=True,
    default=True,
    help="Collect debug_state.txt from log dir")
@click.option(
    "--pip/--no-pip",
    is_flag=True,
    default=True,
    help="Collect installed pip packages")
@click.option(
    "--processes/--no-processes",
    is_flag=True,
    default=True,
    help="Collect info on running processes")
@click.option(
    "--processes-verbose/--no-processes-verbose",
    is_flag=True,
    default=True,
    help="Increase process information verbosity")
@click.option(
    "--tempfile",
    "-T",
    required=False,
    type=str,
    default=None,
    help="Temporary file to use")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local cluster config cache.")
@click.option(
    "--silent",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Whether print a warning message for cluster dump.")
@add_click_logging_options
def cluster_dump(
        cluster_config_file: Optional[str] = None,
        cluster_name: str = None,
        hosts: Optional[str] = None,
        head_only: Optional[bool] = None,
        output: Optional[str] = None,
        logs: bool = True,
        debug_state: bool = True,
        pip: bool = True,
        processes: bool = True,
        processes_verbose: bool = False,
        tempfile: Optional[str] = None,
        no_config_cache=False,
        silent=False):
    """Get log data from one or more nodes.

    Best used with cluster configs:

        cloudtik cluster-dump [cluster.yaml]

    Include the --head-only flag to collect data from head node only.

    Missing fields will be tried to be auto-filled.

    You can also manually specify a list of hosts using the
    ``--hosts <host1,host2,...>`` parameter.
    """
    config = _load_cluster_config(
        cluster_config_file, cluster_name,
        no_config_cache=no_config_cache)
    dump_cluster(
        config=config,
        call_context=cli_call_context(),
        hosts=hosts,
        head_only=head_only,
        output=output,
        logs=logs,
        debug_state=debug_state,
        pip=pip,
        processes=processes,
        processes_verbose=processes_verbose,
        tempfile=tempfile,
        silent=silent)


def _add_command_alias(command, name, hidden):
    add_command_alias(cli, command, name, hidden)


# commands running on working node for handling a cluster
cli.add_command(start)
_add_command_alias(start, name="up", hidden=True)
cli.add_command(stop)
_add_command_alias(stop, name="down", hidden=True)

cli.add_command(attach)
cli.add_command(exec)
cli.add_command(submit)
cli.add_command(run)
cli.add_command(scale)

cli.add_command(upload)
_add_command_alias(upload, name="rsync-up", hidden=True)
_add_command_alias(upload, name="rsync_up", hidden=True)
cli.add_command(download)
_add_command_alias(download, name="rsync-down", hidden=True)
_add_command_alias(download, name="rsync_down", hidden=True)

# commands running on working node for information and status
cli.add_command(status)
cli.add_command(info)
cli.add_command(head_ip)
_add_command_alias(head_ip, name="head_ip", hidden=True)
cli.add_command(head_host)
cli.add_command(worker_ips)
_add_command_alias(worker_ips, name="worker_ips", hidden=True)
cli.add_command(worker_hosts)


cli.add_command(monitor)
cli.add_command(logs)

# commands for advanced management
cli.add_command(start_proxy)
cli.add_command(stop_proxy)

cli.add_command(kill_node)
_add_command_alias(kill_node, name="kill_node", hidden=True)
cli.add_command(wait_for_ready)

# commands running on working node for debug
cli.add_command(process_status)
cli.add_command(resource_metrics)
cli.add_command(debug_status)
cli.add_command(health_check)

cli.add_command(cluster_dump)
_add_command_alias(cluster_dump, name="cluster_dump", hidden=True)

# workspace commands
cli.add_command(workspace)

# storage commands
cli.add_command(storage)

# database commands
cli.add_command(database)

# runtime commands
cli.add_command(runtime)

# head commands
cli.add_command(head)

# node commands (not facing, running on node)
cli.add_command(node)

# dynamic command of runtime
_register_runtime_commands()


def main():
    return cli()


if __name__ == "__main__":
    main()

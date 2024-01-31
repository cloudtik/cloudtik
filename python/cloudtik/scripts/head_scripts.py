import importlib
import logging
import os
import pkgutil
from typing import Optional

import click

from cloudtik.core._private.cli_logger import (add_click_logging_options,
                                               cli_logger)
from cloudtik.core._private.cluster.cluster_operator import (
    debug_status_string, dump_cluster_on_head,
    RUN_ENV_TYPES, teardown_cluster_on_head, cluster_process_status_on_head, rsync_node_on_head, attach_node_on_head,
    start_node_on_head, stop_node_on_head, kill_node_on_head, scale_cluster_on_head,
    _wait_for_ready, _show_cluster_status, _monitor_cluster, cli_call_context, _exec_node_on_head,
    do_health_check, cluster_resource_metrics_on_head, show_info, _run_script_on_head, cluster_logs_on_head)
from cloudtik.core._private.constants import CLOUDTIK_REDIS_DEFAULT_PASSWORD
from cloudtik.core._private.service_discovery.naming import _get_worker_node_hosts, get_cluster_head_host
from cloudtik.core._private.util.redis_utils import get_address_to_use_or_die
from cloudtik.core._private.state import kv_store
from cloudtik.core._private.state.kv_store import kv_initialize_with_address
from cloudtik.core._private.utils import CLOUDTIK_CLUSTER_SCALING_ERROR, \
    CLOUDTIK_CLUSTER_SCALING_STATUS, get_head_bootstrap_config, \
    load_head_cluster_config, parse_bundles_json, parse_resources, prepare_runtime_config_on_head, \
    _get_worker_node_ips, get_cluster_head_ip
from cloudtik.scripts.utils import NaturalOrderGroup, add_command_alias

logger = logging.getLogger(__name__)


def _register_head_runtime_commands():
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
        command_group_name = module_name + "_on_head"
        if command_group_name in _module.__dict__:
            runtime_command_group = _module.__dict__[command_group_name]
            head.add_command(runtime_command_group)


@click.group(cls=NaturalOrderGroup)
def head():
    """
    Commands running on head node only.
    """
    pass


@head.command()
@click.option(
    "--node-ip",
    "-n",
    required=True,
    type=str,
    help="The node ip to attach to.")
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
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
        node_ip, screen, tmux, new, port_forward, host,
        with_env):
    """Attach to worker node from head."""
    port_forward = [(port, port) for port in list(port_forward)]
    attach_node_on_head(
        node_ip,
        screen,
        tmux,
        new,
        port_forward,
        force_to_host=host,
        with_env=with_env)


@head.command()
@click.argument("cmd", required=True, type=str)
@click.option(
    "--node-ip",
    "-n",
    required=False,
    type=str,
    default=None,
    help="The node ip to operate on.")
@click.option(
    "--all-nodes/--no-all-nodes",
    is_flag=True,
    default=False,
    help="Whether to execute on all nodes.")
@click.option(
    "--run-env",
    required=False,
    type=click.Choice(RUN_ENV_TYPES),
    default="auto",
    help="Choose whether to execute this command in a container or directly on"
    " the cluster head. Only applies when docker is configured in the YAML.")
@click.option(
    "--screen", is_flag=True, default=False, help="Run the command in screen.")
@click.option(
    "--tmux", is_flag=True, default=False, help="Run the command in tmux.")
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
    "--port-forward",
    "-p",
    required=False,
    multiple=True,
    type=int,
    help="Port to forward. Use this multiple times to forward multiple ports.")
@click.option(
    "--with-output",
    is_flag=True,
    default=False,
    help="Whether to capture command output.")
@click.option(
    "--parallel/--no-parallel",
    is_flag=True,
    default=True,
    help="Whether the run the commands on nodes in parallel.")
@click.option(
    "--job-waiter",
    required=False,
    type=str,
    help="The job waiter to be used to check the completion of the job.")
@click.option(
    "--with-env",
    is_flag=True,
    default=False,
    help="Run with updater environment variables.")
@add_click_logging_options
def exec(
        cmd, node_ip, all_nodes, run_env, screen, tmux,
        wait_for_workers, min_workers, wait_timeout,
        port_forward, with_output, parallel, job_waiter,
        with_env):
    """Execute command on the worker node from head."""
    port_forward = [(port, port) for port in list(port_forward)]
    config = load_head_cluster_config()
    call_context = cli_call_context()

    _exec_node_on_head(
        config=config,
        call_context=call_context,
        node_ip=node_ip,
        all_nodes=all_nodes,
        cmd=cmd,
        run_env=run_env,
        screen=screen,
        tmux=tmux,
        wait_for_workers=wait_for_workers,
        min_workers=min_workers,
        wait_timeout=wait_timeout,
        port_forward=port_forward,
        with_output=with_output,
        parallel=parallel,
        job_waiter_name=job_waiter,
        with_env=with_env)


@head.command(context_settings={"ignore_unknown_options": True})
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
    "--with-output",
    is_flag=True,
    default=False,
    help="Whether to capture command output.")
@click.argument("script", required=True, type=str)
@click.argument("script_args", nargs=-1)
@add_click_logging_options
def run(
        wait_for_workers, min_workers, wait_timeout,
        with_output, script, script_args):
    """Runs a built-in script (bash or python or a registered command).

    If you want to execute any commands or user scripts, use exec or submit.
    """
    config = load_head_cluster_config()
    call_context = cli_call_context()

    _run_script_on_head(
        config=config,
        call_context=call_context,
        script=script,
        script_args=script_args,
        wait_for_workers=wait_for_workers,
        min_workers=min_workers,
        wait_timeout=wait_timeout,
        with_output=with_output)


@head.command()
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
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
        yes, cpus, gpus, workers, worker_type,
        resources, bundles, up_only, override):
    """Scale the cluster with a specific number cpus or nodes."""
    if bundles:
        bundles = parse_bundles_json(bundles)
    if resources:
        resources = parse_resources(resources)

    scale_cluster_on_head(
        yes,
        cpus=cpus, gpus=gpus,
        workers=workers, worker_type=worker_type,
        resources=resources,
        bundles=bundles,
        up_only=up_only,
        override=override)


@head.command()
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--node-ip",
    "-n",
    required=False,
    type=str,
    default=None,
    help="The worker node ip to upload.")
@click.option(
    "--all-workers/--no-all-workers",
    is_flag=True,
    default=False,
    help="Whether to upload to all workers.")
@add_click_logging_options
def upload(
        source, target, node_ip, all_workers):
    """Upload files to a specified worker node or all nodes."""
    config = load_head_cluster_config()
    call_context = cli_call_context()
    rsync_node_on_head(
        config,
        call_context,
        source,
        target,
        down=False,
        node_ip=node_ip,
        all_workers=all_workers)


@head.command()
@click.argument("source", required=False, type=str)
@click.argument("target", required=False, type=str)
@click.option(
    "--node-ip",
    "-n",
    required=True,
    type=str,
    help="The worker node ip from which to download.")
@add_click_logging_options
def download(source, target, node_ip):
    """Download files from worker node."""
    config = load_head_cluster_config()
    call_context = cli_call_context()
    rsync_node_on_head(
        config,
        call_context,
        source,
        target,
        down=True,
        node_ip=node_ip,
        all_workers=False)


@head.command()
@add_click_logging_options
def status():
    """Show cluster summary status."""
    config = load_head_cluster_config()
    _show_cluster_status(config)


@head.command()
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
        worker_cpus, worker_gpus, worker_memory,
        cpus_per_worker, gpus_per_worker, memory_per_worker,
        sockets_per_worker, total_workers):
    """Show cluster summary information and useful links to use the cluster."""
    cluster_config_file = get_head_bootstrap_config()
    config = load_head_cluster_config()
    show_info(
        config, cluster_config_file,
        worker_cpus, worker_gpus, worker_memory,
        cpus_per_worker, gpus_per_worker, memory_per_worker,
        sockets_per_worker, total_workers)


@head.command()
@click.option(
    "--public",
    is_flag=True,
    default=False,
    help="Return public ip if there is one")
@add_click_logging_options
def head_ip(public):
    """Return the head node IP of a cluster."""
    try:
        config = load_head_cluster_config()
        head_node_ip = get_cluster_head_ip(config, public)
        click.echo(head_node_ip)
    except RuntimeError as re:
        cli_logger.error("Get head IP failed. " + str(re))
        raise re


@head.command()
@add_click_logging_options
def head_host():
    """Return the head node host of a cluster."""
    try:
        config = load_head_cluster_config()
        host = get_cluster_head_host(config)
        click.echo(host)
    except RuntimeError as re:
        cli_logger.error("Get head host failed. " + str(re))
        raise re


@head.command()
@add_click_logging_options
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
def worker_ips(runtime, node_status, separator):
    """Return the list of worker IPs of a cluster."""
    config = load_head_cluster_config()
    ips = _get_worker_node_ips(
        config, runtime=runtime, node_status=node_status)
    if len(ips) > 0:
        if separator:
            click.echo(separator.join(ips))
        else:
            click.echo("\n".join(ips))


@head.command()
@add_click_logging_options
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
def worker_hosts(runtime, node_status, separator):
    """Return the list of worker hosts of a cluster."""
    config = load_head_cluster_config()
    hosts = _get_worker_node_hosts(
        config, runtime=runtime, node_status=node_status)
    if len(hosts) > 0:
        if separator:
            click.echo(separator.join(hosts))
        else:
            click.echo("\n".join(hosts))


@head.command()
@click.option(
    "--lines",
    required=False,
    default=100,
    type=int,
    help="Number of lines to tail.")
@click.option(
    "--file-type",
    required=False,
    type=str,
    default=None,
    help="The type of information to check: log, out, err")
@add_click_logging_options
def monitor(lines, file_type):
    """Tails the monitor logs of a cluster."""
    config = load_head_cluster_config()
    _monitor_cluster(config, lines, file_type=file_type)


@head.command()
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
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
    "--indent-level",
    required=False,
    default=None,
    type=int,
    hidden=True,
    help="The indent level for showing messages during this command.")
@add_click_logging_options
def teardown(
        yes, keep_min_workers, hard, indent_level):
    """Tear down a cluster."""
    if indent_level is not None:
        with cli_logger.indented_by(indent_level):
            teardown_cluster_on_head(
                yes, keep_min_workers, hard=hard)
    else:
        teardown_cluster_on_head(
            yes, keep_min_workers, hard=hard)


@head.command()
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
    "--node-ip",
    required=False,
    type=str,
    default=None,
    help="The node ip address of the node to kill")
@click.option(
    "--indent-level",
    required=False,
    default=None,
    type=int,
    hidden=True,
    help="The indent level for showing messages during this command.")
@add_click_logging_options
def kill_node(
        yes, hard, node_ip, indent_level):
    """Kills a specified worker node and a random worker node."""

    def do_kill_node():
        killed_node_ip = kill_node_on_head(
            yes, hard, node_ip)
        if killed_node_ip:
            click.echo("Killed node with IP " + killed_node_ip)

    if indent_level is not None:
        with cli_logger.indented_by(indent_level):
            do_kill_node()
    else:
        do_kill_node()


@head.command()
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
def wait_for_ready(min_workers, timeout):
    """Wait for the minimum number of workers to be ready."""
    config = load_head_cluster_config()
    call_context = cli_call_context()
    _wait_for_ready(
        config, call_context, min_workers, timeout)


@head.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="Connect with redis password.")
@add_click_logging_options
def debug_status(address, redis_password):
    """Print cluster status, including autoscaling info."""
    if not address:
        address = get_address_to_use_or_die(head=True)
    kv_initialize_with_address(address, redis_password)
    status = kv_store.kv_get(
        CLOUDTIK_CLUSTER_SCALING_STATUS)
    error = kv_store.kv_get(
        CLOUDTIK_CLUSTER_SCALING_ERROR)
    print(debug_status_string(status, error))


@head.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="Connect with redis password.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The list of runtimes to show process status for. If not specified, will all.")
@add_click_logging_options
def process_status(address, redis_password, runtimes):
    """Show cluster process status."""
    if not address:
        address = get_address_to_use_or_die(head=True)
    cluster_process_status_on_head(
        address, redis_password, runtimes)


@head.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="Connect with redis password.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The list of runtimes to print logs for. If not specified, will print all.")
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
        address, redis_password,
        runtimes, node_types, node_ips):
    """Print cluster logs."""
    if not address:
        address = get_address_to_use_or_die(head=True)
    cluster_logs_on_head(
        address, redis_password,
        runtimes, node_types, node_ips)


@head.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="Connect with redis password.")
@add_click_logging_options
def resource_metrics(address, redis_password):
    """Show cluster resource metrics."""
    if not address:
        address = get_address_to_use_or_die(head=True)
    cluster_resource_metrics_on_head(
        address, redis_password)


@head.command()
@click.option(
    "--address",
    required=False,
    type=str,
    help="Override the address to connect to.")
@click.option(
    "--redis-password",
    required=False,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="Connect with redis password.")
@click.option(
    "--component",
    required=False,
    type=str,
    help="Health check for a specific component. Currently supports: "
    "[None]")
@click.option(
    "--with-details",
    is_flag=True,
    default=False,
    help="Whether to show detailed information.")
@add_click_logging_options
def health_check(
        address, redis_password, component, with_details):
    """
    Health check a cluster or a specific component. Exit code 0 is healthy.
    """
    if not address:
        address = get_address_to_use_or_die(head=True)
    do_health_check(
        address, redis_password, component, with_details)


@head.command()
@click.option(
    "--hosts",
    "-h",
    required=False,
    type=str,
    help="Single or list of hosts, separated by comma.")
@click.option(
    "--stream",
    "-S",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="If True, will stream the binary archive contents to stdout")
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
    "--silent",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Whether print a warning message for cluster dump.")
@add_click_logging_options
def cluster_dump(
        hosts: Optional[str] = None,
        stream: bool = False,
        output: Optional[str] = None,
        logs: bool = True,
        debug_state: bool = True,
        pip: bool = True,
        processes: bool = True,
        processes_verbose: bool = False,
        tempfile: Optional[str] = None,
        silent: bool = False,):
    """Collect cluster data and package into an archive on head.

        Usage:

            cloudtik head cluster-dump[--stream/--output file]

        This script is called on head node to fetch the cluster data.
        """

    config = load_head_cluster_config()
    call_context = cli_call_context()
    dump_cluster_on_head(
        config, call_context,
        hosts=hosts,
        stream=stream,
        output=output,
        logs=logs,
        debug_state=debug_state,
        pip=pip,
        processes=processes,
        processes_verbose=processes_verbose,
        temp_file=tempfile,
        silent=silent)


@head.command(hidden=True)
@add_click_logging_options
def prepare():
    """
    Run a configuration preparation on head.
    """
    # Note that this is used for head node only
    config = load_head_cluster_config()
    prepare_runtime_config_on_head(config)


@click.group(cls=NaturalOrderGroup)
def runtime():
    """
    Commands running on head for runtime control.
    """
    pass


@runtime.command()
@click.option(
    "--node-ip",
    "-n",
    required=False,
    type=str,
    default=None,
    help="The node ip on which to execute start commands.")
@click.option(
    "--all-nodes/--no-all-nodes",
    is_flag=True,
    default=True,
    help="Whether to execute start commands to all nodes.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The runtimes to start. Comma separated list.")
@click.option(
    "--indent-level",
    required=False,
    default=None,
    type=int,
    hidden=True,
    help="The indent level for showing messages during this command.")
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
@add_click_logging_options
def start(
        node_ip, all_nodes, runtimes, indent_level, parallel, yes):
    """Run start commands on the specific node or all nodes."""
    def do_start_node():
        start_node_on_head(
            node_ip=node_ip, all_nodes=all_nodes,
            runtimes=runtimes, parallel=parallel, yes=yes)

    if indent_level is not None:
        with cli_logger.indented_by(indent_level):
            do_start_node()
    else:
        do_start_node()


@runtime.command()
@click.option(
    "--node-ip",
    "-n",
    required=False,
    type=str,
    default=None,
    help="The node ip on which to execute start commands.")
@click.option(
    "--all-nodes/--no-all-nodes",
    is_flag=True,
    default=True,
    help="Whether to execute stop commands to all nodes.")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The runtimes to start. Comma separated list.")
@click.option(
    "--indent-level",
    required=False,
    default=None,
    type=int,
    hidden=True,
    help="The indent level for showing messages during this command.")
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
@add_click_logging_options
def stop(
        node_ip, all_nodes, runtimes, indent_level, parallel, yes):
    """Run stop commands on the specific node or all nodes."""
    def do_stop_node():
        stop_node_on_head(
            node_ip=node_ip, all_nodes=all_nodes,
            runtimes=runtimes, parallel=parallel, yes=yes)

    if indent_level is not None:
        with cli_logger.indented_by(indent_level):
            do_stop_node()
    else:
        do_stop_node()


def _add_runtime_command_alias(command, name, hidden):
    add_command_alias(runtime, command, name, hidden)


runtime.add_command(start)
runtime.add_command(stop)

# commands running on head node
head.add_command(attach)
head.add_command(exec)
head.add_command(run)
head.add_command(scale)

head.add_command(upload)
head.add_command(download)
_add_runtime_command_alias(upload, name="rsync-up", hidden=True)
_add_runtime_command_alias(download, name="rsync-down", hidden=True)

head.add_command(status)
head.add_command(info)
head.add_command(head_ip)
head.add_command(head_host)
head.add_command(worker_ips)
head.add_command(worker_hosts)
head.add_command(monitor)

head.add_command(teardown)
head.add_command(kill_node)
head.add_command(wait_for_ready)

# runtime commands
head.add_command(runtime)

head.add_command(debug_status)
head.add_command(process_status)
head.add_command(resource_metrics)
head.add_command(health_check)
head.add_command(cluster_dump)
head.add_command(prepare)

# dynamic command of runtime
_register_head_runtime_commands()

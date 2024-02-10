import logging
import os
import subprocess
import sys
from socket import socket
from typing import Optional

import click
import psutil

from cloudtik.core._private import services
from cloudtik.core._private.cli_logger import (
    add_click_logging_options,
    cli_logger, cf)
from cloudtik.core._private.cluster.cluster_operator import (
    dump_local)
from cloudtik.core._private.constants import CLOUDTIK_PROCESSES, \
    CLOUDTIK_REDIS_DEFAULT_PASSWORD, \
    CLOUDTIK_DEFAULT_PORT, CLOUDTIK_RUNTIME_ENV_RUNTIMES, CLOUDTIK_RUNTIME_ENV_NODE_TYPE, \
    CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID, CLOUDTIK_RUNTIME_ENV_NODE_IP, CLOUDTIK_RUNTIME_ENV_HEAD_HOST, \
    CLOUDTIK_PROCESS_REDIS, CLOUDTIK_PROCESS_NODE_MONITOR, CLOUDTIK_PROCESS_CONTROLLER, CLOUDTIK_PROCESS_LOG_MONITOR, \
    CLOUDTIK_BOOTSTRAP_CONFIG_FILE
from cloudtik.core._private.util.core_utils import get_cloudtik_home_dir, wait_for_port as _wait_for_port, \
    get_node_ip_address, address_to_ip, address_string
from cloudtik.core._private.node.node_services import NodeServicesStarter
from cloudtik.core._private.node.parameter import StartParams
from cloudtik.core._private.util.redis_utils import find_redis_address, validate_redis_address, create_redis_client, \
    wait_for_redis_to_start
from cloudtik.core._private.resource_spec import ResourceSpec
from cloudtik.core._private.util.runtime_utils import get_runtime_value
from cloudtik.core._private.util.service.service_daemon import service_daemon
from cloudtik.core._private.utils import parse_resources_json, run_script
from cloudtik.runtime.common.service_discovery.cluster_nodes import get_cluster_live_nodes_address
from cloudtik.runtime.common.service_discovery.discovery import get_service_node_addresses
from cloudtik.scripts.utils import NaturalOrderGroup, fail_command

logger = logging.getLogger(__name__)


@click.group(cls=NaturalOrderGroup)
def node():
    """
    Commands running on node local only.
    """
    pass


@node.command()
@click.option(
    "--node-ip-address",
    required=False,
    type=str,
    help="the IP address of this node")
@click.option(
    "--address", required=False, type=str, help="the address to use for this node")
@click.option(
    "--port",
    type=int,
    required=False,
    help=f"the port of the head redis process. If not provided, defaults to "
    f"{CLOUDTIK_DEFAULT_PORT}; if port is set to 0, we will"
    f" allocate an available port.")
@click.option(
    "--head",
    is_flag=True,
    default=False,
    help="provide this argument for the head node")
@click.option(
    "--redis-password",
    required=False,
    hidden=True,
    type=str,
    default=CLOUDTIK_REDIS_DEFAULT_PASSWORD,
    help="If provided, secure Redis ports with this password")
@click.option(
    "--redis-shard-ports",
    required=False,
    hidden=True,
    type=str,
    help="the port to use for the Redis shards other than the "
    "primary Redis shard")
@click.option(
    "--redis-max-memory",
    required=False,
    hidden=True,
    type=int,
    help="The max amount of memory (in bytes) to allow redis to use. Once the "
    "limit is exceeded, redis will start LRU eviction of entries. This only "
    "applies to the sharded redis tables (task, object, and profile tables). "
    "By default this is capped at 10GB but can be set higher.")
@click.option(
    "--memory",
    required=False,
    hidden=True,
    type=int,
    help="The amount of memory (in bytes) to make available to workers. "
    "By default, this is set to the available memory on the node.")
@click.option(
    "--num-cpus",
    required=False,
    type=int,
    help="the number of CPUs on this node")
@click.option(
    "--num-gpus",
    required=False,
    type=int,
    help="the number of GPUs on this node")
@click.option(
    "--resources",
    required=False,
    default="{}",
    type=str,
    help="a JSON serialized dictionary mapping resource name to "
    "resource quantity")
@click.option(
    "--cluster-config",
    required=False,
    type=str,
    help="the file that contains the cluster config")
@click.option(
    "--home-dir",
    hidden=True,
    default=None,
    help="manually specify the root session dir of the CloudTik")
@click.option(
    "--no-redirect-output",
    is_flag=True,
    default=False,
    help="do not redirect non-worker stdout and stderr to files")
@click.option(
    "--runtimes",
    required=False,
    type=str,
    default=None,
    hidden=True,
    help="Runtimes enabled for process monitoring purposes")
@click.option(
    "--node-type",
    required=False,
    type=str,
    default=None,
    hidden=True,
    help="The node type for this node.")
@click.option(
    "--node-seq-id",
    required=False,
    type=str,
    default=None,
    hidden=True,
    help="The node seq id for this node.")
@click.option(
    "--state",
    is_flag=True,
    hidden=True,
    default=False,
    help="If True, the redis state services will be started on head "
         "for this command",
)
@click.option(
    "--controller",
    is_flag=True,
    hidden=True,
    default=False,
    help="If True, the cluster controller will be started on head "
         "for this command",
)
@add_click_logging_options
def start(
        node_ip_address, address, port, head,
        redis_password, redis_shard_ports, redis_max_memory,
        memory, num_cpus, num_gpus, resources,
        cluster_config, home_dir, no_redirect_output,
        runtimes, node_type, node_seq_id,
        state, controller):
    """Start the main daemon processes on the local machine."""
    # Convert hostnames to numerical IP address.
    if not node_ip_address:
        node_ip_address = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_IP)
    if node_ip_address is not None:
        node_ip_address = address_to_ip(node_ip_address)
    redirect_output = None if not no_redirect_output else True

    resources = parse_resources_json(resources)

    # Try get from runtime environment variables if not given in arguments
    if not runtimes:
        runtimes = get_runtime_value(CLOUDTIK_RUNTIME_ENV_RUNTIMES)
    if not node_type:
        node_type = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_TYPE)
    if not node_seq_id:
        node_seq_id = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_SEQ_ID)

    start_params = StartParams(
        node_ip_address=node_ip_address,
        memory=memory,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        resources=resources,
        home_dir=home_dir,
        redirect_output=redirect_output,
        runtimes=runtimes,
        node_type=node_type,
        node_seq_id=node_seq_id,
        state=state,
        controller=controller,
    )
    if head:
        # Use default if port is none, allocate an available port if port is 0
        if port is None:
            port = CLOUDTIK_DEFAULT_PORT

        if port == 0:
            with socket() as s:
                s.bind(("", 0))
                port = s.getsockname()[1]

        num_redis_shards = None
        # Start on the head node.
        if redis_shard_ports is not None and address is None:
            redis_shard_ports = redis_shard_ports.split(",")
            # Infer the number of Redis shards from the ports if the number is
            # not provided.
            num_redis_shards = len(redis_shard_ports)

        if controller and not cluster_config:
            # set to the default if not specified explicitly
            cluster_config = CLOUDTIK_BOOTSTRAP_CONFIG_FILE

        # Get the node IP address if one is not provided.
        start_params.update_if_absent(
            node_ip_address=get_node_ip_address())
        start_params.update_if_absent(
            redis_port=port,
            redis_shard_ports=redis_shard_ports,
            redis_max_memory=redis_max_memory,
            num_redis_shards=num_redis_shards,
            redis_max_clients=None,
            cluster_config=cluster_config,
        )

        # Fail early when starting a new cluster when one is already running
        if address is None and state:
            # TODO: since we start redis and clustering services separately
            #  check clustering services exists for duplicated start
            default_address = f"{start_params.node_ip_address}:{port}"
            redis_addresses = find_redis_address(default_address)
            if len(redis_addresses) > 0:
                raise ConnectionError(
                    f"CloudTik is already running at {default_address}. "
                    f"Please specify a different port using the `--port`"
                    f" command to `cloudtik node start`.")

        node_starter = NodeServicesStarter(
            start_params, head=True, shutdown_at_exit=False, spawn_reaper=False)

        redis_address = node_starter.redis_address
        if home_dir is None:
            # Default home directory.
            home_dir = get_cloudtik_home_dir()
        # Using the user-supplied home dir unblocks
        # users who can't write to the default home.
        os.makedirs(home_dir, exist_ok=True)
        current_cluster_path = os.path.join(home_dir, "cloudtik_current_cluster")
        # TODO: Consider using the custom home_dir for this file across the
        # code base.
        with open(current_cluster_path, "w") as f:
            print(redis_address, file=f)
    else:
        # Start on a non-head node.
        if not address:
            head_host = get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_HOST)
            if not head_host:
                cli_logger.abort(
                    "`{}` is required for starting worker node",
                    cf.bold("--address"))
                raise Exception(
                    "Invalid command. --address must be provided for worker node.")
            address = address_string(head_host, CLOUDTIK_DEFAULT_PORT)

        (redis_address,
         redis_ip, redis_port) = validate_redis_address(address)

        # Wait for the Redis server to be started. And throw an exception if we
        # can't connect to it.
        wait_for_redis_to_start(
            redis_ip, redis_port, password=redis_password)

        # Create a Redis client.
        redis_client = create_redis_client(
            redis_address, password=redis_password)

        # Check that the version information on this node matches the version
        # information that the cluster was started with.
        services.check_version_info(redis_client)

        # Get the node IP address if one is not provided.
        start_params.update_if_absent(
            node_ip_address=get_node_ip_address(redis_address))

        start_params.update(redis_address=redis_address)
        NodeServicesStarter(
            start_params, head=False, shutdown_at_exit=False, spawn_reaper=False)

    if head:
        if state:
            startup_msg = "CloudTik state started."
        elif controller:
            startup_msg = "CloudTik controller started."
        else:
            startup_msg = "CloudTik node started."
    else:
        startup_msg = "CloudTik node started."
    cli_logger.success(startup_msg)
    cli_logger.flush()


@node.command()
@click.option(
    "-f",
    "--force",
    is_flag=True,
    help="If set, will send SIGKILL instead of SIGTERM.")
@click.option(
    "--state",
    is_flag=True,
    hidden=True,
    default=False,
    help="If True, the redis state service will be stopped.",
)
@click.option(
    "--controller",
    is_flag=True,
    hidden=True,
    default=False,
    help="If True, the cluster controller service will be stopped.",
)
@add_click_logging_options
def stop(force, state, controller):
    """Stop CloudTik processes on the local machine."""

    def is_process_to_stop(process_name):
        if state or controller:
            if state and process_name == CLOUDTIK_PROCESS_REDIS:
                return True
            if controller and process_name == CLOUDTIK_PROCESS_CONTROLLER:
                return True
            return False
        else:
            return process_name in [
                CLOUDTIK_PROCESS_NODE_MONITOR, CLOUDTIK_PROCESS_LOG_MONITOR]

    is_linux = sys.platform.startswith("linux")
    processes_to_kill = [
        to_kill for to_kill in CLOUDTIK_PROCESSES
        if is_process_to_stop(to_kill[0])
    ]

    process_infos = []
    for proc in psutil.process_iter(["name", "cmdline"]):
        try:
            process_infos.append((proc, proc.name(), proc.cmdline()))
        except psutil.Error:
            pass

    total_found = 0
    total_stopped = 0
    stopped = []
    for keyword, filter_by_cmd, _, _ in processes_to_kill:
        if filter_by_cmd and is_linux and len(keyword) > 15:
            # getting here is an internal bug, so we do not use cli_logger
            msg = ("The filter string should not be more than {} "
                   "characters. Actual length: {}. Filter: {}").format(
                       15, len(keyword), keyword)
            raise ValueError(msg)

        found = []
        for candidate in process_infos:
            proc, proc_cmd, proc_args = candidate
            corpus = (proc_cmd
                      if filter_by_cmd else subprocess.list2cmdline(proc_args))
            if keyword in corpus:
                found.append(candidate)

        for proc, proc_cmd, proc_args in found:
            total_found += 1

            proc_string = str(subprocess.list2cmdline(proc_args))
            try:
                if force:
                    proc.kill()
                else:
                    # TODO: On Windows, this is forceful termination.
                    # We don't want CTRL_BREAK_EVENT, because that would
                    # terminate the entire process group. What to do?
                    proc.terminate()

                if force:
                    cli_logger.verbose("Killed `{}` {} ", cf.bold(proc_string),
                                       cf.dimmed("(via SIGKILL)"))
                else:
                    cli_logger.verbose("Send termination request to `{}` {}",
                                       cf.bold(proc_string),
                                       cf.dimmed("(via SIGTERM)"))

                total_stopped += 1
                stopped.append(proc)
            except psutil.NoSuchProcess:
                cli_logger.verbose(
                    "Attempted to stop `{}`, but process was already dead.",
                    cf.bold(proc_string))
                total_stopped += 1
            except (psutil.Error, OSError) as ex:
                cli_logger.error("Could not terminate `{}` due to {}",
                                 cf.bold(proc_string), str(ex))

    if total_found == 0:
        cli_logger.print("Did not find any active processes.")
    else:
        if total_stopped == total_found:
            cli_logger.success("Stopped {} processes.", total_stopped)
        else:
            cli_logger.warning(
                "Stopped only {} out of {} processes. "
                "Set `{}` to see more details.", total_stopped, total_found,
                cf.bold("-v"))
            cli_logger.warning("Try running the command again, or use `{}`.",
                               cf.bold("--force"))

    if state:
        # Stopping the state service means the end of cluster
        try:
            os.remove(
                os.path.join(get_cloudtik_home_dir(),
                             "cloudtik_current_cluster"))
        except OSError:
            # This just means the file doesn't exist.
            pass
    # Wait for the processes to actually stop.
    psutil.wait_procs(stopped, timeout=2)


@node.command(context_settings={"ignore_unknown_options": True})
@click.argument("script", required=True, type=str)
@click.argument("script_args", nargs=-1)
def run(script, script_args):
    """Runs a built-in script (bash or python or a registered command).

    If you want to execute any commands or user scripts, use exec or submit.
    """
    run_script(script, script_args)


@node.command()
@click.option(
    "--cpu",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Show total CPU available in the current environment - considering docker or K8S.")
@click.option(
    "--memory",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Show total memory in the current environment - considering docker or K8S.")
@click.option(
    "--in-mb",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Show total memory in MB.")
def resources(cpu, memory, in_mb):
    """Show system resource information of the node"""
    resource_spec = ResourceSpec().resolve(is_head=False, available_memory=False)
    if cpu:
        click.echo(resource_spec.num_cpus)
    elif memory:
        if in_mb:
            memory_in_mb = int(resource_spec.memory / (1024 * 1024))
            click.echo(memory_in_mb)
        else:
            click.echo(resource_spec.memory)
    else:
        static_resources = resource_spec.to_resource_dict()
        click.echo(static_resources)


@node.command(context_settings={"ignore_unknown_options": True})
@click.argument("identifier", required=True, type=str)
@click.argument("command", required=True, type=str)
@click.option(
    "--service-class",
    required=False,
    type=str,
    help="The python module and class to run for pulling")
@click.option(
    "--pull-script",
    required=False,
    type=str,
    help="The bash script or python script to run for pulling")
@click.option(
    "--logs-dir",
    required=False,
    default=None,
    type=str,
    help="Manually specify logs dir of this server process")
@click.option(
    "--no-redirect-output",
    is_flag=True,
    default=False,
    help="Do not redirect stdout and stderr to files")
@click.argument("script_args", nargs=-1)
def service(
        identifier, command,
        service_class, pull_script,
        logs_dir, no_redirect_output, script_args):
    """Start a pull service with pull class and parameters."""
    redirect_output = None if not no_redirect_output else True
    service_daemon(
        identifier, command,
        service_class, pull_script, script_args,
        logs_dir=logs_dir,
        redirect_output=redirect_output)


@node.command()
@click.argument("port", required=True, type=int)
@click.option(
    "--host",
    required=False,
    type=str,
    help="The host address to wait.")
@click.option(
    "--timeout",
    type=int,
    required=False,
    default=30,
    help=f"The number of seconds to wait.")
@click.option(
    "--free",
    is_flag=True,
    default=False,
    help="Wait for the port to be free. Default wait for in use.")
@add_click_logging_options
def wait_for_port(port, host, timeout, free):
    """Wait for port to be free or open"""
    _wait_for_port(port, host, timeout, free)


@node.command()
@click.option(
    "--node-type",
    required=False,
    type=str,
    default=None,
    help="The node type of the nodes.")
@click.option(
    "--runtime",
    required=False,
    type=str,
    default=None,
    help="The node which is configured with the runtime.")
@click.option(
    "--host",
    is_flag=True,
    default=False,
    help="Return the host instead of IP if hostname is available.")
@click.option(
    "--sort-by",
    required=False,
    type=str,
    default=None,
    help="Sort the list by a specific property. "
         "Sort by kind and ip if not specified. "
         "Valid values are: node_id, node_ip, node_kind, node_type, "
         "node_seq_id, heartbeat_time.")
@click.option(
    "--reverse",
    is_flag=True,
    default=False,
    help="Whether to sort in reverse order.")
@click.option(
    "--separator",
    required=False,
    type=str,
    default=None,
    help="The separator between worker hosts. Default is change a line.")
@add_click_logging_options
def nodes(
        node_type, runtime, host, sort_by, reverse, separator):
    """List live nodes in the cluster"""
    try:
        hosts = get_cluster_live_nodes_address(
            node_type=node_type, runtime_type=runtime,
            host=host, sort_by=sort_by, reverse=reverse)
        if hosts:
            if separator:
                click.echo(separator.join(hosts))
            else:
                click.echo("\n".join(hosts))
    except RuntimeError as re:
        fail_command("Failed to get cluster nodes.", re)


@node.command()
@click.option(
    "--runtime",
    required=False,
    type=str,
    default=None,
    help="The node which is configured with the runtime.")
@click.option(
    "--service",
    required=False,
    type=str,
    default=None,
    help="List the nodes for the service name.")
@click.option(
    "--service-type",
    required=False,
    type=str,
    default=None,
    help="List the nodes for the service type.")
@click.option(
    "--host",
    is_flag=True,
    default=False,
    help="Return the host instead of IP if hostname is available.")
@click.option(
    "--no-port",
    is_flag=True,
    default=False,
    help="Return the address without service port.")
@click.option(
    "--separator",
    required=False,
    type=str,
    default=None,
    help="The separator between hosts. Default is change a line.")
@add_click_logging_options
def service_nodes(
        runtime, service, service_type,
        host, no_port, separator):
    """List service nodes in the cluster"""
    try:
        node_addresses = get_service_node_addresses(
            runtime_type=runtime,
            service_name=service,
            service_type=service_type,
            host=host, no_port=no_port)
        if node_addresses:
            if separator:
                click.echo(separator.join(node_addresses))
            else:
                click.echo("\n".join(node_addresses))
    except RuntimeError as re:
        fail_command("Failed to get service nodes.", re)


@node.command()
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
    help="Collect debug_state.txt from session dir")
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
    "--runtimes",
    required=False,
    type=str,
    default=None,
    help="The list of runtimes to collect logs from")
@click.option(
    "--silent",
    required=False,
    type=bool,
    is_flag=True,
    default=False,
    help="Whether print a informational message.")
@add_click_logging_options
def dump(
        stream: bool = False,
        output: Optional[str] = None,
        logs: bool = True,
        debug_state: bool = True,
        pip: bool = True,
        processes: bool = True,
        processes_verbose: bool = False,
        tempfile: Optional[str] = None,
        runtimes: str = None,
        silent: bool = False):
    """Collect local data and package into an archive.

    Usage:

        cloudtik node dump [--stream/--output file]

    This script is called on remote nodes to fetch their data.
    """
    dump_local(
        stream=stream,
        output=output,
        logs=logs,
        debug_state=debug_state,
        pip=pip,
        processes=processes,
        processes_verbose=processes_verbose,
        tempfile=tempfile,
        runtimes=runtimes,
        silent=silent)


# core commands running on head and worker node
node.add_command(start)
node.add_command(stop)
node.add_command(run)
node.add_command(resources)
node.add_command(service)
node.add_command(wait_for_port)
node.add_command(nodes)
node.add_command(service_nodes)

# utility commands running on head or worker node for dump local data
node.add_command(dump)

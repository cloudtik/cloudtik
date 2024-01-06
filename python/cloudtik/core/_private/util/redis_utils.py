import os
import time

import psutil
import redis

from cloudtik.core._private import utils as utils, constants as constants
from cloudtik.core._private.constants import CLOUDTIK_DEFAULT_PORT
from cloudtik.core._private.util.core_utils import address_to_ip, address_from_string, get_node_ip_address, \
    address_string


def find_redis_address(address=None):
    """
    Attempts to find all valid redis addresses on this node.

    Returns:
        Set of detected Redis instances.
    """
    # Currently, this extracts the --redis-address from the command
    # that launched the service running on this node, if any.
    pids = psutil.pids()
    redis_addresses = set()
    for pid in pids:
        try:
            proc = psutil.Process(pid)
            # HACK: Workaround for UNIX idiosyncrasy
            # Normally, cmdline() is supposed to return the argument list.
            # But it in some cases (such as when setproctitle is called),
            # an arbitrary string resembling a command-line is stored in
            # the first argument.
            # Explanation: https://unix.stackexchange.com/a/432681
            # More info: https://github.com/giampaolo/psutil/issues/1179
            cmdline = proc.cmdline()
            # NOTE: To support Windows, we can't use
            # `os.path.basename(cmdline[0]) == "abc"` here.
            # TODO: use the right way to detect the redis
            if utils.find_name_in_command(cmdline, "cloudtik_cluster_controller") or \
                    utils.find_name_in_command(cmdline, "cloudtik_node_monitor") or \
                    utils.find_name_in_command(cmdline, "cloudtik_log_monitor"):
                for arglist in cmdline:
                    # Given we're merely seeking --redis-address, we just split
                    # every argument on spaces for now.
                    for arg in arglist.split(" "):
                        # TODO: Find a robust solution for locating Redis.
                        if arg.startswith("--redis-address="):
                            proc_addr = arg.split("=")[1]
                            if address is not None and address != proc_addr:
                                continue
                            redis_addresses.add(proc_addr)
        except psutil.AccessDenied:
            pass
        except psutil.NoSuchProcess:
            pass
    return redis_addresses


def get_address_to_use_or_die(head=False):
    """
    Attempts to find an address for an existing cluster if it is not
    already specified as an environment variable.
    Returns:
        A string to redis address
    """
    return os.environ.get(
        constants.CLOUDTIK_ADDRESS_ENV,
        find_redis_address_or_die(head=head))


def find_redis_address_or_die(head=False):
    redis_addresses = find_redis_address()
    if len(redis_addresses) > 1:
        raise ConnectionError(
            f"Found multiple active Redis instances: {redis_addresses}. "
            "Please specify the one to connect to by setting `address`.")
    elif not redis_addresses:
        if head:
            # the cluster controller may not be started for errors
            node_ip = get_node_ip_address()
            return address_string(node_ip, CLOUDTIK_DEFAULT_PORT)
        raise ConnectionError(
            "Could not find any running Redis instance. "
            "Please specify the one to connect to by setting `address`.")
    return redis_addresses.pop()


def validate_redis_address(address):
    """Validates address parameter.

    Returns:
        redis_address: string containing the full <host:port> address.
        redis_ip: string representing the ip portion of the address.
        redis_port: integer representing the port portion of the address.
    """

    if address == "auto":
        address = find_redis_address_or_die()

    redis_address = address_to_ip(address)
    redis_address_parts = redis_address.split(":")
    if len(redis_address_parts) != 2:
        raise ValueError(f"Malformed address. Expected '<host>:<port>',"
                         f" but got {redis_address} from {address}.")
    redis_ip = redis_address_parts[0]
    try:
        redis_port = int(redis_address_parts[1])
    except ValueError:
        raise ValueError("Malformed address port. Must be an integer.")
    if redis_port < 1024 or redis_port > 65535:
        raise ValueError("Invalid address port. Must "
                         "be between 1024 and 65535.")

    return address, redis_ip, redis_port


def create_redis_client(
        redis_address, password=None, prefer_ip=False):
    """Create a Redis client.

    Args:
        The IP address, port, and password of the Redis server.

    Returns:
        A Redis client.
    """
    if not hasattr(create_redis_client, "instances"):
        create_redis_client.instances = {}
    else:
        cli = create_redis_client.instances.get(redis_address)
        if cli is not None:
            try:
                cli.ping()
                return cli
            except Exception:
                create_redis_client.instances.pop(redis_address)

    (redis_address,
     redis_ip, redis_port) = validate_redis_address(redis_address)
    redis_host, _ = address_from_string(redis_address)
    if prefer_ip:
        redis_host = redis_ip
    # For this command to work, some other client (on the same machine
    # as Redis) must have run "CONFIG SET protected-mode no".
    create_redis_client.instances[redis_address] = redis.StrictRedis(
        host=redis_host, port=int(redis_port), password=password)

    return create_redis_client.instances[redis_address]


def wait_for_redis_to_start(redis_host, redis_port, password=None):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_host (str): The host address of the redis server.
        redis_port (int): The port of the redis server.
        password (str): The password of the redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, password=password)
    # Wait for the Redis server to start.
    num_retries = constants.CLOUDTIK_START_REDIS_WAIT_RETRIES
    delay = 0.001
    for i in range(num_retries):
        try:
            # Run some random command and see if it worked.
            redis_client.client_list()
        # If the Redis service is delayed getting set up for any reason, we may
        # get a redis.ConnectionError: Error 111 connecting to host:port.
        # Connection refused.
        # Unfortunately, redis.ConnectionError is also the base class of
        # redis.AuthenticationError. We *don't* want to obscure a
        # redis.AuthenticationError, because that indicates the user provided a
        # bad password. Thus a double except clause to ensure a
        # redis.AuthenticationError isn't trapped here.
        except redis.AuthenticationError as authEx:
            raise RuntimeError("Unable to connect to Redis at {}:{}.".format(
                redis_host, redis_port)) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_host}:"
                    f"{redis_port} after {num_retries} retries. Check that "
                    f"{redis_host}:{redis_port} is reachable from this "
                    "machine. If it is not, your firewall may be blocking "
                    "this port. If the problem is a flaky connection, try "
                    "setting the environment variable "
                    "`CLOUDTIK_START_REDIS_WAIT_RETRIES` to increase the number of"
                    " attempts to ping the Redis server.") from connEx
            # Wait a little bit.
            time.sleep(delay)
            delay *= 2
        else:
            break
    else:
        raise RuntimeError(
            f"Unable to connect to Redis (after {num_retries} retries). "
            "If the Redis instance is on a different machine, check that "
            "your firewall and relevant ports are configured properly. "
            "You can also set the environment variable "
            "`CLOUDTIK_START_REDIS_WAIT_RETRIES` to increase the number of "
            "attempts to ping the Redis server.")

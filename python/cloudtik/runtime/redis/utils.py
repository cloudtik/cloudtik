import copy
import os
import time
from typing import Any, Dict

from cloudtik.core._private.constants import CLOUDTIK_RUNTIME_ENV_HEAD_IP, CLOUDTIK_RUNTIME_ENV_NODE_IP, \
    CLOUDTIK_RUNTIME_ENV_WORKSPACE, CLOUDTIK_RUNTIME_ENV_CLUSTER
from cloudtik.core._private.core_utils import get_config_for_update, exec_with_call
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_REDIS
from cloudtik.core._private.runtime_utils import get_first_data_disk_dir, get_worker_ips_ready_from_head, \
    get_runtime_config_from_node, get_runtime_value, run_func_with_retry
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, SERVICE_DISCOVERY_FEATURE_KEY_VALUE, define_runtime_service_on_head, \
    define_runtime_service_on_worker
from cloudtik.core._private.utils import is_node_seq_id_enabled, enable_node_seq_id, \
    _sum_min_workers, get_runtime_config_for_update
from cloudtik.runtime.common.lock.runtime_lock import get_runtime_lock

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["redis-server", True, "Redis Server", "node"],
    ]

REDIS_SERVICE_PORT_CONFIG_KEY = "port"
REDIS_CLUSTER_PORT_CONFIG_KEY = "cluster_port"
REDIS_MASTER_SIZE_CONFIG_KEY = "master_size"
REDIS_RESHARD_DELAY_CONFIG_KEY = "reshard_delay"

REDIS_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
REDIS_CLUSTER_MODE_NONE = "none"
# simple cluster
REDIS_CLUSTER_MODE_SIMPLE = "simple"
# replication
REDIS_CLUSTER_MODE_REPLICATION = "replication"
# sharding cluster
REDIS_CLUSTER_MODE_SHARDING = "sharding"

REDIS_PASSWORD_CONFIG_KEY = "password"

REDIS_SERVICE_TYPE = BUILT_IN_RUNTIME_REDIS
REDIS_REPLICA_SERVICE_TYPE = REDIS_SERVICE_TYPE + "-replica"
REDIS_SERVICE_PORT_DEFAULT = 6379

REDIS_PASSWORD_DEFAULT = "cloudtik"

REDIS_START_WAIT_RETRIES = 32

REDIS_NODE_TYPE_MASTER = "master"
REDIS_NODE_TYPE_SLAVE = "slave"
REDIS_SHARDING_SLOTS = 16384

REDIS_RESHARD_DELAY_DEFAULT = 5


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_REDIS, {})


def _get_service_port(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_SERVICE_PORT_CONFIG_KEY, REDIS_SERVICE_PORT_DEFAULT)


def _get_cluster_port(redis_config: Dict[str, Any]):
    service_port = _get_service_port(redis_config)
    return redis_config.get(
        REDIS_CLUSTER_PORT_CONFIG_KEY, service_port + 10000)


def _get_cluster_mode(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_CLUSTER_MODE_CONFIG_KEY, REDIS_CLUSTER_MODE_REPLICATION)


def _get_master_size(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_MASTER_SIZE_CONFIG_KEY)


def _get_reshard_delay(redis_config: Dict[str, Any]):
    return redis_config.get(
        REDIS_RESHARD_DELAY_CONFIG_KEY, REDIS_RESHARD_DELAY_DEFAULT)


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_REDIS)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {"redis": logs_dir}


def update_config_master_size(cluster_config, master_size):
    runtime_config_to_update = get_runtime_config_for_update(cluster_config)
    redis_config_to_update = get_config_for_update(
        runtime_config_to_update, BUILT_IN_RUNTIME_REDIS)
    redis_config_to_update[REDIS_MASTER_SIZE_CONFIG_KEY] = master_size


def _configure_master_size(redis_config, cluster_config):
    num_static_nodes = _sum_min_workers(cluster_config) + 1
    if num_static_nodes < 3:
        raise RuntimeError("Redis Cluster for sharding requires at least 3 master nodes.")

    # WARNING: the static nodes when starting the cluster will
    # limit the number of masters.
    user_master_size = _get_master_size(redis_config)
    master_size = user_master_size
    if not master_size:
        # for sharding, decide the number of masters if not specified
        if num_static_nodes <= 5:
            master_size = num_static_nodes
        else:
            master_size = num_static_nodes // 2
    else:
        if master_size < 3:
            master_size = 3
        elif master_size > num_static_nodes:
            master_size = num_static_nodes
    if master_size != user_master_size:
        update_config_master_size(cluster_config, master_size)


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    redis_config = _get_config(runtime_config)
    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode != REDIS_CLUSTER_MODE_NONE:
        # We must enable the node seq id (stable seq id is preferred)
        # But we don't enforce it.
        if not is_node_seq_id_enabled(cluster_config):
            enable_node_seq_id(cluster_config)

        if cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
            _configure_master_size(redis_config, cluster_config)

    return cluster_config


def _validate_config(config: Dict[str, Any]):
    pass


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    redis_config = _get_config(runtime_config)

    service_port = _get_service_port(redis_config)
    runtime_envs["REDIS_SERVICE_PORT"] = service_port

    cluster_mode = _get_cluster_mode(redis_config)
    runtime_envs["REDIS_CLUSTER_MODE"] = cluster_mode

    if cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
        cluster_port = _get_cluster_port(redis_config)
        runtime_envs["REDIS_CLUSTER_PORT"] = cluster_port

        master_size = _get_master_size(redis_config)
        if not master_size:
            # This just for safety, master size will be checked at bootstrap
            master_size = 1
        runtime_envs["REDIS_MASTER_SIZE"] = master_size

    password = redis_config.get(
        REDIS_PASSWORD_CONFIG_KEY, REDIS_PASSWORD_DEFAULT)
    runtime_envs["REDIS_PASSWORD"] = password

    return runtime_envs


def _get_runtime_endpoints(runtime_config: Dict[str, Any], cluster_head_ip):
    service_port = _get_service_port(runtime_config)
    endpoints = {
        "redis": {
            "name": "Redis",
            "url": "{}:{}".format(cluster_head_ip, service_port)
        },
    }
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    service_port = _get_service_port(runtime_config)
    service_ports = {
        "redis": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_runtime_services(
        runtime_config: Dict[str, Any], cluster_name: str) -> Dict[str, Any]:
    redis_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(redis_config)
    service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, REDIS_SERVICE_TYPE)
    service_port = _get_service_port(redis_config)

    def define_redis_service(define_fn, service_type=None):
        if not service_type:
            service_type = REDIS_SERVICE_TYPE
        define_fn(
            service_type,
            service_discovery_config, service_port,
            features=[SERVICE_DISCOVERY_FEATURE_KEY_VALUE])

    cluster_mode = _get_cluster_mode(redis_config)
    if cluster_mode == REDIS_CLUSTER_MODE_REPLICATION:
        # primary service on head and replica service on workers
        replica_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, REDIS_REPLICA_SERVICE_TYPE)
        services = {
            service_name: define_redis_service(define_runtime_service_on_head),
            replica_service_name: define_redis_service(
                define_runtime_service_on_worker, REDIS_REPLICA_SERVICE_TYPE),
        }
    elif cluster_mode == REDIS_CLUSTER_MODE_SHARDING:
        # Service register for each node but don't give key-value feature to avoid
        # these service been discovered.
        # TODO: Ideally a middle layer needs to expose a client discoverable service.
        services = {
            service_name: define_redis_service(define_runtime_service),
        }
    elif cluster_mode == REDIS_CLUSTER_MODE_SIMPLE:
        services = {
            service_name: define_redis_service(define_runtime_service),
        }
    else:
        # single standalone on head
        services = {
            service_name: define_redis_service(define_runtime_service_on_head),
        }
    return services


###################################
# Calls from node at runtime
###################################


def _get_data_dir():
    data_disk_dir = get_first_data_disk_dir()
    if data_disk_dir:
        data_dir = os.path.join(data_disk_dir, "redis", "data")
    else:
        data_dir = os.path.join(_get_home_dir(), "data")
    return data_dir


def wait_for_redis_to_start(redis_ip_address, redis_port, password=None):
    """Wait for a Redis server to be available.

    This is accomplished by creating a Redis client and sending a random
    command to the server until the command gets through.

    Args:
        redis_ip_address (str): The IP address of the redis server.
        redis_port (int): The port of the redis server.
        password (str): The password of the redis server.

    Raises:
        Exception: An exception is raised if we could not connect with Redis.
    """
    import redis
    redis_client = redis.StrictRedis(
        host=redis_ip_address, port=redis_port, password=password)
    # Wait for the Redis server to start.
    num_retries = REDIS_START_WAIT_RETRIES
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
                redis_ip_address, redis_port)) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_ip_address}:"
                    f"{redis_port} after {num_retries} retries.") from connEx
            # Wait a little
            time.sleep(delay)
            delay *= 2
        else:
            break
    else:
        raise RuntimeError(
            f"Unable to connect to Redis (after {num_retries} retries). ")


def init_cluster_service(head):
    # TODO: choose to do one of the following:
    #  0. Do nothing if the node already done with joining the cluster
    #  1. Join to the bootstrap list
    #  2. Bootstrap the initial cluster
    #  3. Join the cluster as master and do a re-sharding
    #  4. Join the cluster as replica

    # We store a file in data dir to mark the node has initialized
    data_dir = _get_data_dir()
    cluster_init_file = os.path.join(data_dir, "cluster.init")
    if os.path.isfile(cluster_init_file):
        # already initialized
        return

    runtime_config = get_runtime_config_from_node(head)

    # if we are not initialized, the head will bootstrap the sharding
    if head:
        # For head, check whether there are workers running.
        worker_ips = get_worker_ips_ready_from_head(
            runtime=BUILT_IN_RUNTIME_REDIS)
        if not worker_ips:
            bootstrap_cluster()
        else:
            join_cluster_with_workers(runtime_config, worker_ips)
    else:
        # For workers, we assume the head must be bootstrapped and running
        join_cluster_with_head(runtime_config)

    finalize_init(cluster_init_file)


def finalize_init(cluster_init_file):
    # Creating cluster init file
    with open(cluster_init_file, 'w') as fp:
        pass


def bootstrap_cluster():
    # Bootstrap from head
    from redis import StrictRedis as Redis
    local_host = '127.0.0.1'
    password = get_runtime_value("REDIS_PASSWORD")
    port = get_runtime_value("REDIS_SERVICE_PORT")

    wait_for_redis_to_start(
        local_host, port, password=password)
    redis_client = Redis(
        host=local_host, port=port, password=password)
    # assign all slots to head to bootstrap a single node cluster
    redis_client.execute_command('CLUSTER', 'ADDSLOTSRANGE', 0, 16383)


def join_cluster_with_workers(runtime_config, worker_hosts):
    # Head cold restart with running workers
    # it means that the head is doing a cold restart (without original storage)
    # This should be the rare cases for a storage cluster.
    # For such cases, we assume that the existing workers are doing right and
    # the head is act as any other new workers to join the sharding the cluster
    # by contacting one of the live workers (or retry with others if fail)
    # WARNING: we should also avoid the case that if there are running workers
    # but they are not ready and then the head node get a restart.
    myid, node_host, port, password = meet_with_cluster(
        runtime_config, worker_hosts)
    assign_cluster_role(
        runtime_config, myid, node_host, port, password, worker_hosts)


def join_cluster_with_head(runtime_config):
    # Join a new worker with head

    # TODO: to support host names
    head_node_ip = get_runtime_value(CLOUDTIK_RUNTIME_ENV_HEAD_IP)
    if not head_node_ip:
        raise RuntimeError("Missing head node ip environment variable for the running node.")

    cluster_nodes = [head_node_ip]
    myid, node_host, port, password = meet_with_cluster(
        runtime_config, cluster_nodes)
    assign_cluster_role(
        runtime_config, myid, node_host, port, password, cluster_nodes)


def _get_myid(startup_nodes, node_host, port, password):
    from redis.cluster import RedisCluster
    from redis.cluster import ClusterNode

    redis_cluster = RedisCluster(
        startup_nodes=startup_nodes, password=password)
    myid_bytes = redis_cluster.cluster_myid(
        target_node=ClusterNode(node_host, port))
    if myid_bytes is None:
        raise RuntimeError(
            "Failed to get the node id: {}.".format(node_host))
    myid = myid_bytes.decode()
    return myid


def meet_with_cluster(runtime_config, cluster_nodes):
    from redis.cluster import RedisCluster
    from redis.cluster import ClusterNode

    node_ip = get_runtime_value(CLOUDTIK_RUNTIME_ENV_NODE_IP)
    if not node_ip:
        raise RuntimeError("Missing node ip environment variable for the running node.")

    node_host = node_ip
    port = get_runtime_value("REDIS_SERVICE_PORT")
    password = get_runtime_value("REDIS_PASSWORD")
    startup_nodes = [ClusterNode(cluster_node, port) for cluster_node in cluster_nodes]

    wait_for_redis_to_start(
        node_host, port, password=password)

    def cluster_meet():
        # retrying
        redis_cluster = RedisCluster(
            startup_nodes=startup_nodes, password=password)
        redis_cluster.cluster_meet(node_ip, port)
    run_func_with_retry(cluster_meet)

    # wait a few seconds for a better chance that other new masters join
    # so that the reshard is more efficiently which avoid move around repeatedly
    redis_config = _get_config(runtime_config)
    reshard_delay = _get_reshard_delay(redis_config)
    if reshard_delay:
        time.sleep(reshard_delay)

    # Although cluster meet is completed, we need to wait myself to appear
    myid = _get_myid(
        startup_nodes, node_host, port, password=password)

    def check_myself_show():
        redis_cluster = RedisCluster(
            startup_nodes=startup_nodes, password=password)
        _check_myself_show(redis_cluster, myid)

    run_func_with_retry(check_myself_show)
    return myid, node_ip, port, password


def _check_myself_show(redis_cluster, myid):
    nodes_info = _get_cluster_nodes_info(redis_cluster)
    if not nodes_info:
        raise RuntimeError("No cluster node information returned.")

    my_node_info = nodes_info.get(myid)
    if not my_node_info:
        raise RuntimeError(
            "Node with id {} doesn't show up in cluster yet.".format(myid))


def _get_task_lock(runtime_config, task_name):
    workspace_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_WORKSPACE)
    cluster_name = get_runtime_value(CLOUDTIK_RUNTIME_ENV_CLUSTER)
    lock_name = f"{workspace_name}.{cluster_name}.redis.{task_name}"
    return get_runtime_lock(runtime_config, lock_name)


def assign_cluster_role(
        runtime_config, myid, node_host, port, password, cluster_nodes):

    def retry_func():
        _assign_cluster_role(
            runtime_config, myid, node_host, port, password, cluster_nodes)

    lock = _get_task_lock(runtime_config, "role")
    with lock.hold():
        run_func_with_retry(retry_func)


def _parse_node_type(flags_str):
    if not flags_str:
        return None
    flags = flags_str.split(',')
    if REDIS_NODE_TYPE_MASTER in flags:
        return REDIS_NODE_TYPE_MASTER
    elif REDIS_NODE_TYPE_SLAVE in flags:
        return REDIS_NODE_TYPE_SLAVE
    return None


def _parse_link_state(link_state):
    return True if link_state == "connected" else False


def parse_slots(slots):
    result_slots = []
    for slot in slots:
        # Single number: 3894
        # Range: 3900-4000
        # Importing slot: [slot_number-<-importing_from_node_id]
        # Migrating slot: [slot_number->-migrating_to_node_id]
        if not slot or slot.startswith("["):
            continue
        slots_range = slot.split('-')
        result_slots.append(slots_range)
    return result_slots


def _get_cluster_nodes_info(redis_cluster):
    # The output of the command is just a space-separated CSV string,
    # where each line represents a node in the cluster.
    # <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv>
    # <config-epoch> <link-state> <slot> <slot> ... <slot>
    nodes_info_by_address = redis_cluster.cluster_nodes()
    if not nodes_info_by_address:
        raise RuntimeError("Failed to get the cluster nodes information.")

    nodes_info = {}
    for address, node_info in nodes_info_by_address.items():
        new_node_info = copy.deepcopy(node_info)
        new_node_info["address"] = address
        new_node_info["type"] = _parse_node_type(new_node_info["flags"])
        nodes_info[new_node_info["node_id"]] = new_node_info
    return nodes_info


def _parse_cluster_nodes_info(nodes_info_str):
    node_info_str_list = nodes_info_str.split("\n")
    nodes_info = {}
    for node_info_str in node_info_str_list:
        # split by space
        node_info_fields = node_info_str.split(' ')
        num_fields = len(node_info_fields)
        if num_fields < 9:
            raise RuntimeError(
                "Bad format for cluster node information. Expect at least {} fields got {}".format(
                    9, num_fields))
        if num_fields > 9:
            node_slots = parse_slots(node_info_fields[9:])
        else:
            node_slots = []
        node_info = {
            "node_id": node_info_fields[0],
            "address": node_info_fields[1],
            "type": _parse_node_type(node_info_fields[3]),
            "flags": node_info_fields[3],
            "master_id": node_info_fields[4],
            "connected": _parse_link_state(node_info_fields[8]),
            "slots": node_slots
        }
        nodes_info[node_info["node_id"]] = node_info

    return nodes_info


def _get_master_nodes(nodes_info):
    return {node_id: node_info for node_id, node_info in nodes_info.items()
            if node_info["type"] == REDIS_NODE_TYPE_MASTER}


def _assign_cluster_role(
        runtime_config, myid, node_host, port, password, cluster_nodes):
    # assign master role to add slots or act as replica of master
    from redis.cluster import RedisCluster
    from redis.cluster import ClusterNode

    startup_nodes = [ClusterNode(cluster_node, port) for cluster_node in cluster_nodes]
    redis_cluster = RedisCluster(
        startup_nodes=startup_nodes, password=password)

    nodes_info = _get_cluster_nodes_info(redis_cluster)
    if not nodes_info:
        raise RuntimeError("No cluster node information returned.")

    # get the list of the masters with slots assigned
    master_nodes = _get_master_nodes(nodes_info)
    if not master_nodes:
        # Shall we consider this is the first node and assign all?
        raise RuntimeError("No master node information returned.")

    # generate reshard plan and execute
    reshard_plan = _get_reshard_plan(myid, master_nodes)
    for reshard_action in reshard_plan:
        _execute_reshard(node_host, port, password, reshard_action)


def _get_reshard_plan(myid, master_nodes):
    # The simple algorithm
    # 1. count the total number of masters
    # 2. calculate the number of slots should assign to a single master.
    # 3. For each master which has more slots than it should have for the new number
    # move N slots to this node until this node have its part

    # Assume that we are already in the master list because I have meet
    total_masters = len(master_nodes)
    slots_per_master = REDIS_SHARDING_SLOTS // total_masters
    reshard_plan = []
    existing_slots = get_num_slots_of_node(myid, master_nodes)
    slots_remaining = slots_per_master - existing_slots
    for node_id, master_node in master_nodes.items():
        if node_id == myid:
            continue
        num_slots = _get_num_slots_of(master_node["slots"])
        if num_slots > slots_per_master:
            # try to move some to this node
            num_candidates = num_slots - slots_per_master
            if num_candidates > slots_remaining:
                num_candidates = slots_remaining

            if num_candidates > 0:
                reshard_action = {
                    "from": node_id,
                    "to": myid,
                    "slots": num_candidates
                }
                reshard_plan.append(reshard_action)
                slots_remaining -= num_candidates
                if slots_remaining <= 0:
                    # we get enough slots
                    break
    return reshard_plan


def get_num_slots_of_node(node_id, master_nodes):
    node_info = master_nodes.get(node_id)
    if not node_info:
        return 0
    return _get_num_slots_of(node_info["slots"])


def _get_num_slots_of(slots):
    if not slots:
        return 0
    num_slots = 0
    for slot in slots:
        if len(slot) == 1:
            num_slots += 1
        else:
            # it's a range
            slots_start = int(slot[0])
            slots_end = int(slot[1])
            num_slots += (slots_end - slots_start + 1)
    return num_slots


def _execute_reshard(node_host, port, password, reshard_action):
    # Execute
    # redis-cli -a cloudtik --no-auth-warning --cluster reshard host:port
    # --cluster-from from_id --cluster-to to_id --cluster-slots slots_num --cluster-yes
    cmd = ["redis-cli", "-a", password, "--no-auth-warning",
           "--cluster", "reshard", f"{node_host}:{port}",
           "--cluster-from", reshard_action["from"],
           "--cluster-to", reshard_action["to"],
           "--cluster-slots", str(reshard_action["slots"]),
           "--cluster-yes"]
    cmd_str = " ".join(cmd)

    # Log to a file with timestamp at logs dir
    logs_dir = os.path.join(_get_home_dir(), "logs")
    log_filename = "reshard-{}-{}".format(node_host, time.time_ns())
    reshard_log_file = os.path.join(logs_dir, log_filename)
    cmd_str = cmd_str + " >" + reshard_log_file + " 2>&1"
    exec_with_call(cmd_str)

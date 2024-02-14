import copy
import logging
import os
import sys
import time

from redis.cluster import RedisCluster
from redis.cluster import ClusterNode

from cloudtik.core._private.util.core_utils import exec_with_call
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_REDIS
from cloudtik.core._private.util.runtime_utils import get_first_data_disk_dir, get_worker_ips_ready_from_head, \
    get_runtime_config_from_node, get_runtime_value, run_func_with_retry, get_runtime_head_host, \
    get_runtime_node_host, get_runtime_node_ip, get_runtime_workspace_name, \
    get_runtime_cluster_name, get_runtime_node_type
from cloudtik.runtime.common.lock.runtime_lock import get_runtime_lock, get_runtime_lock_url
from cloudtik.runtime.redis.utils import _get_home_dir, _get_master_size, _get_config, _get_reshard_delay, \
    _get_sharding_config, _get_master_node_type

logger = logging.getLogger(__name__)

REDIS_START_WAIT_RETRIES = 32

REDIS_NODE_TYPE_MASTER = "master"
REDIS_NODE_TYPE_SLAVE = "slave"
REDIS_SHARDING_SLOTS = 16384

REDIS_CLUSTER_INIT_FILE = ".initialized"

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
    import redis
    redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, password=password)
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
                redis_host, redis_port)) from authEx
        except redis.ConnectionError as connEx:
            if i >= num_retries - 1:
                raise RuntimeError(
                    f"Unable to connect to Redis at {redis_host}:"
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
    cluster_init_file = os.path.join(data_dir, REDIS_CLUSTER_INIT_FILE)
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
            _bootstrap_cluster()
        else:
            _join_cluster_with_workers(runtime_config, worker_ips)
    else:
        # For workers, we assume the head must be bootstrapped and running
        _join_cluster_with_head(runtime_config)

    _finalize_init(cluster_init_file)


def _finalize_init(cluster_init_file):
    # Creating cluster init file
    with open(cluster_init_file, 'w') as fp:
        pass


def _bootstrap_cluster():
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


def _join_cluster_with_workers(runtime_config, worker_hosts):
    # Head cold restart with running workers
    # it means that the head is doing a cold restart (without original storage)
    # This should be the rare cases for a storage cluster.
    # For such cases, we assume that the existing workers are doing right and
    # the head is act as any other new workers to join the sharding the cluster
    # by contacting one of the live workers (or retry with others if fail)
    # WARNING: we should also avoid the case that if there are running workers
    # but they are not ready and then the head node get a restart.
    node_id, node_host, port, password = _meet_with_cluster(
        runtime_config, worker_hosts)
    _assign_cluster_role_with_lock(
        runtime_config, node_id, node_host, port,
        password, worker_hosts, head=True)


def _join_cluster_with_head(runtime_config):
    # Join a new worker with head
    head_host = get_runtime_head_host()
    cluster_nodes = [head_host]
    node_id, node_host, port, password = _meet_with_cluster(
        runtime_config, cluster_nodes)
    _assign_cluster_role_with_lock(
        runtime_config, node_id, node_host, port,
        password, cluster_nodes)


def _get_myid(startup_nodes, node_host, port, password):
    redis_cluster = RedisCluster(
        startup_nodes=startup_nodes, password=password)
    myid_bytes = redis_cluster.cluster_myid(
        target_node=ClusterNode(node_host, port))
    if myid_bytes is None:
        raise RuntimeError(
            "Failed to get the node id: {}.".format(node_host))
    myid = myid_bytes.decode()
    return myid


def _meet_with_cluster(runtime_config, cluster_nodes):
    node_ip = get_runtime_node_ip()
    node_host = get_runtime_node_host()
    port = get_runtime_value("REDIS_SERVICE_PORT")
    password = get_runtime_value("REDIS_PASSWORD")
    startup_nodes = [ClusterNode(cluster_node, port) for cluster_node in cluster_nodes]

    wait_for_redis_to_start(
        node_host, port, password=password)

    def cluster_meet():
        # retrying
        # https://github.com/redis/redis/issues/10433
        # CLUSTER MEET supports only IP. It doesn't support hostname.
        redis_cluster = RedisCluster(
            startup_nodes=startup_nodes, password=password)
        redis_cluster.cluster_meet(node_ip, port)
    run_func_with_retry(cluster_meet)

    # wait a few seconds for a better chance that other new masters join
    # so that the reshard is more efficiently which avoid move around repeatedly
    redis_config = _get_config(runtime_config)
    sharding_config = _get_sharding_config(redis_config)
    reshard_delay = _get_reshard_delay(sharding_config)
    if reshard_delay:
        time.sleep(reshard_delay)

    # Although cluster meet is completed, we need to wait myself to appear
    node_id = _get_myid(
        startup_nodes, node_host, port, password=password)

    def check_node_show():
        redis_cluster = RedisCluster(
            startup_nodes=startup_nodes, password=password)
        _check_node_show(redis_cluster, node_id)

    run_func_with_retry(check_node_show)
    return node_id, node_host, port, password


def _check_node_show(redis_cluster, node_id):
    _get_existing_node_info(redis_cluster, node_id)


def _get_task_lock(runtime_config, task_name):
    workspace_name = get_runtime_workspace_name()
    cluster_name = get_runtime_cluster_name()
    lock_name = f"{workspace_name}.{cluster_name}.redis.{task_name}"
    url = get_runtime_lock_url(runtime_config, BUILT_IN_RUNTIME_REDIS)
    return get_runtime_lock(url, lock_name)


def _assign_cluster_role_with_lock(
        runtime_config, node_id, node_host, port,
        password, cluster_nodes, head=False):

    def retry_func():
        _assign_cluster_role(
            runtime_config, node_id, node_host, port,
            password, cluster_nodes, head=head)

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


def _parse_slots(slots):
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
            node_slots = _parse_slots(node_info_fields[9:])
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


def _get_nodes_of_type(nodes_info, node_type):
    return {node_id: node_info for node_id, node_info in nodes_info.items()
            if node_info["type"] == node_type}


def _get_nodes_with_slots(nodes_info):
    return {node_id: node_info for node_id, node_info in nodes_info.items()
            if _get_num_slots_of(node_info["slots"]) > 0}


def _get_current_node_role(sharding_config, num_master_with_slots):
    # return True for master, False for replica
    master_node_type = _get_master_node_type(sharding_config)
    if master_node_type:
        node_type = get_runtime_node_type()
        if node_type != master_node_type:
            return False
    else:
        master_size = _get_master_size(sharding_config)
        if num_master_with_slots >= master_size:
            return False
    return True


def _assign_cluster_role(
        runtime_config, node_id, node_host, port,
        password, cluster_nodes, head=False):
    # assign master role to add slots or act as replica of master
    startup_nodes = [ClusterNode(cluster_node, port) for cluster_node in cluster_nodes]
    redis_cluster = RedisCluster(
        startup_nodes=startup_nodes, password=password)

    nodes_info = _get_cluster_nodes_info(redis_cluster)
    if not nodes_info:
        raise RuntimeError("No cluster node information returned.")

    # get the list of the masters with slots assigned
    master_nodes = _get_nodes_of_type(
        nodes_info, REDIS_NODE_TYPE_MASTER)
    if not master_nodes:
        # Shall we consider this is the first node and assign all?
        raise RuntimeError("No master node information returned.")

    master_nodes_with_slots = _get_nodes_with_slots(master_nodes)
    num_master_with_slots = len(master_nodes_with_slots)
    redis_config = _get_config(runtime_config)
    sharding_config = _get_sharding_config(redis_config)
    master_role = True if head else _get_current_node_role(
        sharding_config, num_master_with_slots)
    if not master_role:
        # this node will be a replica, choose a master
        slave_nodes = _get_nodes_of_type(
            nodes_info, REDIS_NODE_TYPE_SLAVE)
        master_replicas = _get_master_replicas(
            master_nodes_with_slots, slave_nodes)
        _assign_replica(
            master_replicas, node_id, node_host, port,
            password, startup_nodes)
    else:
        # generate reshard plan and execute
        reshard_plan = _get_reshard_plan(node_id, master_nodes)
        if reshard_plan:
            for reshard_action in reshard_plan:
                # TODO: run with retry here?
                _execute_reshard(node_host, port, password, reshard_action)

            _check_reshard_ok(redis_cluster, node_id)

            # TODO: rebalance the existing replica for the new master?


def _check_reshard_ok(redis_cluster, node_id):
    # we need wait slots to show in nodes
    def check_slots_assigned():
        _check_slots_assigned(redis_cluster, node_id)

    # one minutes
    run_func_with_retry(
        check_slots_assigned, num_retries=20, retry_interval=3)


def _get_existing_node_info(redis_cluster, node_id):
    nodes_info = _get_cluster_nodes_info(redis_cluster)
    if not nodes_info:
        raise RuntimeError("No cluster node information returned.")

    node_info = nodes_info.get(node_id)
    if not node_info:
        raise RuntimeError(
            "Node with id {} doesn't show up in cluster.".format(node_id))
    return node_info


def _check_slots_assigned(redis_cluster, node_id):
    node_info = _get_existing_node_info(redis_cluster, node_id)
    if node_info["type"] != REDIS_NODE_TYPE_MASTER:
        raise RuntimeError(
            "Node id {} is not set as master node.".format(node_id))

    num_slots = _get_num_slots_of(node_info["slots"])
    if num_slots <= 0:
        raise RuntimeError(
            "Node id {} has no slots assigned.".format(node_id))


def _get_master_replicas(master_nodes, slave_nodes):
    master_replicas = {}
    for node_id in master_nodes:
        master_replicas[node_id] = set()

    if not slave_nodes:
        return master_replicas

    for node_id, node_info in slave_nodes.items():
        master_id = node_info.get("master_id")
        if not master_id or master_id == "-":
            continue
        if master_id not in master_replicas:
            master_replicas[master_id] = {node_id}
        else:
            replicas = master_replicas[master_id]
            replicas.add(node_id)
    return master_replicas


def _get_master_with_minimum_replicas(master_replicas):
    min_master_id = None
    min_replicas = sys.maxsize
    for master_id, replicas in master_replicas.items():
        num_replicas = len(replicas)
        if num_replicas < min_replicas:
            min_master_id = master_id
            min_replicas = num_replicas
    return min_master_id


def _assign_replica(
        master_replicas, node_id, node_host, port,
        password, startup_nodes):
    master_id = _get_master_with_minimum_replicas(
        master_replicas)
    redis_cluster = RedisCluster(
        startup_nodes=startup_nodes, password=password)
    redis_cluster.cluster_replicate(
        target_nodes=[ClusterNode(node_host, port)],
        node_id=master_id)

    # we need wait the master id appears as master of this replica
    def check_replica_set():
        _check_replica_set(redis_cluster, node_id, master_id)

    # one minutes
    run_func_with_retry(
        check_replica_set, num_retries=20, retry_interval=3)


def _check_replica_set(redis_cluster, node_id, master_id):
    node_info = _get_existing_node_info(redis_cluster, node_id)
    if node_info["type"] != REDIS_NODE_TYPE_SLAVE:
        raise RuntimeError(
            "Node id {} is not set as replica node.".format(node_id))

    if node_info["master_id"] != master_id:
        raise RuntimeError(
            "Node id {} is not set a replica node of master {}.".format(
                node_id, master_id))


def _get_reshard_plan(node_id, master_nodes):
    # The simple algorithm
    # 1. count the total number of masters
    # 2. calculate the number of slots should assign to a single master.
    # 3. For each master which has more slots than it should have for the new number
    # move N slots to this node until this node have its part

    # Assume that we are already in the master list because I have meet
    total_masters = len(master_nodes)
    slots_per_master = REDIS_SHARDING_SLOTS // total_masters
    reshard_plan = []
    existing_slots = _get_num_slots_of_node(node_id, master_nodes)
    slots_remaining = slots_per_master - existing_slots
    for master_id, master_node in master_nodes.items():
        if master_id == node_id:
            continue
        num_slots = _get_num_slots_of(master_node["slots"])
        if num_slots > slots_per_master:
            # try to move some to this node
            num_candidates = num_slots - slots_per_master
            if num_candidates > slots_remaining:
                num_candidates = slots_remaining

            if num_candidates > 0:
                reshard_action = {
                    "from": master_id,
                    "to": node_id,
                    "slots": num_candidates
                }
                reshard_plan.append(reshard_action)
                slots_remaining -= num_candidates
                if slots_remaining <= 0:
                    # we get enough slots
                    break
    return reshard_plan


def _get_num_slots_of_node(node_id, master_nodes):
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

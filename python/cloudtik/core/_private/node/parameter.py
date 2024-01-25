import logging

import cloudtik.core._private.constants as constants

logger = logging.getLogger(__name__)


class StartParams:
    """A class used to store the start-up parameters used.

    Attributes:
        redis_address (str): The address of the Redis server to connect to. If
            this address is not provided, then this command will start Redis, a
            cluster controller, and some workers.
            It will also kill these processes when Python exits.
        redis_port (int): The port that the primary Redis shard should listen
            to. If None, then it will fall back to constants.CLOUDTIK_DEFAULT_PORT,
            or a random port if the default is not available.
        redis_shard_ports: A list of the ports to use for the non-primary Redis
            shards. If None, then it will fall back to the ports right after
            redis_port, or random ports if those are not available.
        redis_max_memory: The max amount of memory (in bytes) to allow redis
            to use, or None for no limit. Once the limit is exceeded, redis
            will start LRU eviction of entries. This only applies to the
            sharded redis tables (task and object tables).
        node_ip_address (str): The IP address of the node that we are on.
        num_redis_shards: The number of Redis shards to start in addition to
            the primary Redis shard.
        redis_max_clients: If provided, attempt to configure Redis with this
            maxclients number.
        redis_password (str): Prevents external clients without the password
            from connecting to Redis if provided.
        home_dir (str): If provided, it will specify the root directory of
            session data.
        no_log_monitor (bool): If False, then start a log monitor to
            monitor the log files for all processes on this node and push their
            contents to Redis.
        cluster_config: path to cluster config file.
        env_vars (dict): Override environment variables for the node.
        num_cpus (int): Number of CPUs to configure the cloudtik.
        num_gpus (int): Number of GPUs to configure the cloudtik.
        resources: A dictionary mapping the name of a resource to the quantity
            of that resource available.
        memory: Total available memory for workers requesting memory.
        redirect_output (bool): True if stdout and stderr for non-worker
            processes should be redirected to files and false otherwise.
        runtimes: Runtimes enabled on this node.
        node_type: The node type of this node.
        node_seq_id: The node SEQ ID of this node.
        state: Start state service on the head.
        controller: Start controller serviced on the head.
    """

    def __init__(
            self,
            redis_address=None,
            redis_max_memory=None,
            redis_port=None,
            redis_shard_ports=None,
            node_ip_address=None,
            num_redis_shards=None,
            redis_max_clients=None,
            redis_password=constants.CLOUDTIK_REDIS_DEFAULT_PASSWORD,
            home_dir=None,
            no_log_monitor=None,
            cluster_config=None,
            env_vars=None,
            resources=None,
            num_cpus=None,
            num_gpus=None,
            memory=None,
            redirect_output=None,
            runtimes=None,
            node_type=None,
            node_seq_id=None,
            state=False,
            controller=False,
    ):
        self.redis_address = redis_address
        self.redis_max_memory = redis_max_memory
        self.redis_port = redis_port
        self.redis_shard_ports = redis_shard_ports
        self.node_ip_address = node_ip_address
        self.num_redis_shards = num_redis_shards
        self.redis_max_clients = redis_max_clients
        self.redis_password = redis_password
        self.home_dir = home_dir
        self.no_log_monitor = no_log_monitor
        self.cluster_config = cluster_config
        self.env_vars = env_vars
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.redirect_output = redirect_output
        self.resources = resources
        self.runtimes = runtimes
        self.node_type = node_type
        self.node_seq_id = node_seq_id
        self.state = state
        self.controller = controller
        self._check_usage()

    def update(self, **kwargs):
        """Update the settings according to the keyword arguments.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if hasattr(self, arg):
                setattr(self, arg, kwargs[arg])
            else:
                raise ValueError(
                    f"Invalid StartParams parameter in update: {arg}")

        self._check_usage()

    def update_if_absent(self, **kwargs):
        """Update the settings when the target fields are None.

        Args:
            kwargs: The keyword arguments to set corresponding fields.
        """
        for arg in kwargs:
            if hasattr(self, arg):
                if getattr(self, arg) is None:
                    setattr(self, arg, kwargs[arg])
            else:
                raise ValueError(
                    "Invalid StartParams parameter in"
                    " update_if_absent: %s" % arg)

        self._check_usage()

    def update_pre_selected_port(self):
        """Update the pre-selected port information

        Returns:
            The dictionary mapping of component -> ports.
        """

        def wrap_port(port):
            # 0 port means select a random port for the grpc server.
            if port is None or port == 0:
                return []
            else:
                return [port]

        # Create a dictionary of the component -> port mapping.
        pre_selected_ports = {
            "redis": wrap_port(self.redis_port),
        }
        redis_shard_ports = self.redis_shard_ports
        if redis_shard_ports is None:
            redis_shard_ports = []
        pre_selected_ports["redis_shards"] = redis_shard_ports

        # Update the pre selected port set.
        self.reserved_ports = set()
        for comp, port_list in pre_selected_ports.items():
            for port in port_list:
                if port in self.reserved_ports:
                    raise ValueError(
                        f"Component {comp} is trying to use "
                        f"a port number {port} that is used by "
                        "other components.\n"
                        f"Port information: "
                        f"{self._format_ports(pre_selected_ports)}\n"
                        "If you allocate ports, "
                        "please make sure the same port is not used by "
                        "multiple components.")
                self.reserved_ports.add(port)

    def _check_usage(self):
        if self.resources is not None:
            def build_error(resource, alternative):
                return (
                    f"{self.resources} -> `{resource}` cannot be a "
                    "custom resource because it is one of the default resources "
                    f"({constants.CLOUDTIK_DEFAULT_RESOURCES}). "
                    f"Use `{alternative}` instead. For example, use `cloudtik node start "
                    f"--{alternative.replace('_', '-')}=1` instead of "
                    f"`cloudtik node start --resources={{'{resource}': 1}}`"
                )

            assert "CPU" not in self.resources, build_error("CPU", "num_cpus")
            assert "GPU" not in self.resources, build_error("GPU", "num_gpus")
            assert "memory" not in self.resources, build_error("memory", "memory")

    def _format_ports(self, pre_selected_ports):
        """Format the pre selected ports information to be more
        human readable.
        """
        ports = pre_selected_ports.copy()

        for comp, port_list in ports.items():
            if len(port_list) == 1:
                ports[comp] = port_list[0]
            elif len(port_list) == 0:
                # Nothing is selected, meaning it will be randomly selected.
                ports[comp] = "random"
            elif comp == "worker_ports":
                min_port = port_list[0]
                max_port = port_list[len(port_list) - 1]
                if len(port_list) < 50:
                    port_range_str = str(port_list)
                else:
                    port_range_str = f"from {min_port} to {max_port}"
                ports[comp] = f"{len(port_list)} ports {port_range_str}"
        return ports

import json
import logging
from typing import Any, Dict, Optional
import time
import urllib.error

from cloudtik.core._private import constants
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_YARN
from cloudtik.core._private.util.core_utils import address_to_ip, url_read
from cloudtik.core._private.state.state_utils import NODE_STATE_NODE_ID, NODE_STATE_NODE_IP, NODE_STATE_TIME
from cloudtik.core._private.utils import make_node_id, \
    convert_nodes_to_cpus, convert_nodes_to_memory, get_runtime_config, \
    get_resource_demands_for
from cloudtik.core.scaling_policy import ScalingPolicy, ScalingState, SCALING_INSTRUCTIONS_SCALING_TIME, \
    SCALING_INSTRUCTIONS_RESOURCE_DEMANDS, SCALING_NODE_STATE_TOTAL_RESOURCES, \
    SCALING_NODE_STATE_AVAILABLE_RESOURCES, SCALING_NODE_STATE_RESOURCE_LOAD

YARN_REST_ENDPOINT_CLUSTER_NODES = "http://{}:{}/ws/v1/cluster/nodes"
YARN_REST_ENDPOINT_CLUSTER_METRICS = "http://{}:{}/ws/v1/cluster/metrics"

YARN_SCALING_MODE_APPS_PENDING = "apps-pending"
YARN_SCALING_MODE_AGGRESSIVE = "aggressive"
YARN_SCALING_MODE_NONE = "none"

YARN_SCALING_RESOURCE_MEMORY = constants.CLOUDTIK_RESOURCE_MEMORY
YARN_SCALING_RESOURCE_CPU = constants.CLOUDTIK_RESOURCE_CPU

YARN_SCALING_STEP_DEFAULT = 1
APP_PENDING_THRESHOLD_DEFAULT = 1

# The free cores to check for scaling up
APP_PENDING_FREE_CORES_THRESHOLD_DEFAULT = 4
# The free memory MB to check for scaling up
APP_PENDING_FREE_MEMORY_THRESHOLD_DEFAULT = 1024
# When the free resource ratio is lower than this threshold, it starts up scaling
AGGRESSIVE_FREE_RATIO_THRESHOLD_DEFAULT = 0.1

logger = logging.getLogger(__name__)


def _address_to_ip(address):
    try:
        return address_to_ip(address)
    except Exception:
        return None


class YARNScalingPolicy(ScalingPolicy):
    def __init__(
            self,
            config: Dict[str, Any],
            head_host: str,
            rest_port) -> None:
        ScalingPolicy.__init__(self, config, head_host)

        # scaling parameters
        self.scaling_config = {}
        self.scaling_mode = YARN_SCALING_MODE_NONE
        self.scaling_step = YARN_SCALING_STEP_DEFAULT
        self.scaling_resource = YARN_SCALING_RESOURCE_MEMORY
        self.apps_pending_threshold = 1
        self.apps_pending_free_cores_threshold = APP_PENDING_FREE_CORES_THRESHOLD_DEFAULT
        self.apps_pending_free_memory_threshold = APP_PENDING_FREE_MEMORY_THRESHOLD_DEFAULT
        self.aggressive_free_ratio_threshold = AGGRESSIVE_FREE_RATIO_THRESHOLD_DEFAULT

        self._reset_yarn_config()

        self.rest_port = rest_port
        self.last_state_time = 0
        self.last_resource_demands_time = 0
        self.last_resource_state_snapshot = None

    def name(self):
        return "scaling-with-yarn"

    def _reset_yarn_config(self):
        runtime_config = get_runtime_config(self.config)
        yarn_config = runtime_config.get(BUILT_IN_RUNTIME_YARN, {})
        self.scaling_config = yarn_config.get("scaling", {})

        # Update the scaling parameters
        self.scaling_mode = self.scaling_config.get("scaling_mode", YARN_SCALING_MODE_NONE)
        self.scaling_step = self.scaling_config.get("scaling_step", YARN_SCALING_STEP_DEFAULT)
        self.scaling_resource = self.scaling_config.get("scaling_resource", YARN_SCALING_RESOURCE_MEMORY)
        self.apps_pending_threshold = self.scaling_config.get(
            "apps_pending_threshold", APP_PENDING_THRESHOLD_DEFAULT)
        self.apps_pending_free_cores_threshold = self.scaling_config.get(
            "apps_pending_free_cores_threshold", APP_PENDING_FREE_CORES_THRESHOLD_DEFAULT)
        self.apps_pending_free_memory_threshold = self.scaling_config.get(
            "apps_pending_free_memory_threshold", APP_PENDING_FREE_MEMORY_THRESHOLD_DEFAULT)
        self.aggressive_free_ratio_threshold = self.scaling_config.get(
            "aggressive_free_ratio_threshold", AGGRESSIVE_FREE_RATIO_THRESHOLD_DEFAULT)

    def get_scaling_state(self) -> Optional[ScalingState]:
        self.last_state_time = time.time()
        autoscaling_instructions = self._get_autoscaling_instructions()
        node_resource_states, lost_nodes = self._get_node_resource_states()

        scaling_state = ScalingState()
        scaling_state.set_autoscaling_instructions(autoscaling_instructions)
        scaling_state.set_node_resource_states(node_resource_states)
        scaling_state.set_lost_nodes(lost_nodes)
        return scaling_state

    def _need_more_nodes_for_cores(self, cluster_metrics):
        # TODO: Refine the algorithm here for better scaling decisions
        num_nodes = 0

        if self.scaling_mode == YARN_SCALING_MODE_AGGRESSIVE:
            # aggressive mode
            # When the availableVirtualCores are less than the configured threshold percentage
            # it starts to scaling up
            available = float(cluster_metrics["availableVirtualCores"])
            total = float(cluster_metrics["totalVirtualCores"])
            free_ratio = available/total
            if free_ratio < self.aggressive_free_ratio_threshold:
                num_nodes = self.scaling_step
        else:
            # apps-pending mode
            if (cluster_metrics["appsPending"] >= self.apps_pending_threshold
                    and cluster_metrics["availableVirtualCores"] < self.apps_pending_free_cores_threshold):
                num_nodes = self.scaling_step

        return num_nodes

    def _need_more_cores(self, cluster_metrics):
        num_nodes = self._need_more_nodes_for_cores(cluster_metrics)
        return self.get_number_of_cores_to_scale(num_nodes)

    def _need_more_nodes_for_memory(self, cluster_metrics):
        # TODO: Refine the algorithm here for better scaling decisions
        num_nodes = 0

        if self.scaling_mode == YARN_SCALING_MODE_AGGRESSIVE:
            # aggressive mode
            # When the availableMB are less than the configured threshold percentage
            # it starts to scaling up
            available = float(cluster_metrics["availableMB"])
            total = float(cluster_metrics["totalMB"])
            free_ratio = available/total
            if free_ratio < self.aggressive_free_ratio_threshold:
                num_nodes = self.scaling_step
        else:
            # apps-pending mode
            if (cluster_metrics["appsPending"] >= self.apps_pending_threshold
                    and cluster_metrics["availableMB"] < self.apps_pending_free_memory_threshold):
                num_nodes = self.scaling_step

        return num_nodes

    def _need_more_memory(self, cluster_metrics):
        num_nodes = self._need_more_nodes_for_memory(cluster_metrics)
        return self.get_memory_to_scale(num_nodes)

    def _need_more_resources(self, cluster_metrics):
        requesting_resources = {}
        if self.scaling_resource == YARN_SCALING_RESOURCE_CPU:
            resource_id = constants.CLOUDTIK_RESOURCE_CPU
            resource_amount = self._need_more_cores(cluster_metrics)
        else:
            resource_id = constants.CLOUDTIK_RESOURCE_MEMORY
            resource_amount = self._need_more_memory(cluster_metrics)
        if resource_amount > 0:
            requesting_resources[resource_id] = resource_amount
        return requesting_resources

    def get_number_of_cores_to_scale(self, nodes):
        if not nodes:
            return 0
        return convert_nodes_to_cpus(self.config, nodes)

    def get_memory_to_scale(self, nodes):
        if not nodes:
            return 0
        return convert_nodes_to_memory(self.config, nodes)

    def _get_autoscaling_instructions(self):
        # Use the following information to make the decisions
        """
            "appsPending": 0,
            "appsRunning": 0,

            "availableMB": 17408,
            "allocatedMB": 0,
            "totalMB": 17408,

            "availableVirtualCores": 7,
            "allocatedVirtualCores": 1,
            "totalVirtualCores": 8,

            "containersAllocated": 0,
            "containersReserved": 0,
            "containersPending": 0,
        """

        if not self.scaling_mode or self.scaling_mode == YARN_SCALING_MODE_NONE:
            return None

        cluster_metrics_url = YARN_REST_ENDPOINT_CLUSTER_METRICS.format(
            self.head_host, self.rest_port)
        try:
            content = url_read(cluster_metrics_url, timeout=10)
        except urllib.error.URLError as e:
            logger.error(
                "Failed to retrieve the cluster metrics: {}".format(str(e)))
            return None

        cluster_metrics_response = json.loads(content)

        autoscaling_instructions = {}
        resource_demands = []

        if "clusterMetrics" in cluster_metrics_response:
            cluster_metrics = cluster_metrics_response["clusterMetrics"]

            if logger.isEnabledFor(logging.DEBUG):
                cluster_info = {
                    "appsPending": cluster_metrics["appsPending"],
                    "appsRunning": cluster_metrics["appsRunning"],
                    "totalVirtualCores": cluster_metrics["totalVirtualCores"],
                    "allocatedVirtualCores": cluster_metrics["allocatedVirtualCores"],
                    "availableVirtualCores": cluster_metrics["availableVirtualCores"],
                    "totalMemoryMB": cluster_metrics["totalMB"],
                    "allocatedMemoryMB": cluster_metrics["allocatedMB"],
                    "availableMemoryMB": cluster_metrics["availableMB"],
                    "containersAllocated": cluster_metrics["containersAllocated"],
                    "containersPending": cluster_metrics["containersPending"],
                    "activeNodes": cluster_metrics["activeNodes"],
                    "unhealthyNodes": cluster_metrics["unhealthyNodes"],
                }
                logger.debug(
                    "Cluster metrics: {}".format(cluster_info))

            resource_requesting = self._need_more_resources(cluster_metrics)
            if resource_requesting:
                for resource_id, resource_amount in resource_requesting.items():
                    resource_demands_for_resource = get_resource_demands_for(
                        resource_amount, resource_id, self.config)
                    resource_demands += resource_demands_for_resource
                    self._log_scaling(resource_id, resource_amount, cluster_metrics)

                self.last_resource_demands_time = self.last_state_time
                self.last_resource_state_snapshot = {
                    "totalVirtualCores": cluster_metrics["totalVirtualCores"],
                    "allocatedVirtualCores": cluster_metrics["allocatedVirtualCores"],
                    "availableVirtualCores": cluster_metrics["availableVirtualCores"],
                    "totalMemoryMB": cluster_metrics["totalMB"],
                    "allocatedMemoryMB": cluster_metrics["allocatedMB"],
                    "availableMemoryMB": cluster_metrics["availableMB"],
                    "resource_requesting": resource_requesting,
                }

        autoscaling_instructions[SCALING_INSTRUCTIONS_SCALING_TIME] = self.last_state_time
        autoscaling_instructions[SCALING_INSTRUCTIONS_RESOURCE_DEMANDS] = resource_demands
        if len(resource_demands) > 0:
            logger.debug(
                "Resource demands: {}".format(resource_demands))

        return autoscaling_instructions

    def _log_scaling(
            self, resource_id, resource_amount, cluster_metrics):
        if self.scaling_resource == YARN_SCALING_RESOURCE_CPU:
            utilization_msg = "{}/{} cpus are free".format(
                cluster_metrics["availableVirtualCores"],
                cluster_metrics["totalVirtualCores"],)
        else:
            utilization_msg = "{}/{} memory are free".format(
                cluster_metrics["availableMB"],
                cluster_metrics["totalMB"])
        logger.info(
            "Scaling event: {}. Requesting {} more {}...".format(
                utilization_msg, resource_amount, resource_id))

    def _get_node_resource_states(self):
        # Use the following information to make the decisions
        """
            "nodeHostName":"host.domain.com",
            "nodeHTTPAddress":"host.domain.com:8042",
            "lastHealthUpdate": 1476995346399,
            "version": "3.0.0",
            "healthReport":"",
            "numContainers":0,
            "usedMemoryMB":0,
            "availMemoryMB":8192,
            "usedVirtualCores":0,
            "availableVirtualCores":8,
            "resourceUtilization":
            {
              "nodePhysicalMemoryMB":1027,
              "nodeVirtualMemoryMB":1027,
              "nodeCPUUsage":0.016661113128066063,
              "aggregatedContainersPhysicalMemoryMB":0,
              "aggregatedContainersVirtualMemoryMB":0,
              "containersCPUUsage":0
            }
        """

        cluster_nodes_url = YARN_REST_ENDPOINT_CLUSTER_NODES.format(
            self.head_host, self.rest_port)
        try:
            content = url_read(cluster_nodes_url, timeout=10)
        except urllib.error.URLError as e:
            logger.error("Failed to retrieve the cluster nodes metrics: {}".format(str(e)))
            return None, None

        cluster_nodes_response = json.loads(content)
        node_resource_states = {}
        lost_nodes = {}
        if ("nodes" in cluster_nodes_response
                and "node" in cluster_nodes_response["nodes"]):
            cluster_nodes = cluster_nodes_response["nodes"]["node"]
            for node in cluster_nodes:
                host_name = node["nodeHostName"]
                node_ip = _address_to_ip(host_name)
                if node_ip is None:
                    continue

                node_id = make_node_id(node_ip)
                if node["state"] != "RUNNING":
                    lost_nodes[node_id] = node_ip
                    continue

                total_resources = {
                    constants.CLOUDTIK_RESOURCE_CPU: node["availableVirtualCores"] + node["usedVirtualCores"],
                    constants.CLOUDTIK_RESOURCE_MEMORY: int(node["availMemoryMB"] + node["usedMemoryMB"]) * 1024 * 1024
                }
                free_resources = {
                    constants.CLOUDTIK_RESOURCE_CPU: node["availableVirtualCores"],
                    constants.CLOUDTIK_RESOURCE_MEMORY: int(node["availMemoryMB"]) * 1024 * 1024
                }
                cpu_load = 0.0
                if "resourceUtilization" in node:
                    cpu_load = node["resourceUtilization"].get("nodeCPUUsage", 0.0)
                resource_load = {
                    "load": {
                        constants.CLOUDTIK_RESOURCE_CPU: cpu_load
                    },
                    "in_use": True if node["numContainers"] > 0 else False
                }
                node_resource_state = {
                    NODE_STATE_NODE_ID: node_id,
                    NODE_STATE_NODE_IP: node_ip,
                    NODE_STATE_TIME: self.last_state_time,
                    SCALING_NODE_STATE_TOTAL_RESOURCES: total_resources,
                    SCALING_NODE_STATE_AVAILABLE_RESOURCES: free_resources,
                    SCALING_NODE_STATE_RESOURCE_LOAD: resource_load
                }
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "Node resources: {}".format(node_resource_state))
                node_resource_states[node_id] = node_resource_state

        # if the lost nodes appears in RUNNING, exclude it
        lost_nodes = {
            node_id: lost_nodes[node_id] for node_id in lost_nodes if node_id not in node_resource_states
        }

        return node_resource_states, lost_nodes

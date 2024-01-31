import json
import logging
import time
import traceback
from typing import Optional, Dict

from cloudtik.core._private.cluster.cluster_metrics import ClusterMetrics
from cloudtik.core._private.cluster.event_summarizer import EventSummarizer
from cloudtik.core._private.constants import CLOUDTIK_RESOURCE_REQUESTS, CLOUDTIK_CLUSTER_REQUESTS
from cloudtik.core._private.state.kv_store import kv_initialized, kv_get, kv_put, kv_save
from cloudtik.core._private.state.scaling_state import ScalingStateClient
from cloudtik.core._private.state.state_utils import NODE_STATE_NODE_IP, NODE_STATE_TIME
from cloudtik.core._private.util.core_utils import get_json_object_hash
from cloudtik.core.scaling_policy import SCALING_NODE_STATE_RESOURCE_LOAD, SCALING_NODE_STATE_TOTAL_RESOURCES, \
    SCALING_NODE_STATE_AVAILABLE_RESOURCES, SCALING_RESOURCE_REQUEST_REQUESTS, SCALING_RESOURCE_REQUEST_TIME, \
    SCALING_RESOURCE_REQUEST_OVERRIDE

logger = logging.getLogger(__name__)

MAX_FAILURES_FOR_LOGGING = 16


class ClusterMetricsUpdater:
    def __init__(
            self,
            cluster_metrics: ClusterMetrics,
            event_summarizer: Optional[EventSummarizer],
            scaling_state_client: ScalingStateClient):
        self.cluster_metrics = cluster_metrics
        self.event_summarizer = event_summarizer
        self.scaling_state_client = scaling_state_client
        self.last_avail_resources = None
        self.cluster_metrics_failures = 0
        self.last_cluster_requests_hash = None

    def initialize(self):
        # initialize the metrics update for existing resource requests
        # so that the cluster resources state is continued
        self.load_cluster_requests()

    def update(self):
        self._update_cluster_metrics()
        self._update_resource_requests()
        self._update_event_summary()

    def _update_cluster_metrics(self):
        try:
            heartbeat_nodes = {}
            self._update_node_heartbeats(heartbeat_nodes)
            self._update_scaling_metrics(heartbeat_nodes)

            # reset if there is a success
            self.cluster_metrics_failures = 0
        except Exception as e:
            if (self.cluster_metrics_failures == 0
                    or self.cluster_metrics_failures == MAX_FAILURES_FOR_LOGGING):
                # detailed form
                error = traceback.format_exc()
                logger.exception(
                    f"Load metrics update failed with the following error:\n{error}")
            elif self.cluster_metrics_failures < MAX_FAILURES_FOR_LOGGING:
                # short form
                logger.exception(
                    f"Load metrics update failed with the following error:{str(e)}")

            if self.cluster_metrics_failures == MAX_FAILURES_FOR_LOGGING:
                logger.exception(
                    f"The above error has been showed consecutively"
                    f" for {self.cluster_metrics_failures} times. Stop showing.")

            self.cluster_metrics_failures += 1

    def _update_node_heartbeats(
            self, heartbeat_nodes: Dict[str, str]):
        cluster_heartbeat_state = self.scaling_state_client.get_cluster_heartbeat_state()
        for node_id, node_heartbeat_state in cluster_heartbeat_state.node_heartbeat_states.items():
            ip = node_heartbeat_state.node_ip
            last_heartbeat_time = node_heartbeat_state.last_heartbeat_time
            heartbeat_nodes[node_id] = ip
            self.cluster_metrics.update_heartbeat(
                ip, node_id, last_heartbeat_time)

    def _update_scaling_metrics(
            self, heartbeat_nodes: Dict[str, str]):
        """Fetches resource usage data from control state and updates load metrics."""
        scaling_state = self.scaling_state_client.get_scaling_state()
        resource_requests_updated = self.cluster_metrics.update_autoscaling_instructions(
            scaling_state.autoscaling_instructions)
        if resource_requests_updated:
            self._save_cluster_requests()

        # If there is no scaling metrics for nodes, we still need to make sure
        # to set node last used status so that the idle nodes can be killed
        # We will depend on the timeout of a previous node resource states
        # if there is no latest reporting from that node
        node_resource_states = scaling_state.node_resource_states
        if node_resource_states is None:
            node_resource_states = {}

        for node_id, node_resource_state in node_resource_states.items():
            ip = node_resource_state[NODE_STATE_NODE_IP]
            resource_time = node_resource_state[NODE_STATE_TIME]
            # Node resource state
            total_resources = node_resource_state[SCALING_NODE_STATE_TOTAL_RESOURCES]
            available_resources = node_resource_state[SCALING_NODE_STATE_AVAILABLE_RESOURCES]
            resource_load = node_resource_state[SCALING_NODE_STATE_RESOURCE_LOAD]

            self.cluster_metrics.update_node_resources(
                ip, node_id, resource_time,
                total_resources, available_resources, resource_load)

        # All the nodes that shows in heartbeat but not in reported with node resources
        # We consider it is idle
        resource_time = time.time()
        for node_id, ip in heartbeat_nodes.items():
            if node_id in node_resource_states:
                continue
            total_resources = {}
            available_resources = {}
            resource_load = {}
            self.cluster_metrics.update_node_resources(
                ip, node_id, resource_time,
                total_resources, available_resources, resource_load)

    def _update_resource_requests(self):
        """Fetches resource requests from the internal KV and updates load."""
        if not kv_initialized():
            return

        try:
            data = kv_get(CLOUDTIK_RESOURCE_REQUESTS)
            if not data:
                return
            resource_requests = json.loads(data)
        except Exception:
            logger.exception(
                "Error loading resource requests")
            return

        request_resources = resource_requests.get(SCALING_RESOURCE_REQUEST_REQUESTS)
        request_override = resource_requests.get(SCALING_RESOURCE_REQUEST_OVERRIDE, False)
        updated = self.cluster_metrics.set_resource_requests(
            resource_requests[SCALING_RESOURCE_REQUEST_TIME],
            request_resources,
            override=request_override)
        if updated:
            # save the full view of resource requests
            self._save_cluster_requests()

    def _save_cluster_requests(self):
        resource_requests = self.cluster_metrics.get_resource_requests()
        last_requesting_time = self.cluster_metrics.last_requesting_time
        save_time = time.time()
        cluster_requests = {
            "save_time": save_time,
            "last_requesting_time": last_requesting_time,
            SCALING_RESOURCE_REQUEST_REQUESTS: resource_requests
        }
        cluster_requests_hash = get_json_object_hash(cluster_requests)
        if cluster_requests_hash != self.last_cluster_requests_hash:
            # save to state only when data changed
            self._do_save_cluster_requests(cluster_requests)
            self.last_cluster_requests_hash = cluster_requests_hash

    def _do_save_cluster_requests(self, cluster_requests):
        try:
            kv_put(
                CLOUDTIK_CLUSTER_REQUESTS,
                json.dumps(cluster_requests),
                overwrite=True)
            # We need the cluster requests being able to persist for restarting
            # Do a Save
            kv_save()
        except Exception:
            logger.exception(
                "Error saving cluster resource requests.")

    def load_cluster_requests(self):
        if not kv_initialized():
            return

        try:
            data = kv_get(CLOUDTIK_CLUSTER_REQUESTS)
            if not data:
                return
            cluster_requests = json.loads(data)
        except Exception:
            logger.exception(
                "Error loading cluster resource requests")
            return

        self.last_cluster_requests_hash = get_json_object_hash(
            cluster_requests)
        last_requesting_time = cluster_requests.get("last_requesting_time", 0)
        resource_requests = cluster_requests.get(SCALING_RESOURCE_REQUEST_REQUESTS)
        # TODO: check for resource requests for node types no longer exists
        self.cluster_metrics.initialize(
            resource_requests, last_requesting_time)

    def _update_event_summary(self):
        """Report the current size of the cluster.

        To avoid log spam, only cluster size changes (CPU or GPU count change)
        are reported to the event summarizer. The event summarizer will report
        only the latest cluster size per batch.
        """
        avail_resources = self.cluster_metrics.resources_avail_summary()
        if avail_resources != self.last_avail_resources:
            self.event_summarizer.add(
                "Resized to {}.",  # e.g., Resized to 100 CPUs, 4 GPUs.
                quantity=avail_resources,
                aggregate=lambda old, new: new)
            self.last_avail_resources = avail_resources

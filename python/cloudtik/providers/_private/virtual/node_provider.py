import copy
import logging
from types import ModuleType
from typing import Any, Dict, Optional, List

from cloudtik.core._private.call_context import CallContext
from cloudtik.core._private.utils import FILE_MOUNTS_CONFIG_KEY, AUTH_CONFIG_KEY, get_head_node_config, \
    _is_permanent_data_volumes
from cloudtik.core.command_executor import CommandExecutor
from cloudtik.core.node_provider import NodeProvider
from cloudtik.core.tags import CLOUDTIK_TAG_CLUSTER_NAME, CLOUDTIK_TAG_WORKSPACE_NAME
from cloudtik.providers._private.virtual.config import prepare_virtual, post_prepare_virtual, bootstrap_virtual, \
    bootstrap_virtual_for_api, delete_cluster_disks
from cloudtik.providers._private.virtual.virtual_container_scheduler import VirtualContainerScheduler

logger = logging.getLogger(__name__)


class VirtualNodeProvider(NodeProvider):
    """NodeProvider for automatically managed containers on local node.

    The container management is handled by ssh to docker0 bridge of
    local node and execute docker commands over it.
    On working node environment, it can simply to run docker commands.
    Within the cluster environment, for example, on head, you cannot run docker command
    because it is within the container. For this case, it ssh to docker0 and
    execute all the commands including run new docker containers, attach to a worker container.
    """

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)
        self.virtual_scheduler = VirtualContainerScheduler(
            provider_config, cluster_name)

    def non_terminated_nodes(self, tag_filters):
        # Only get the non terminated nodes associated with this cluster name.
        tag_filters[CLOUDTIK_TAG_CLUSTER_NAME] = self.cluster_name
        tag_filters[CLOUDTIK_TAG_WORKSPACE_NAME] = self.provider_config["workspace_name"]
        return self.virtual_scheduler.non_terminated_nodes(tag_filters)

    def is_running(self, node_id):
        return self.virtual_scheduler.is_running(node_id)

    def is_terminated(self, node_id):
        return self.virtual_scheduler.is_terminated(node_id)

    def node_tags(self, node_id):
        return self.virtual_scheduler.node_tags(node_id)

    def external_ip(self, node_id):
        return self.virtual_scheduler.external_ip(node_id)

    def internal_ip(self, node_id):
        return self.virtual_scheduler.internal_ip(node_id)

    def create_node(self, node_config, tags, count):
        # Tag the newly created node with this cluster name. Helps to get
        # the right nodes when calling non_terminated_nodes.
        tags[CLOUDTIK_TAG_CLUSTER_NAME] = self.cluster_name
        tags[CLOUDTIK_TAG_WORKSPACE_NAME] = self.provider_config["workspace_name"]
        self.virtual_scheduler.create_node(
            node_config, tags, count)

    def set_node_tags(self, node_id, tags):
        self.virtual_scheduler.set_node_tags(node_id, tags)

    def terminate_node(self, node_id):
        self.virtual_scheduler.terminate_node(node_id)

    def terminate_nodes(self, node_ids: List[str]) -> Optional[Dict[str, Any]]:
        return self.virtual_scheduler.terminate_nodes(node_ids)

    def get_node_info(self, node_id):
        return self.virtual_scheduler.get_node_info(node_id)

    def with_environment_variables(
            self, node_type_config: Dict[str, Any], node_id: str):
        return {}

    def get_command_executor(
            self,
            call_context: CallContext,
            log_prefix: str,
            node_id: str,
            auth_config: Dict[str, Any],
            cluster_name: str,
            process_runner: ModuleType,
            use_internal_ip: bool,
            docker_config: Optional[Dict[str, Any]] = None
    ) -> CommandExecutor:

        return self.virtual_scheduler.get_command_executor(
            call_context=call_context,
            log_prefix=log_prefix,
            node_id=node_id,
            auth_config=auth_config,
            cluster_name=cluster_name,
            process_runner=process_runner,
            use_internal_ip=use_internal_ip,
            docker_config=docker_config
        )

    def prepare_config_for_head(
            self, cluster_config: Dict[str, Any],
            remote_config: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a new cluster config with custom configs for head node."""
        # Set in cluster flag
        remote_config["provider"]["virtual_in_cluster"] = True

        # auth was changed for remote config too
        if AUTH_CONFIG_KEY in remote_config:
            remote_config["provider"][AUTH_CONFIG_KEY] = copy.deepcopy(
                remote_config[AUTH_CONFIG_KEY])

        # copy file mounts to provider since file_mounts updated for head
        if FILE_MOUNTS_CONFIG_KEY in remote_config:
            remote_config["provider"][FILE_MOUNTS_CONFIG_KEY] = copy.deepcopy(
                remote_config[FILE_MOUNTS_CONFIG_KEY])

        # copy file mounts to provider since file_mounts updated for local
        if FILE_MOUNTS_CONFIG_KEY in cluster_config:
            cluster_config["provider"][FILE_MOUNTS_CONFIG_KEY] = copy.deepcopy(
                cluster_config[FILE_MOUNTS_CONFIG_KEY])

        return remote_config

    def prepare_node_config_for_launch_hash(
            self, node_config: Dict[str, Any]) -> Dict[str, Any]:
        if "port_mappings" in node_config:
            node_config = copy.deepcopy(node_config)
            node_config.pop("port_mappings", None)
            return node_config
        return node_config

    def prepare_config_for_runtime_hash(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        node_config = get_head_node_config(cluster_config)
        if "port_mappings" in node_config:
            cluster_config = copy.deepcopy(cluster_config)
            node_config = get_head_node_config(cluster_config)
            node_config.pop("port_mappings", None)
            return cluster_config

        return cluster_config

    def cleanup_cluster(
            self, cluster_config: Dict[str, Any], deep: bool = False):
        """Cleanup the cluster by deleting additional resources other than the nodes.
        If deep flag is true, do a deep clean up all the resources
        """
        if deep and _is_permanent_data_volumes(self.provider_config):
            delete_cluster_disks(
                self.provider_config, self.cluster_name, cluster_config)

    @staticmethod
    def bootstrap_config(cluster_config):
        return bootstrap_virtual(cluster_config)

    @staticmethod
    def bootstrap_config_for_api(
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return bootstrap_virtual_for_api(cluster_config)

    @staticmethod
    def prepare_config(cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return prepare_virtual(cluster_config)

    @staticmethod
    def post_prepare(
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Fills out missing fields after the user config is merged
        with defaults and before validate"""
        return post_prepare_virtual(cluster_config)

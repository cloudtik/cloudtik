import logging
from typing import Any, Dict, Optional

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.mongodb.utils import _get_runtime_processes, \
    _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _get_runtime_endpoints, _get_head_service_ports, \
    _validate_config, _bootstrap_runtime_config, \
    _prepare_config_on_head, _node_configure, _prepare_config, register_service

logger = logging.getLogger(__name__)


class MongoDBRuntime(RuntimeBase):
    """Implementation for MongoDB Runtime for a high available replicated
    MongoDB cluster.
    It supports the following topology:
    1. A standalone server: on head
    2. A replication cluster: primary and secondary on head and workers
    3. A sharding cluster: shards on workers

    Notice of limitations:
    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare runtime specific configurations"""
        return _prepare_config(self.runtime_config, cluster_config)

    def bootstrap_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Final chance to update the config with runtime specific configurations
        This happens after provider bootstrap_config is done.
        """
        cluster_config = _bootstrap_runtime_config(
            self.runtime_config, cluster_config)
        return cluster_config

    def prepare_config_on_head(
            self, cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Configure runtime such as using service discovery to configure
        internal service addresses the runtime depends.
        The head configuration will be updated and saved with the returned configuration.
        """
        return _prepare_config_on_head(
            self.runtime_config, cluster_config)

    def validate_config(self, cluster_config: Dict[str, Any]):
        """Validate cluster configuration from runtime perspective."""
        _validate_config(cluster_config)

    def with_environment_variables(
            self, config: Dict[str, Any], provider: NodeProvider,
            node_id: str) -> Dict[str, Any]:
        """Export necessary runtime environment variables for running node commands.
        For example: {"ENV_NAME": value}
        """
        return _with_runtime_environment_variables(
            self.runtime_config, config=config)

    def node_configure(self, head: bool):
        """ This method is called on every node as the first step of executing runtime
        configure command.
        """
        _node_configure(self.runtime_config, head)

    def cluster_booting_completed(
            self, cluster_config: Dict[str, Any], head_node_id: str) -> None:
        register_service(
            self.runtime_config, cluster_config, head_node_id)

    def get_runtime_endpoints(
            self, cluster_config: Dict[str, Any], cluster_head_ip: str):
        return _get_runtime_endpoints(
            self.runtime_config, cluster_config, cluster_head_ip)

    def get_head_service_ports(self) -> Optional[Dict[str, Any]]:
        return _get_head_service_ports(self.runtime_config)

    def get_runtime_services(self, cluster_config: Dict[str, Any]):
        return _get_runtime_services(self.runtime_config, cluster_config)

    @staticmethod
    def get_logs() -> Dict[str, str]:
        return _get_runtime_logs()

    @staticmethod
    def get_processes():
        return _get_runtime_processes()

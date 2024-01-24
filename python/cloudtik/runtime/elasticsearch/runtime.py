import logging
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MOUNT
from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.elasticsearch.utils import _get_runtime_processes, \
    _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _get_runtime_endpoints, _get_head_service_ports, \
    _validate_config, _bootstrap_runtime_config

logger = logging.getLogger(__name__)


class ElasticSearchRuntime(RuntimeBase):
    """Implementation of ElasticSearch Runtime for a standalone instance, a
    ElasticSearch Cluster.
    It supports the following topology:
    1. A standalone server: on head
    2. A cluster: with multiple roles on each node or roles based on node
    types.

    Notice of limitations:

    Hints:
    1. Checking cluster status:
    curl 'http://host:9200/_cat/nodes'
    secure cluster: curl -k --user elastic:password 'https://host:9200/_cat/nodes'
    2. Reset elastic user password:
    elasticsearch-reset-password -u elastic
    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def bootstrap_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Final chance to update the config with runtime specific configurations
        This happens after provider bootstrap_config is done.
        """
        cluster_config = _bootstrap_runtime_config(
            self.runtime_config, cluster_config)
        return cluster_config

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

    @staticmethod
    def get_dependencies():
        # Enable repository path need the use of mount runtime to provide
        # distributed file system as local folder.
        return [BUILT_IN_RUNTIME_MOUNT]

import logging
from typing import Any, Dict, Optional

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.mysql.utils import _get_runtime_processes, \
    _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _get_runtime_endpoints, _get_head_service_ports, _validate_config, _bootstrap_runtime_config, \
    _get_health_check

logger = logging.getLogger(__name__)


class MySQLRuntime(RuntimeBase):
    """Implementation of MySQL Runtime for a high available replicated
    MySQL database cluster.
    It supports the following topology:
    1. A standalone server: on head
    2. A replication cluster: primary on head and replicas on workers
    3. A group replication cluster: A single primary or all possible primary
    but must bootstrap from head.

    Notice of limitations:
    1. For replication cluster, we currently don't allow to run primary on workers.
    2. For group replication cluster, we :
        A. Support a fresh start of new cluster
        B. Support a full restart of an existing cluster and assuming the head node has the
        latest data.
        C. Head node restart with workers are running is not yet supported.
        D. Every node can be possibly become the primary. Client or a middle layer needs
        to handle which primary node to connect for write.

    Hints:
    1. Checking replication status:
    mysql -u root -h host --password=password mysql <<< "SHOW REPLICA STATUS"
    2. Checking group replication status:
    mysql -u root -h host --password=password mysql <<< "SELECT * FROM performance_schema.replication_group_members"

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

    def get_health_check(
            self,
            cluster_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return _get_health_check(self.runtime_config, cluster_config)

    @staticmethod
    def get_logs() -> Dict[str, str]:
        return _get_runtime_logs()

    @staticmethod
    def get_processes():
        return _get_runtime_processes()

import logging
from typing import Any, Dict, Optional

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.redis.utils import _get_runtime_processes, \
    _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _get_runtime_endpoints, _get_head_service_ports, _validate_config, _bootstrap_runtime_config, \
    _get_health_check

logger = logging.getLogger(__name__)


class RedisRuntime(RuntimeBase):
    """Implementation of Redis Runtime for a standalone instance, a simple cluster
    or replicated Redis Cluster or a sharding Redis Cluster.
    It supports the following topology:
    1. A standalone server: on head
    2. A simple cluster of standalone servers: on head and workers
    3. A replication cluster: primary on head and replicas on workers
    4. A sharding cluster: with multiple shards and replicas.

    Notice of limitations:
    1. For simple cluster, each instance is standalone and user will need to do manual
    sharding by keys if needed.
    2. For replication cluster
    If sentinel is not enabled, we currently don't allow to run primary on workers. All
    the workers are configured to replicate from the head and in read-only mode.
    If sentinel is enabled, a replica may be promoted to primary and new nodes will replica
    from the new primary. And the existing replica will follow the new primary.
    In this mode, we can either use a sentinel aware client or use a load balancer with role
    based health checker.
    3. Cluster aware client library:
    To use a client library with Redis Cluster, the client libraries need to be cluster-aware.
    Check the following for more information:
    https://developer.redis.com/operate/redis-at-scale/scalability/redis-cluster-and-client-libraries/

    Hints:
    1. Checking replication status:
    redis-cli -p 6379 -a cloudtik role
    2. Checking cluster status:
    redis-cli -p 6379 -a cloudtik cluster nodes

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

import logging
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_POSTGRES
from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.pgbouncer.utils import _get_runtime_processes, \
    _get_runtime_endpoints, _get_head_service_ports, _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _bootstrap_runtime_config, _validate_config, _prepare_config

logger = logging.getLogger(__name__)


class PgBouncerRuntime(RuntimeBase):
    """Implementation for PgBouncer Runtime for connection pool a Postgres cluster for client.

    Hints:
    1. Checking status:
        PGPASSWORD=cloudtik psql -p 6432 -h localhost -U pgbouncer -d pgbouncer <<< "SHOW STATS;"
    SHOW STATS: Displays transaction count, timing, etc.
    SHOW POOLS: Displays active, waiting client and server counts.
                It also shows how long the oldest client waited in the queue.
                It is very helpful when determining pool_size.
    SHOW SERVERS: Displays information about database connections made by PgBouncer.
    SHOW CLIENTS: Displays information about clients that connected via PgBouncer.
    SHOW DATABASES: Displays information about configured databases.
    2. Testing the pool:
        PGPASSWORD=cloudtik psql -p 6432 -h localhost -U cloudtik -d database_name
    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return _prepare_config(self.runtime_config, cluster_config)

    def bootstrap_config(
            self,
            cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return _bootstrap_runtime_config(self.runtime_config, cluster_config)

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
        return [BUILT_IN_RUNTIME_POSTGRES]

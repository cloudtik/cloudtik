import logging
from typing import Any, Dict, Optional

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.grafana.utils import _get_runtime_processes, \
    _get_runtime_endpoints, _get_head_service_ports, _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs

logger = logging.getLogger(__name__)


class GrafanaRuntime(RuntimeBase):
    """Implementation for Grafana Runtime for monitor visualization"""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

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

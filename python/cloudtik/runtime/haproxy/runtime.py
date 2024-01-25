import logging
from typing import Any, Dict, Optional

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.haproxy.utils import _get_runtime_processes, \
    _get_runtime_endpoints, _get_head_service_ports, _get_runtime_services, _with_runtime_environment_variables, \
    _validate_config, _get_runtime_logs, _prepare_config_on_head, _node_configure

logger = logging.getLogger(__name__)


class HAProxyRuntime(RuntimeBase):
    """Implementation for HAProxy Runtime for Load Balancer

    Hints:
    1. Check HAProxy status with runtime API:
    Install socat: sudo apt-get update -y && sudo apt-get install socat -y
    Show help: echo "help" | socat stdio tcp4-connect:127.0.0.1:19999
    Show states of backend servers: echo "show stat" | socat stdio tcp4-connect:127.0.0.1:19999

    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config_on_head(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Configure runtime such as using service discovery to configure
        internal service addresses the runtime depends.
        The head configuration will be updated and saved with the returned configuration.
        """
        return _prepare_config_on_head(self.runtime_config, cluster_config)

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

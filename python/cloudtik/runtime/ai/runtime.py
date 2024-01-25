import logging
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MYSQL, BUILT_IN_RUNTIME_POSTGRES, \
    BUILT_IN_RUNTIME_SSHSERVER, BUILT_IN_RUNTIME_MOUNT
from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.ai.utils import _with_runtime_environment_variables, \
    _get_runtime_processes, _get_runtime_logs, _get_runtime_endpoints, register_service, _get_head_service_ports, \
    _get_runtime_services, _prepare_config_on_head, _prepare_config, _node_configure, _node_services
from cloudtik.runtime.common.runtime_base import RuntimeBase

logger = logging.getLogger(__name__)


class AIRuntime(RuntimeBase):
    """Implementation for AI Runtime"""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare runtime specific configurations"""
        return _prepare_config(cluster_config)

    def prepare_config_on_head(
            self, cluster_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Configure runtime such as using service discovery to configure
        internal service addresses the runtime depends.
        The head configuration will be updated and saved with the returned configuration.
        """
        return _prepare_config_on_head(cluster_config)

    def with_environment_variables(
            self, config: Dict[str, Any], provider: NodeProvider,
            node_id: str) -> Dict[str, Any]:
        """Export necessary runtime environment variables for running node commands.
        For example: {"ENV_NAME": value}
        """
        return _with_runtime_environment_variables(
            self.runtime_config, config=config,
            provider=provider, node_id=node_id)

    def node_configure(self, head: bool):
        """ This method is called on every node as the first step of executing runtime
        configure command.
        """
        _node_configure(self.runtime_config, head)

    def node_services(self, head: bool):
        """ This method is called on every node as the first step of executing runtime
        services command.
        """
        _node_services(self.runtime_config, head)

    def cluster_booting_completed(
            self, cluster_config: Dict[str, Any], head_node_id: str) -> None:
        register_service(cluster_config, head_node_id)

    def get_runtime_endpoints(
            self, cluster_config: Dict[str, Any], cluster_head_ip: str):
        return _get_runtime_endpoints(cluster_config, cluster_head_ip)

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
        return [
            BUILT_IN_RUNTIME_MYSQL,
            BUILT_IN_RUNTIME_POSTGRES,
            BUILT_IN_RUNTIME_SSHSERVER,
        ]

    @staticmethod
    def get_required():
        return [BUILT_IN_RUNTIME_MOUNT]

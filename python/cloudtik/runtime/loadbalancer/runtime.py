import logging
from typing import Any, Dict

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.loadbalancer.utils import _get_runtime_processes, \
    _with_runtime_environment_variables, \
    _validate_config, _get_runtime_logs, _prepare_config_on_head, _node_configure, _bootstrap_runtime_config, \
    _prepare_config

logger = logging.getLogger(__name__)


class LoadBalancerRuntime(RuntimeBase):
    """Implementation for LoadBalancer Runtime for controlling the cloud Load Balancer
    creating and targets updating.
    This is not the real load balancer service but the controller of a cloud load balancer.
    The Load Balancer runtime will create real Cloud Balancer based on service configurations.
    Usually we use Cloud Load Balancer as the public available user entry to services.
    For load balancer for internal services which is not available to public, you can use
    the runtime such as HAProxy or NGINX which is more for your control.
    Of course, you can still use this to create internal load balancer for internal services.

    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        return _prepare_config(self.runtime_config, cluster_config)

    def bootstrap_config(self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Final chance to update the config with runtime specific configurations
        This happens after provider bootstrap_config is done.
        """
        cluster_config = _bootstrap_runtime_config(
            self.runtime_config, cluster_config)
        return cluster_config

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

    @staticmethod
    def get_logs() -> Dict[str, str]:
        return _get_runtime_logs()

    @staticmethod
    def get_processes():
        return _get_runtime_processes()

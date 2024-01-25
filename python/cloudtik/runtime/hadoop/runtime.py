import logging
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HDFS
from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.hadoop.utils import _with_runtime_environment_variables, \
    _validate_config, _prepare_config, _prepare_config_on_head, _node_configure

logger = logging.getLogger(__name__)


class HadoopRuntime(RuntimeBase):
    """Implementation for Hadoop Client Runtime"""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare runtime specific configurations"""
        return _prepare_config(cluster_config)

    def validate_config(self, cluster_config: Dict[str, Any]):
        """Validate cluster configuration from runtime perspective."""
        _validate_config(cluster_config)

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

    @staticmethod
    def get_dependencies():
        return [BUILT_IN_RUNTIME_HDFS]

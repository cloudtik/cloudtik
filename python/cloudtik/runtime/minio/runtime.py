import logging
from typing import Any, Dict, Tuple

from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.minio.utils import _get_runtime_processes, \
    _get_runtime_services, _with_runtime_environment_variables, \
    _get_runtime_logs, _configure, _validate_config

logger = logging.getLogger(__name__)


class MinIORuntime(RuntimeBase):
    """Implementation for MinIO Runtime for S3 compatible object storage.
    MinIO requires using expansion notation {x...y} to denote a sequential
    series of MinIO hosts when creating a server pool. So MinIO will need
    depend on Consul to provide DNS service for cluster node and depend on
    one of the DNS resolvers (dnsmasq, coredns, bind) to be set as the system
    default resolver which forward to Consul DNS for resolving node host names.
    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

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

    def configure(self, head: bool):
        """ This method is called on every node as the first step of executing runtime
        configure command.
        """
        _configure(self.runtime_config, head)

    def get_runtime_services(self, cluster_name: str):
        return _get_runtime_services(self.runtime_config, cluster_name)

    def get_node_constraints(
            self, cluster_config: Dict[str, Any]) -> Tuple[bool, bool, bool]:
        """Whether the runtime nodes need minimal nodes launch before going to setup.
        Usually this is because the setup of the nodes need to know each other.
        """
        return True, False, False

    @staticmethod
    def get_logs() -> Dict[str, str]:
        return _get_runtime_logs()

    @staticmethod
    def get_processes():
        return _get_runtime_processes()

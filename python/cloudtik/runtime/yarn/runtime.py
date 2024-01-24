import logging
from typing import Any, Dict, Optional

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HDFS
from cloudtik.core.job_waiter import JobWaiter
from cloudtik.core.node_provider import NodeProvider
from cloudtik.core.scaling_policy import ScalingPolicy
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.yarn.job_waiter import YARNJobWaiter
from cloudtik.runtime.yarn.utils import _prepare_config, _with_runtime_environment_variables, \
    get_runtime_processes, get_runtime_logs, _get_runtime_endpoints, \
    _get_head_service_ports, _get_scaling_policy, _get_runtime_services

logger = logging.getLogger(__name__)


class YARNRuntime(RuntimeBase):
    """Implementation for YARN Runtime"""

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def prepare_config(
            self, cluster_config: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare runtime specific configurations"""
        return _prepare_config(cluster_config)

    def with_environment_variables(
            self, config: Dict[str, Any], provider: NodeProvider,
            node_id: str) -> Dict[str, Any]:
        return _with_runtime_environment_variables(
            self.runtime_config, config=config,
            provider=provider, node_id=node_id)

    def get_runtime_endpoints(
            self, cluster_config: Dict[str, Any], cluster_head_ip: str):
        return _get_runtime_endpoints(cluster_config, cluster_head_ip)

    def get_head_service_ports(self) -> Optional[Dict[str, Any]]:
        return _get_head_service_ports(self.runtime_config)

    def get_runtime_services(self, cluster_config: Dict[str, Any]):
        return _get_runtime_services(self.runtime_config, cluster_config)

    def get_scaling_policy(
            self, cluster_config: Dict[str, Any], head_host: str
    ) -> Optional[ScalingPolicy]:
        return _get_scaling_policy(
            self.runtime_config, cluster_config, head_host)

    def get_job_waiter(
            self, cluster_config: Dict[str, Any]) -> Optional[JobWaiter]:
        return YARNJobWaiter(cluster_config)

    @staticmethod
    def get_logs() -> Dict[str, str]:
        return get_runtime_logs()

    @staticmethod
    def get_processes():
        return get_runtime_processes()

    @staticmethod
    def get_dependencies():
        return [BUILT_IN_RUNTIME_HDFS]

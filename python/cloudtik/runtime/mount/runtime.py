import logging
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_HADOOP
from cloudtik.core.node_provider import NodeProvider
from cloudtik.runtime.common.runtime_base import RuntimeBase
from cloudtik.runtime.mount.utils import _get_runtime_processes, \
    _with_runtime_environment_variables, _node_configure

logger = logging.getLogger(__name__)


class MountRuntime(RuntimeBase):
    """Implementation for File System Mount Runtime which provides service
    to mount a distributed file system to a local path.
    For discovering the local storage services, it depends on Hadoop (client) runtime
    and its configurations. If we want flexibility, we can decouple this in the future.
    """

    def __init__(self, runtime_config: Dict[str, Any]) -> None:
        super().__init__(runtime_config)

    def with_environment_variables(
            self, config: Dict[str, Any], provider: NodeProvider,
            node_id: str) -> Dict[str, Any]:
        """Export necessary runtime environment variables for running node commands.
        """
        return _with_runtime_environment_variables(
            self.runtime_config, config=config)

    def node_configure(self, head: bool):
        """ This method is called on every node as the first step of executing runtime
        configure command.
        """
        _node_configure(self.runtime_config, head)

    @staticmethod
    def get_processes():
        """Return a list of processes for this runtime."""
        return _get_runtime_processes()

    @staticmethod
    def get_dependencies():
        return [BUILT_IN_RUNTIME_HADOOP]

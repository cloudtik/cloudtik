from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MOUNT, BUILT_IN_RUNTIME_HADOOP
from cloudtik.core._private.util.core_utils import export_environment_variables
from cloudtik.runtime.common.hadoop import with_remote_storage, with_storage_properties
from cloudtik.runtime.common.service_discovery.cluster import has_runtime_in_cluster

RUNTIME_PROCESSES = [
    # The first element is the substring to filter.
    # The second element, if True, is to filter ps results by command name.
    # The third element is the process name.
    # The forth element, if node, the process should on all nodes,if head, the process should on head node.
    ["s3fs", True, "S3 Fuse", "node"],
    ["blobfuse2", True, "Azure Fuse", "node"],
    ["gcsfuse", True, "GCS Fuse", "node"],
    ["ossfs", True, "OSS Fuse", "node"],
    ["fuse_dfs", True, "HDFS Fuse", "node"],
    ["proc_nfs3", False, "HDFS NFS", "node"],
]


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_MOUNT, {})


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _with_hdfs_mount_method(mount_config, runtime_envs):
    mount_method = mount_config.get("hdfs_mount_method")
    if mount_method:
        runtime_envs["HDFS_MOUNT_METHOD"] = mount_method


def _with_runtime_environment_variables(runtime_config, config):
    runtime_envs = {"MOUNT_CLIENT": True}
    mount_config = _get_config(runtime_config)
    _with_hdfs_mount_method(mount_config, runtime_envs)
    return runtime_envs


def _node_configure(runtime_config, head: bool):
    # if Hadoop runtime (client) is installed, we export corresponding environment
    if has_runtime_in_cluster(runtime_config, BUILT_IN_RUNTIME_HADOOP):
        hadoop_config = runtime_config.get(BUILT_IN_RUNTIME_HADOOP, {})

        envs = {}
        envs = with_remote_storage(hadoop_config, envs)
        envs = with_storage_properties(runtime_config, envs)
        export_environment_variables(envs)

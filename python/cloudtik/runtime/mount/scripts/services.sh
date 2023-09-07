#!/bin/bash

# Current bin directory
BIN_DIR=`dirname "$0"`
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
export CLOUD_FS_MOUNT_PATH=/cloudtik/fs
export LOCAL_FS_MOUNT_PATH=/cloudtik/localfs

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# storage mount functions
. "$BIN_DIR"/mount-storage.sh

set_head_option "$@"
set_service_command "$@"
set_head_address

case "$SERVICE_COMMAND" in
start)
    # Mount cloud filesystem or hdfs
    mount_cloud_fs
    ;;
stop)
    # Unmount cloud filesystem or hdfs
    unmount_cloud_fs
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0

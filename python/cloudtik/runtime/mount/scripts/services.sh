#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
MOUNT_HOME=$USER_HOME/runtime/mount

export DEFAULT_FS_MOUNT_PATH=/cloudtik/fs
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
    # Will set necessary environments variables needed for service starting
    . $MOUNT_HOME/conf/mount.conf
    # Mount cloud filesystem or hdfs
    mount_storage_fs
    ;;
stop)
    # Unmount cloud filesystem or hdfs
    unmount_storage_fs
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0

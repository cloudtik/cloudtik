#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

# import util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

if [ ! -n "${TRINO_HOME}" ]; then
    echo "Trino is not installed."
    exit 1
fi

set_head_option "$@"
set_service_command "$@"

case "$SERVICE_COMMAND" in
start)
    $TRINO_HOME/bin/launcher start
    ;;
stop)
    $TRINO_HOME/bin/launcher stop
    ;;
-h|--help)
    echo "Usage: $0 start|stop --head" >&2
    ;;
*)
    echo "Usage: $0 start|stop --head" >&2
    ;;
esac

exit 0

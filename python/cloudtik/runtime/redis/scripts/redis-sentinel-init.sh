#!/usr/bin/env bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

. "$ROOT_DIR"/common/scripts/util-cluster.sh

# Load redis sentinel functions
. "$BIN_DIR"/redis-sentinel.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

_main() {
    # log all commands outputs
    CLOUDTIK_SCRIPT_DEBUG=true
    # check whether this is first time
    local -r sentinel_init_file=${REDIS_SENTINEL_DATA_DIR}/.initialized
    if [ ! -f "${sentinel_init_file}" ]; then
        # initialize sentinel configuration
        redis_sentinel_initialize
        if [ $? -eq 0 ]; then
            mkdir -p "${sentinel_init_file}"
            touch "${sentinel_init_file}"
        fi
    fi
}

if ! _is_sourced; then
	_main "$@"
fi

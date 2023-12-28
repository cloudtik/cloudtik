#!/bin/bash
# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Load redis functions
. "$BIN_DIR"/redis.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

_main() {
    # Init script for Redis Server started.
    redis_configure_default
    if [ "${REDIS_CLUSTER_MODE}" == "replication" ]; then
        redis_configure_replication
    elif [ "${REDIS_CLUSTER_MODE}" == "sharding" ]; then
        redis_configure_sharding
    fi
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
  _main "$@"
fi

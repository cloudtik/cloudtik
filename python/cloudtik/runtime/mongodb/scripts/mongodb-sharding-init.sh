#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Load mongodb-sharding functions
. "$BIN_DIR"/mongodb-sharding.sh

# check to see if this file is being run or sourced from another script
_is_sourced() {
	# https://unix.stackexchange.com/a/215279
	[ "${#FUNCNAME[@]}" -ge 2 ] \
		&& [ "${FUNCNAME[0]}" = '_is_sourced' ] \
		&& [ "${FUNCNAME[1]}" = 'source' ]
}

_main() {
    # Ensure MongoDB env var settings are valid
    mongodb_sharded_validate

    # Ensure MongoDB is stopped when this script ends.
    trap "mongodb_stop_all" EXIT

    # Ensure MongoDB is initialized
    if [[ "$MONGODB_SHARDING_MODE" = "mongos" ]]; then
        mongodb_sharded_mongos_initialize
    elif [[ "$MONGODB_SHARDING_MODE" = "configsvr" ]]; then
        mongodb_sharded_mongod_initialize
        mongodb_sharded_mongos_initialize
    else
        mongodb_sharded_mongos_initialize
        mongodb_start_mongos
        mongodb_sharded_mongod_initialize
    fi

    if [[ -n "$MONGODB_INITSCRIPTS_DIR" ]]; then
        # Allow running custom initialization scripts
        mongodb_custom_init_scripts
    fi
}

# If we are sourced from elsewhere, don't perform any further actions
if ! _is_sourced; then
	_main "$@"
fi

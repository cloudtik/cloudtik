#!/usr/bin/env bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

. "$ROOT_DIR"/common/scripts/util-cluster.sh

# Load repmgr functions
. "$BIN_DIR"/repmgr.sh

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
    if [ "${POSTGRES_ROLE}" == "primary" ]; then
        # check whether this is first time
        POSTGRES_REPMGR_INIT_FILE=${POSTGRES_REPMGR_DATA_DIR}/.initialized
        if [ ! -f "${POSTGRES_REPMGR_INIT_FILE}" ]; then
            repmgr_register_primary
            if [ $? -eq 0 ]; then
                mkdir -p "${POSTGRES_REPMGR_DATA_DIR}"
                touch "${POSTGRES_REPMGR_INIT_FILE}"
            fi
        fi
    else
        repmgr_unregister_standby
        repmgr_register_standby
    fi
}

if ! _is_sourced; then
	_main "$@"
fi

#!/usr/bin/env bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

set -o errexit
set -o nounset
set -o pipefail

# Load pgpool functions
. "$BIN_DIR"/pgpool.sh

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
PGPOOL_HOME=$RUNTIME_PATH/pgpool
. ${PGPOOL_HOME}/conf/pgpool

# TODO: Needs the following variables be set in pgpool
# PGPOOL_PORT, PGPOOL_PCP_PORT
# PGPOOL_ADMIN_USER, PGPOOL_ADMIN_PASSWORD
# PGPOOL_POSTGRES_USER, PGPOOL_POSTGRES_PASSWORD
pgpool_healthcheck

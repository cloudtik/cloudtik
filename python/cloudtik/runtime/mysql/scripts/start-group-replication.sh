#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

# schema initialization functions
. "$BIN_DIR"/mysql-init.sh

# TODO: For group replication, a few cases needs to be considered:
# 1. For the first time creating, the head node needs to execute start group replication
# with bootstrap.
# 2. For a full restart of the cluster, currently we can only handle the case that the head
# node is with the most up to date data. In this case, we do the same as the first time
# creating.
# 3. For head node failure and restart, other members are still in running. The head node
# don't need bootstrap. Instead it just join with running workers as seeds

# Instead of using group_replication_bootstrap_group=ON, it is safer to manually start
# group replication and turn on group_replication_bootstrap_group ON and OFF in commands.
start_group_replication_with_bootstrap() {
    mysql_check_connection "$@"
    if [ $? != 0 ]; then
        echo "Error: timeout waiting for service ready."
    else
        mysql_bootstrap_group_replication "$@"
    fi
}

start_group_replication_with_bootstrap "$@"

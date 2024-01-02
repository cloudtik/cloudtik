#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# schema initialization functions
. "$BIN_DIR"/mysql.sh

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
bootstrap_group_replication() {
    mysql_check_connection "$@"
    if [ $? != 0 ]; then
        echo "Error: timeout waiting for service ready."
    else
        # Case 1 and Case 2. start group replication with bootstrap
        running_worker_hosts=$(cloudtik head worker-hosts --runtime=mysql --node-status=up-to-date)
        if [ -z "$running_worker_hosts" ]; then
            # Case 1 or Case 2
            echo "Bootstrapping group replication"
            mysql_bootstrap_group_replication "$@"
        else
            # Case 3
            local group_seeds=""
            for worker_host in ${running_worker_hosts}; do
                local group_seed="${worker_host}:${MYSQL_GROUP_REPLICATION_PORT}"
                if [ -z "${group_seeds}" ]; then
                    group_seeds="${group_seed}"
                else
                    group_seeds="${group_seeds},${group_seed}"
                fi
            done

            echo "Starting group replication seeding by workers: ${group_seeds}"
            update_in_file "${MYSQL_CONF_FILE}" \
              "{%group.replication.group.seeds%}" "${group_seeds}"
            mysql_start_group_replication "$@"
        fi
    fi
}

bootstrap_group_replication "$@"

#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/yarn/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_hadoop_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed."
        exit 1
    fi
}

set_resources_for_yarn() {
    # For nodemanager
    memory_ratio=0.8
    if [ ! -z "${YARN_RESOURCE_MEMORY_RATIO}" ]; then
        memory_ratio=${YARN_RESOURCE_MEMORY_RATIO}
    fi
    total_memory=$(awk -v ratio=${memory_ratio} -v total_physical_memory=$(cloudtik node resources --memory --in-mb) 'BEGIN{print ratio * total_physical_memory}')
    total_memory=${total_memory%.*}
    total_vcores=$(cloudtik node resources --cpu)

    # For Head Node
    if [ "$IS_HEAD_NODE" == "true" ]; then
        local -r bootstrap_config="~/cloudtik_bootstrap_config.yaml"
        yarn_container_maximum_vcores=$(cat "$bootstrap_config" | jq '."runtime"."yarn"."yarn_container_resource"."yarn_container_maximum_vcores"')
        yarn_container_maximum_memory=$(cat "$bootstrap_config" | jq '."runtime"."yarn"."yarn_container_resource"."yarn_container_maximum_memory"')
    fi
}

update_yarn_config() {
    yarn_scheduler_class="org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler"
    if [ "${YARN_SCHEDULER}" == "fair" ];then
        yarn_scheduler_class="org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler"
    fi
    sed -i "s/{%yarn.resourcemanager.scheduler.class%}/${yarn_scheduler_class}/g" $yarn_config_file
    if [ "$IS_HEAD_NODE" == "true" ];then
        sed -i "s/{%yarn.scheduler.maximum-allocation-mb%}/${yarn_container_maximum_memory}/g" $yarn_config_file
        sed -i "s/{%yarn.scheduler.maximum-allocation-vcores%}/${yarn_container_maximum_vcores}/g" $yarn_config_file
        sed -i "s/{%yarn.nodemanager.resource.memory-mb%}/${yarn_container_maximum_memory}/g" $yarn_config_file
        sed -i "s/{%yarn.nodemanager.resource.cpu-vcores%}/${yarn_container_maximum_vcores}/g" $yarn_config_file
    else
        sed -i "s/{%yarn.scheduler.maximum-allocation-mb%}/${total_memory}/g" $yarn_config_file
        sed -i "s/{%yarn.scheduler.maximum-allocation-vcores%}/${total_vcores}/g" $yarn_config_file
        sed -i "s/{%yarn.nodemanager.resource.memory-mb%}/${total_memory}/g" $yarn_config_file
        sed -i "s/{%yarn.nodemanager.resource.cpu-vcores%}/${total_vcores}/g" $yarn_config_file
    fi
}

update_data_disks_config() {
    # set nodemanager.local-dirs
    nodemanager_local_dirs=$(get_data_disk_dirs)
    if [ -z "$nodemanager_local_dirs" ]; then
        nodemanager_local_dirs="${HADOOP_HOME}/data/nodemanager/local-dir"
    fi
    sed -i "s!{%yarn.nodemanager.local-dirs%}!${nodemanager_local_dirs}!g" $yarn_config_file
}

configure_yarn() {
    prepare_base_conf
    mkdir -p ${HADOOP_HOME}/logs
    yarn_config_file=${OUTPUT_DIR}/hadoop/yarn-site.xml

    sed -i "s/{%resourcemanager.host%}/${HEAD_HOST_ADDRESS}/g" $yarn_config_file

    set_resources_for_yarn
    update_yarn_config
    update_data_disks_config

    cp -r $yarn_config_file ${HADOOP_HOME}/etc/hadoop/
}

set_head_option "$@"
check_hadoop_installed
set_head_address
configure_yarn

exit 0

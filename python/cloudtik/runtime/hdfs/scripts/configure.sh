#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Hadoop common functions
. "$ROOT_DIR"/common/scripts/hadoop.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/hdfs/conf
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

get_first_dfs_dir() {
    local data_disk_dir=$(get_first_data_disk_dir)
    local data_dir
    if [ -z "$data_disk_dir" ]; then
        data_dir="${HADOOP_HOME}/data/dfs"
    else
        data_dir="$data_disk_dir/dfs"
    fi
    echo "${data_dir}"
}

update_hdfs_data_disks_config() {
    local hdfs_nn_dirs=""
    local hdfs_dn_dirs=""
    if [ -d "/mnt/cloudtik" ]; then
        for data_disk in /mnt/cloudtik/*; do
            [ -d "$data_disk" ] || continue
            local data_dir="$data_disk/dfs"
            if [ "$HDFS_FORCE_CLEAN" == "true" ]; then
                sudo rm -rf "$data_dir"
            fi
            if [ -z "$hdfs_nn_dirs" ]; then
                hdfs_nn_dirs="$data_dir/nn"
            else
                hdfs_nn_dirs="$hdfs_nn_dirs,$data_dir/nn"
            fi
            if [ -z "$hdfs_dn_dirs" ]; then
                hdfs_dn_dirs="$data_dir/dn"
            else
                hdfs_dn_dirs="$hdfs_dn_dirs,$data_dir/dn"
            fi
        done
    fi

    # if no disks mounted
    if [ -z "$hdfs_nn_dirs" ]; then
        if [ "$HDFS_FORCE_CLEAN" == "true" ]; then
            sudo rm -rf "${HADOOP_HOME}/data/dfs"
        fi
        hdfs_nn_dirs="${HADOOP_HOME}/data/dfs/nn"
        hdfs_dn_dirs="${HADOOP_HOME}/data/dfs/dn"
    fi

    sed -i "s!{%dfs.namenode.name.dir%}!${hdfs_nn_dirs}!g" ${HDFS_SITE_CONFIG}
    sed -i "s!{%dfs.datanode.data.dir%}!${hdfs_dn_dirs}!g" ${HDFS_SITE_CONFIG}
}

update_proxy_user_for_current_user() {
    CURRENT_SYSTEM_USER=$(whoami)

    if [ "${CURRENT_SYSTEM_USER}" != "root" ]; then
        HADOOP_PROXY_USER_PROPERTIES="\
    <property>\n\
        <name>hadoop.proxyuser.${CURRENT_SYSTEM_USER}.groups</name>\n\
        <value>*</value>\n\
    </property>\n\
    <property>\n\
        <name>hadoop.proxyuser.${CURRENT_SYSTEM_USER}.hosts</name>\n\
        <value>*</value>\n\
    </property>"
        sed -i "s#{%hadoop.proxyuser.properties%}#${HADOOP_PROXY_USER_PROPERTIES}#g" ${CORE_SITE_CONFIG}
    else
        sed -i "s#{%hadoop.proxyuser.properties%}#""#g" ${CORE_SITE_CONFIG}
    fi
}

update_nfs_dump_dir() {
    # set nfs gateway dump dir
    local data_disk_dir=$(get_first_data_disk_dir)
    local nfs_dump_dir
    if [ -z "$data_disk_dir" ]; then
        nfs_dump_dir="/tmp/.hdfs-nfs"
    else
        nfs_dump_dir="$data_disk_dir/tmp/.hdfs-nfs"
    fi
    sed -i "s!{%dfs.nfs3.dump.dir%}!${nfs_dump_dir}!g" ${HDFS_SITE_CONFIG}
}

update_journal_data_disks_config() {
    local dfs_dir=$(get_first_dfs_dir)
    local journal_data_dir="${dfs_dir}/journal"
    if [ "$HDFS_FORCE_CLEAN" == "true" ]; then
        sudo rm -rf "$journal_data_dir"
    fi
    mkdir -p "$journal_data_dir"
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.journalnode.edits.dir%}" "${journal_data_dir}"
}

finalize_hdfs_config() {
    # override hdfs conf
    cp ${CORE_SITE_CONFIG} ${HDFS_CONF_DIR}/core-site.xml
    cp ${HDFS_SITE_CONFIG} ${HDFS_CONF_DIR}/hdfs-site.xml

    # configure the default hadoop to HDFS, if there are other components configure
    # Hadoop, it will override this file
    cp ${CORE_SITE_CONFIG} ${HADOOP_HOME}/etc/hadoop/core-site.xml
    cp ${HDFS_SITE_CONFIG} ${HADOOP_HOME}/etc/hadoop/hdfs-site.xml
}

update_name_services() {
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.name.service%}" "${HDFS_NAME_SERVICE}"
    local hdfs_name_nodes="$(get_hdfs_name_nodes)"
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.ha.name.nodes%}" "${hdfs_name_nodes}"

    local hdfs_name_addresses="$(get_hdfs_name_addresses)"
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.ha.name.addresses%}" "${hdfs_name_addresses}"
}

configure_simple_hdfs() {
    local fs_default_dir="hdfs://${HEAD_HOST_ADDRESS}:${HDFS_SERVICE_PORT}"
    update_in_file ${CORE_SITE_CONFIG} \
      "{%fs.default.name%}" "${fs_default_dir}"

    update_proxy_user_for_current_user
    update_hdfs_data_disks_config
    update_nfs_dump_dir

    finalize_hdfs_config
}

configure_name_cluster() {
    HDFS_SITE_CONFIG=${OUTPUT_DIR}/hadoop/hdfs-site-name.xml

    local fs_default_dir="hdfs://${HDFS_NAME_SERVICE}"
    update_in_file ${CORE_SITE_CONFIG} \
      "{%fs.default.name%}" "${fs_default_dir}"
    update_name_services

    update_proxy_user_for_current_user
    update_hdfs_data_disks_config
    update_nfs_dump_dir

    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.namenode.journal.nodes%}" "${HDFS_JOURNAL_NODES}"

    local hdfs_auto_failover="false"
    local hdfs_zookeeper_quorum=""
    if [ "${HDFS_AUTO_FAILOVER}" == "true" ]; then
        hdfs_zookeeper_quorum="${HDFS_ZOOKEEPER_QUORUM}"
        hdfs_auto_failover="true"
    fi
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.ha.auto.failover%}" "${hdfs_auto_failover}"
    update_in_file "${HDFS_SITE_CONFIG}" \
      "{%dfs.ha.zookeeper.quorum%}" "${hdfs_zookeeper_quorum}"

    finalize_hdfs_config
}

configure_journal_cluster() {
    HDFS_SITE_CONFIG=${OUTPUT_DIR}/hadoop/hdfs-site-journal.xml
    update_journal_data_disks_config
    # override hdfs conf
    cp ${HDFS_SITE_CONFIG} ${HDFS_CONF_DIR}/hdfs-site.xml
}

configure_data_cluster() {
    HDFS_SITE_CONFIG=${OUTPUT_DIR}/hadoop/hdfs-site-data.xml

    local fs_default_dir="hdfs://${HDFS_NAME_SERVICE}"
    update_in_file ${CORE_SITE_CONFIG} \
      "{%fs.default.name%}" "${fs_default_dir}"
    update_name_services

    update_proxy_user_for_current_user
    update_hdfs_data_disks_config
    update_nfs_dump_dir

    finalize_hdfs_config
}

configure_ha_cluster() {
    if [ "${HDFS_CLUSTER_ROLE}" == "name" ]; then
        configure_name_cluster
    elif [ "${HDFS_CLUSTER_ROLE}" == "journal" ]; then
        configure_journal_cluster
    else
        configure_data_cluster
    fi
}

configure_variable() {
    set_variable_in_file "${HDFS_CONF_DIR}/hdfs" "$@"
}

configure_service_init() {
    # The following environment variables are needed for hdfs-init.sh
    echo "# HDFS init variables" > ${HDFS_CONF_DIR}/hdfs
    configure_variable HDFS_PORT "${HDFS_SERVICE_PORT}"
    configure_variable HDFS_HTTP_PORT "${HDFS_HTTP_PORT}"
    configure_variable HDFS_HEAD_NODE ${IS_HEAD_NODE}
    configure_variable HDFS_NODE_IP "${NODE_IP_ADDRESS}"
    configure_variable HDFS_CLUSTER_MODE "${HDFS_CLUSTER_MODE}"

    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        if [ "${HDFS_CLUSTER_ROLE}" == "name" ]; then
            configure_variable HDFS_NAME_SERVICE_ID "nn${CLOUDTIK_NODE_SEQ_ID}"
        fi
    fi
}

configure_hdfs() {
    prepare_base_conf
    mkdir -p ${HADOOP_HOME}/logs

    HDFS_CONF_DIR=${HADOOP_HOME}/etc/hdfs
    # copy the existing hadoop conf
    mkdir -p ${HDFS_CONF_DIR}
    cp -r ${HADOOP_HOME}/etc/hadoop/* ${HDFS_CONF_DIR}/

    CORE_SITE_CONFIG=${OUTPUT_DIR}/hadoop/core-site.xml
    HDFS_SITE_CONFIG=${OUTPUT_DIR}/hadoop/hdfs-site.xml

    if [ "${HDFS_CLUSTER_MODE}" == "ha_cluster" ]; then
        configure_ha_cluster
    else
        configure_simple_hdfs
    fi

    # Set variables for export for hdfs-init.sh
    configure_service_init
}

set_head_option "$@"
check_hadoop_installed
set_head_address
set_node_address
configure_hdfs

exit 0

#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
DNSMASQ_HOME=$RUNTIME_PATH/dnsmasq

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/dnsmasq/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_dnsmasq_installed() {
    if ! command -v dnsmasq &> /dev/null
    then
        echo "Dnsmasq is not installed."
        exit 1
    fi
}

configure_dnsmasq() {
    prepare_base_conf

    ETC_DEFAULT=/etc/default
    sudo mkdir -p ${ETC_DEFAULT}

    sed -i "s#{%dnsmasq.home%}#${DNSMASQ_HOME}#g" ${OUTPUT_DIR}/dnsmasq
    sudo cp ${OUTPUT_DIR}/dnsmasq ${ETC_DEFAULT}/dnsmasq

    DNSMASQ_CONF_DIR=${DNSMASQ_HOME}/conf
    DNSMASQ_CONF_INCLUDE_DIR=${DNSMASQ_CONF_DIR}/conf.d
    mkdir -p ${DNSMASQ_CONF_INCLUDE_DIR}

    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/dnsmasq.conf
    sed -i "s#{%listen.address%}#${NODE_IP_ADDRESS}#g" ${CONFIG_TEMPLATE_FILE}
    sed -i "s#{%listen.port%}#${DNSMASQ_SERVICE_PORT}#g" ${CONFIG_TEMPLATE_FILE}

    # dnsmasq will use /etc/resolv.conf for upstream.
    # TODO: if we want to use this DNS server as the system default, we need:
    # 1. copy the /etc/resolv.conf to a backup file if backup file not exists
    # 2. direct dnsmasq to use the backup copy as upstream
    # 3. modify /etc/resolve.conf to use dnsmasq as resolver

    SYSTEM_RESOLV_CONF="/etc/resolv.conf"
    ORIGIN_RESOLV_CONF="${DNSMASQ_HOME}/conf/resolv.conf"

    # backup the system resolv conf only once
    if [ ! -f "${ORIGIN_RESOLV_CONF}" ]; then
        cp ${SYSTEM_RESOLV_CONF} ${ORIGIN_RESOLV_CONF}
    fi

    if [ "${DNSMASQ_DEFAULT_RESOLVER}" == "true" ]; then
        UPSTREAM_RESOLV_CONF=${ORIGIN_RESOLV_CONF}
    else
        UPSTREAM_RESOLV_CONF=${SYSTEM_RESOLV_CONF}
    fi

    sed -i "s#{%upstream.resolv.conf%}#${UPSTREAM_RESOLV_CONF}#g" \
      ${CONFIG_TEMPLATE_FILE}

    cp ${CONFIG_TEMPLATE_FILE} ${DNSMASQ_CONF_INCLUDE_DIR}/dnsmasq.conf

    # generate additional name server records for specific (service discovery) domain
    if [ "${DNSMASQ_CONSUL_RESOLVE}" == "true" ]; then
        # TODO: handle consul port other than default
        cp ${OUTPUT_DIR}/consul ${DNSMASQ_CONF_INCLUDE_DIR}/consul
    fi
}

set_head_option "$@"
check_dnsmasq_installed
set_node_address
configure_dnsmasq

exit 0

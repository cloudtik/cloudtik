#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
COREDNS_HOME=$RUNTIME_PATH/coredns

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/coredns/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_coredns_installed() {
    if [ ! -f "${COREDNS_HOME}/coredns" ]; then
        echo "CoreDNS is not installed."
        exit 1
    fi
}

configure_coredns() {
    prepare_base_conf
    mkdir -p ${COREDNS_HOME}/logs

    COREDNS_CONF_DIR=${COREDNS_HOME}/conf
    mkdir -p ${COREDNS_CONF_DIR}

    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/Corefile

    sed -i "s#{%bind.ip%}#${NODE_IP_ADDRESS}#g" \
      `grep "{%bind.ip%}" -rl ${OUTPUT_DIR}`
    sed -i "s#{%bind.port%}#${COREDNS_SERVICE_PORT}#g" \
      `grep "{%bind.port%}" -rl ${OUTPUT_DIR}`

    # generate additional name server records for specific (service discovery) domain
    if [ "${COREDNS_CONSUL_RESOLVE}" == "true" ]; then
        # TODO: handle consul port other than default
        echo "import ${COREDNS_CONF_DIR}/Corefile.consul" >> ${CONFIG_TEMPLATE_FILE}
        cp ${OUTPUT_DIR}/Corefile.consul ${COREDNS_CONF_DIR}/Corefile.consul
    fi

    SYSTEM_RESOLV_CONF="/etc/resolv.conf"
    ORIGIN_RESOLV_CONF="${COREDNS_HOME}/conf/resolv.conf"

    # backup the system resolv conf only once
    if [ ! -f "${ORIGIN_RESOLV_CONF}" ]; then
        cp ${SYSTEM_RESOLV_CONF} ${ORIGIN_RESOLV_CONF}
    fi

    if [ "${COREDNS_DEFAULT_RESOLVER}" == "true" ]; then
        UPSTREAM_RESOLV_CONF=${ORIGIN_RESOLV_CONF}
    else
        UPSTREAM_RESOLV_CONF=${SYSTEM_RESOLV_CONF}
    fi

    sed -i "s#{%upstream.resolv.conf%}#${UPSTREAM_RESOLV_CONF}#g" \
      ${OUTPUT_DIR}/Corefile.upstream

    echo "import ${COREDNS_CONF_DIR}/Corefile.upstream" >> ${CONFIG_TEMPLATE_FILE}
    cp ${OUTPUT_DIR}/Corefile.upstream ${COREDNS_CONF_DIR}/Corefile.upstream

    cp ${CONFIG_TEMPLATE_FILE} ${COREDNS_CONF_DIR}/Corefile
}

set_head_option "$@"
check_coredns_installed
set_node_address
configure_coredns

exit 0

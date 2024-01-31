#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
BIND_HOME=$RUNTIME_PATH/bind

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/bind/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_bind_installed() {
    if ! command -v named &> /dev/null
    then
        echo "Bind is not installed."
        exit 1
    fi
}

configure_bind() {
    prepare_base_conf

    BIND_LOGS_DIR=${BIND_HOME}/logs
    mkdir -p ${BIND_LOGS_DIR}
    sudo chown root:bind ${BIND_LOGS_DIR} >/dev/null 2>&1
    sudo chmod -R 775 ${BIND_LOGS_DIR}

    ETC_DEFAULT=/etc/default
    sudo mkdir -p ${ETC_DEFAULT}

    sed -i "s#{%bind.home%}#${BIND_HOME}#g" ${OUTPUT_DIR}/named
    sudo cp ${OUTPUT_DIR}/named ${ETC_DEFAULT}/named

    BIND_CONF_DIR=${BIND_HOME}/conf
    mkdir -p ${BIND_CONF_DIR}

    CONFIG_TEMPLATE_FILE=${OUTPUT_DIR}/named.conf

    sed -i "s#{%bind.home%}#${BIND_HOME}#g" \
      ${CONFIG_TEMPLATE_FILE} ${OUTPUT_DIR}/named.conf.logging

    sed -i "s#{%listen.address%}#${NODE_IP_ADDRESS}#g" ${OUTPUT_DIR}/named.conf.options
    sed -i "s#{%listen.port%}#${BIND_SERVICE_PORT}#g" ${OUTPUT_DIR}/named.conf.options

    if [ -z "${BIND_DNSSEC_VALIDATION}" ]; then
        DNSSEC_VALIDATION="yes"
    else
        DNSSEC_VALIDATION="${BIND_DNSSEC_VALIDATION}"
    fi
    sed -i "s#{%dnssec.validation%}#${DNSSEC_VALIDATION}#g" ${OUTPUT_DIR}/named.conf.options

    # generate additional name server records for specific (service discovery) domain
    if [ "${BIND_CONSUL_RESOLVE}" == "true" ]; then
        # TODO: handle consul port other than default
        cp ${OUTPUT_DIR}/named.conf.consul ${BIND_CONF_DIR}/named.conf.consul
        echo "include \"${BIND_HOME}/conf/named.conf.consul\";" >> ${CONFIG_TEMPLATE_FILE}
    fi

    SYSTEM_RESOLV_CONF="/etc/resolv.conf"
    ORIGIN_RESOLV_CONF="${BIND_HOME}/conf/resolv.conf"

    # backup the system resolv conf only once
    if [ ! -f "${ORIGIN_RESOLV_CONF}" ]; then
        cp ${SYSTEM_RESOLV_CONF} ${ORIGIN_RESOLV_CONF}
    fi

    # python configure script will write named.conf.upstream
    echo "include \"${BIND_HOME}/conf/named.conf.upstream\";" >> ${CONFIG_TEMPLATE_FILE}

    cp ${OUTPUT_DIR}/named.conf.options ${BIND_CONF_DIR}/named.conf.options
    cp ${OUTPUT_DIR}/named.conf.logging ${BIND_CONF_DIR}/named.conf.logging
    cp ${CONFIG_TEMPLATE_FILE} ${BIND_CONF_DIR}/named.conf
}

set_head_option "$@"
check_bind_installed
set_node_address
configure_bind

exit 0

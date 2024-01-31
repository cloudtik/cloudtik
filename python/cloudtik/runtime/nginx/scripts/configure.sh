#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime
NGINX_HOME=$RUNTIME_PATH/nginx

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/nginx/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_nginx_installed() {
    if ! command -v nginx &> /dev/null
    then
        echo "NGINX is not installed."
        exit 1
    fi
}

configure_web() {
    cat ${OUTPUT_DIR}/nginx-web-base.conf >> ${NGINX_CONFIG_FILE}
    mkdir -p ${NGINX_CONFIG_DIR}/web
    cp ${OUTPUT_DIR}/nginx-web.conf ${NGINX_CONFIG_DIR}/web/web.conf
}

configure_dns_backend() {
    # configure a load balancer based on Consul DNS interface
    local template_file=${OUTPUT_DIR}/nginx-load-balancer-dns.conf

    # Consul DNS interface based service discovery
    sed -i "s#{%backend.service.dns.name%}#${NGINX_BACKEND_SERVICE_DNS_NAME}#g" ${template_file}
    sed -i "s#{%backend.service.port%}#${NGINX_BACKEND_SERVICE_PORT}#g" ${template_file}

    cat ${template_file} >> ${NGINX_CONFIG_FILE}
}

configure_static_backend() {
    # python configure script will write upstream block
    cat ${OUTPUT_DIR}/nginx-load-balancer-static.conf >> ${NGINX_CONFIG_FILE}
    mkdir -p ${NGINX_CONFIG_DIR}/upstreams
}

configure_dynamic_backend() {
    # python discovery script will write upstream block and do reload if needed
    cat ${OUTPUT_DIR}/nginx-load-balancer-dynamic.conf >> ${NGINX_CONFIG_FILE}
    mkdir -p ${NGINX_CONFIG_DIR}/upstreams
    mkdir -p ${NGINX_CONFIG_DIR}/routers
}

configure_load_balancer() {
    if [ "${NGINX_CONFIG_MODE}" == "dns" ]; then
        configure_dns_backend
    elif [ "${NGINX_CONFIG_MODE}" == "static" ]; then
        configure_static_backend
    elif [ "${NGINX_CONFIG_MODE}" == "dynamic" ]; then
        configure_dynamic_backend
    else
        echo "WARNING: Unsupported configure mode for load balancer: ${NGINX_CONFIG_MODE}"
    fi
}

configure_api_gateway_dns() {
    # discovery services will discovery the api backends
    :
}

configure_api_gateway_dynamic() {
    # discovery services will discovery the api backends and upstream servers
    mkdir -p ${NGINX_CONFIG_DIR}/upstreams
}

configure_api_gateway() {
    cat ${OUTPUT_DIR}/nginx-api-gateway-base.conf >> ${NGINX_CONFIG_FILE}
    cp ${OUTPUT_DIR}/nginx-api-gateway.conf ${NGINX_CONFIG_DIR}/api-gateway.conf
    cp ${OUTPUT_DIR}/nginx-api-gateway-json-errors.conf ${NGINX_CONFIG_DIR}/api-gateway-json-errors.conf
    mkdir -p ${NGINX_CONFIG_DIR}/routers

    if [ "${NGINX_CONFIG_MODE}" == "dns" ]; then
        configure_api_gateway_dns
    elif [ "${NGINX_CONFIG_MODE}" == "dynamic" ]; then
        configure_api_gateway_dynamic
    else
        echo "WARNING: Unsupported configure mode for API gateway: ${NGINX_CONFIG_MODE}"
    fi
}

configure_nginx() {
    prepare_base_conf
    NGINX_CONFIG_FILE=${OUTPUT_DIR}/nginx.conf

    mkdir -p ${NGINX_HOME}/logs

    ETC_DEFAULT=/etc/default
    sudo mkdir -p ${ETC_DEFAULT}

    sed -i "s#{%nginx.home%}#${NGINX_HOME}#g" `grep "{%nginx.home%}" -rl ${OUTPUT_DIR}`
    sudo cp ${OUTPUT_DIR}/nginx ${ETC_DEFAULT}/nginx

    NGINX_CONFIG_DIR=${NGINX_HOME}/conf
    mkdir -p ${NGINX_CONFIG_DIR}

    sed -i "s#{%server.listen.ip%}#${NODE_IP_ADDRESS}#g" `grep "{%server.listen.ip%}" -rl ${OUTPUT_DIR}`
    sed -i "s#{%server.listen.port%}#${NGINX_LISTEN_PORT}#g" `grep "{%server.listen.port%}" -rl ${OUTPUT_DIR}`

    if [ "${NGINX_APP_MODE}" == "web" ]; then
        configure_web
    elif [ "${NGINX_APP_MODE}" == "load-balancer" ]; then
        configure_load_balancer
    elif [ "${NGINX_APP_MODE}" == "api-gateway" ]; then
        configure_api_gateway
    else
        echo "WARNING: Unknown application mode: ${NGINX_APP_MODE}"
    fi

    cp ${NGINX_CONFIG_FILE} ${NGINX_CONFIG_DIR}/nginx.conf
}

set_head_option "$@"
check_nginx_installed
set_head_address
set_node_address
configure_nginx

exit 0

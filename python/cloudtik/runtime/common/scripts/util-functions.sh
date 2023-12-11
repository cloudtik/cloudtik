#!/bin/bash

COMMON_SCRIPTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

. ${COMMON_SCRIPTS_DIR}/util-os.sh
. ${COMMON_SCRIPTS_DIR}/util-file.sh
. ${COMMON_SCRIPTS_DIR}/util-service.sh
. ${COMMON_SCRIPTS_DIR}/util-cluster.sh

# global variables
CLOUDTIK_DOWNLOADS="https://d30257nes7d4fq.cloudfront.net/downloads"

clean_install_cache() {
    (sudo rm -rf /var/lib/apt/lists/* \
        && sudo apt-get clean \
        && which conda > /dev/null && conda clean -itqy)
}

clean_apt() {
    sudo rm -rf /var/lib/apt/lists/* && sudo apt-get clean
}

update_resolv_conf() {
    local BACKUP_RESOLV_CONF=$1
    cp /etc/resolv.conf ${BACKUP_RESOLV_CONF}
    shift
    SCRIPTS_DIR=$(dirname ${BASH_SOURCE[0]})
    sudo env PATH=$PATH python ${SCRIPTS_DIR}/resolv-conf.py "$@"
}

restore_resolv_conf() {
    local BACKUP_RESOLV_CONF=$1
    if [ -f "${BACKUP_RESOLV_CONF}" ]; then
        sudo cp ${BACKUP_RESOLV_CONF} /etc/resolv.conf
    fi
}

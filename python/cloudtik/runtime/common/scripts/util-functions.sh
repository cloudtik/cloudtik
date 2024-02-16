#!/bin/bash

COMMON_SCRIPTS_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)

. ${COMMON_SCRIPTS_DIR}/util-os.sh
. ${COMMON_SCRIPTS_DIR}/util-file.sh
. ${COMMON_SCRIPTS_DIR}/util-service.sh
. ${COMMON_SCRIPTS_DIR}/util-net.sh
. ${COMMON_SCRIPTS_DIR}/util-cluster.sh

# global variables
CLOUDTIK_DOWNLOADS="https://d30257nes7d4fq.cloudfront.net/downloads"

clean_install() {
    (sudo rm -rf /var/lib/apt/lists/* \
        && sudo apt-get clean \
        && which conda > /dev/null \
        && conda clean -itqy)
}

clean_apt() {
    sudo rm -rf /var/lib/apt/lists/* \
      && sudo apt-get clean
}

update_resolv_conf() {
    local -r backup_resolv_conf="${1:?backup resolv conf file is missing}"
    cp /etc/resolv.conf ${backup_resolv_conf}
    shift
    local script_dir=$(dirname ${BASH_SOURCE[0]})
    sudo env PATH=$PATH python ${script_dir}/resolv-conf.py "$@"
}

restore_resolv_conf() {
    local -r backup_resolv_conf="${1:?backup resolv conf file is missing}"
    if [ -f "${backup_resolv_conf}" ]; then
        sudo cp ${backup_resolv_conf} /etc/resolv.conf
    fi
}

update_systemd_resolved() {
    local -r resolved_conf_name="${1:?resolved conf name is missing}"
    local -r dns_address="${2:?DNS address is missing}"
    local -r resolved_conf_dir="/etc/systemd/resolved.conf.d"
    local -r resolved_conf="${resolved_conf_dir}/${resolved_conf_name}.conf"
    # write the following contents to our resolved conf
    sudo mkdir -p "${resolved_conf_dir}"
    # [Resolve]
    # DNS=dns_address
    # DNSSEC=false
    # Domains=~cloudtik
    printf '[Resolve]\nDNS=%s\nDNSSEC=false\nDomains=~cloudtik\n' \
      "${dns_address}" | sudo tee "${resolved_conf}" >/dev/null
    if [ -f "${resolved_conf}" ]; then
        sudo systemctl restart systemd-resolved
    fi
}

restore_systemd_resolved() {
    local -r resolved_conf_name="${1:?resolved conf name is missing}"
    local -r resolved_conf_dir="/etc/systemd/resolved.conf.d"
    local -r resolved_conf="${resolved_conf_dir}/${resolved_conf_name}.conf"
    if [ -f "${resolved_conf}" ]; then
        sudo rm -f "${resolved_conf}"
        sudo systemctl restart systemd-resolved
    fi
}

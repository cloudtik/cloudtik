#!/usr/bin/env bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CLOUDTIK_HOME="$(dirname "$(dirname "$SCRIPT_DIR")")"

USER_HOME=/home/$(whoami)
LAB_HOME=${USER_HOME}/lab
LAB_CONFIG=${LAB_HOME}/config

# Import the default vars
. ${CLOUDTIK_HOME}/dev/set-default-vars.sh

mkdir -p ${LAB_HOME}/share
mkdir -p ${LAB_HOME}/disks
mkdir -p ${LAB_CONFIG}

cp -r ${SCRIPT_DIR}/config/* ${LAB_CONFIG}/
CUR_USER=$(whoami)
sed -i "s/{%user%}/${CUR_USER}/g" `grep "{%user%}" -rl ${LAB_CONFIG}`

bash ${SCRIPT_DIR}/lab-update.sh

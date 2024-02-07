#!/usr/bin/env bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CLOUDTIK_HOME="$(dirname "$(dirname "$SCRIPT_DIR")")"

USER_HOME=/home/$(whoami)
LAB_HOME=${USER_HOME}/lab
LAB_CONFIG=${LAB_HOME}/config

# Import the default vars
. "${CLOUDTIK_HOME}"/dev/set-default-vars.sh

mkdir -p ${LAB_HOME}/share

cd ${CLOUDTIK_HOME}
git pull
bash ./dev/nightly-build.sh --no-pull
bash ./dev/install-cloudtik.sh --local --nightly --reinstall

PYTHON_TAG=${PYTHON_VERSION//./}
CLOUDTIK_VERSION=$(sed -n 's/__version__ = \"\(..*\)\"/\1/p' ${CLOUDTIK_HOME}/python/cloudtik/__init__.py)
CLOUDTIK_PACKAGE_PREFIX="cloudtik-${CLOUDTIK_VERSION}-cp${PYTHON_TAG}-cp${PYTHON_TAG}-"
CLOUDTIK_PACKAGE="${CLOUDTIK_PACKAGE_PREFIX}manylinux2014_x86_64.nightly.whl"
cp python/dist/cloudtik-1.5.0-cp38-cp38-manylinux2014_x86_64.nightly.whl ${LAB_HOME}/share
sed -i "s/cloudtik-.*-cp.*-cp.*-/${CLOUDTIK_PACKAGE_PREFIX}/g" `grep "cloudtik-.*-cp.*-cp.*-" -rl ${LAB_CONFIG}`
echo "Lab updated to use ${CLOUDTIK_PACKAGE}"

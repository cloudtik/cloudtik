#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

echo "Installing dev tools..."
bash ${SCRIPT_DIR}/install-dev.sh

echo "Installing docker..."
bash ${SCRIPT_DIR}/install-docker.sh
sudo service docker start

echo "Building CloudTik wheel..."
bash ${SCRIPT_DIR}/nightly-build.sh --build-redis

echo "Installing CloudTik wheel..."
bash ${SCRIPT_DIR}/install-cloudtik.sh --local --nightly

echo "Done"

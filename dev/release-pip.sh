#!/usr/bin/env bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CLOUDTIK_HOME=$( cd -- "$( dirname -- "${SCRIPT_DIR}" )" &> /dev/null && pwd )

# Import the default vars
. "$SCRIPT_DIR"/set-default-vars.sh

CLOUDTIK_BRANCH="main"

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    --branch)
        # Override for the branch.
        shift
        CLOUDTIK_BRANCH=$1
        ;;
    *)
        echo "Usage: release-pip.sh"
        exit 1
    esac
    shift
done

PYTHON_TAG=${PYTHON_VERSION//./}
cd $CLOUDTIK_HOME

source $CONDA_HOME/bin/activate cloudtik_py${PYTHON_TAG} || conda create -n cloudtik_py${PYTHON_TAG} -y python=${PYTHON_VERSION}
source $CONDA_HOME/bin/activate cloudtik_py${PYTHON_TAG}

which twine || pip install twine

# upload pip
twine check ./python/dist/*
twine upload ./python/dist/*

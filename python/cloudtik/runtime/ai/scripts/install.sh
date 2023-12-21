#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

export USER_HOME=/home/$(whoami)
export RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

# Frameworks installation functions
. "$BIN_DIR"/install-ai-frameworks.sh

install_tools() {
    # Install necessary tools
    which numactl > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install numactl -y > /dev/null)
    which cmake > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install cmake -y > /dev/null)
    which g++-9 > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install g++-9 -y > /dev/null)

    # SQL client tools
    which mysql > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install mysql-client -y > /dev/null)

    POSTGRES_DRIVER=$(pip freeze | grep psycopg2)
    if [ "${POSTGRES_DRIVER}" == "" ]; then
        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install libpq-dev -y > /dev/null
    fi

    which psql > /dev/null \
      || (sudo apt-get -qq update -y > /dev/null; \
          sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install postgresql-client -y > /dev/null)
}

install_ai() {
    mkdir -p $RUNTIME_PATH/mlflow

    # Install Machine Learning libraries and components
    echo "Installing machine learning tools and libraries..."
    # chardet==3.0.4 from azure-cli
    pip --no-cache-dir -qq install \
        mlflow==2.3.1 \
        SQLAlchemy==1.4.46 \
        alembic==1.10.1 \
        pymysql==1.0.3 \
        pyarrow==8.0.0 \
        hyperopt==0.2.7 \
        scikit-learn==1.0.2 \
        xgboost==1.7.5 \
        transformers==4.30.2 \
        pandas==2.0.1 \
        category-encoders==2.6.0 \
        h5py==3.8.0 \
        lightgbm==3.3.5 \
        tensorflow-text==2.12.1 \
        datasets~=2.9.0 \
        tensorflow-datasets~=4.8.2 \
        tensorflow-hub~=0.12.0 \
        protobuf==3.20.3 \
        psycopg2==2.9.6

    echo "Installing deep learning frameworks and libraries..."
    pip --no-cache-dir -qq install tensorflow==2.12.0

    if [ "$AI_WITH_GPU" == "true" ]; then
        echo "Installing torch for GPU..."
        pip --no-cache-dir -qq install torch==1.13.1+cu117 torchvision==0.14.1+cu117 \
            --extra-index-url https://download.pytorch.org/whl/cu117
    else
        echo "Installing torch for CPU..."
        pip --no-cache-dir -qq install torch==1.13.1 torchvision==0.14.1 \
            --extra-index-url https://download.pytorch.org/whl/cpu
    fi

    pip --no-cache-dir -qq install transformers==4.11.0
    pip --no-cache-dir -qq install librosa==0.9.2 opencv-python-headless==4.6.0.66 tensorflow-addons==0.17.1

    if [ "$AI_WITH_ONEAPI" == "true" ]; then
        . "$BIN_DIR"/install-ai-oneapi.sh
        install_ai_oneapi
    else
        install_openmpi
    fi

    echo "Installing Horovod..."
    export CXX=/usr/bin/g++-9 \
      && HOROVOD_WITH_TENSORFLOW=1 HOROVOD_WITH_PYTORCH=1 \
        HOROVOD_WITHOUT_MXNET=1 HOROVOD_WITH_GLOO=1 HOROVOD_WITH_MPI=1 \
        pip --no-cache-dir -qq install horovod[tensorflow,keras,pytorch,spark,pytorch-spark]==0.27.0
}

set_head_option "$@"
install_tools
install_ai
clean_install

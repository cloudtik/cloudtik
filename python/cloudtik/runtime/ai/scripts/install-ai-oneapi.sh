#!/bin/bash

setup_oneapi_repository() {
    wget -O- -q https://apt.repos.intel.com/intel-gpg-keys/GPG-PUB-KEY-INTEL-SW-PRODUCTS.PUB \
        | gpg --dearmor | sudo tee /usr/share/keyrings/oneapi-archive-keyring.gpg > /dev/null \
    && echo "deb [signed-by=/usr/share/keyrings/oneapi-archive-keyring.gpg] https://apt.repos.intel.com/oneapi all main" \
        | sudo tee /etc/apt/sources.list.d/oneAPI.list > /dev/null \
    && sudo apt-get update -y > /dev/null
}

cleanup_oneapi_repository() {
    sudo rm -f /etc/apt/sources.list.d/oneAPI.list
}

install_oneapi_mpi() {
    echo "Installing Intel MPI..."
    ONEAPI_MPI_HOME=/opt/intel/oneapi/mpi
    if [ ! -d "${ONEAPI_MPI_HOME}" ]; then
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
          intel-oneapi-mpi-2021.8.0 intel-oneapi-mpi-devel-2021.8.0 > /dev/null
        echo "if [ -f '/opt/intel/oneapi/mpi/latest/env/vars.sh' ]; then . '/opt/intel/oneapi/mpi/latest/env/vars.sh'; fi" >> ~/.bashrc
    fi
    source ${ONEAPI_MPI_HOME}/latest/env/vars.sh
}

install_oneapi_ccl() {
    echo "Installing oneCCL..."
    ONEAPI_COMPILER_HOME=/opt/intel/oneapi/compiler
    ONEAPI_TBB_HOME=/opt/intel/oneapi/tbb
    if [ ! -d "${ONEAPI_COMPILER_HOME}" ]; then
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
          intel-oneapi-compiler-dpcpp-cpp-runtime-2023.0.0 \
          intel-oneapi-compiler-shared-runtime-2023.0.0 > /dev/null
        echo "if [ -f '/opt/intel/oneapi/tbb/latest/env/vars.sh' ]; then . '/opt/intel/oneapi/tbb/latest/env/vars.sh'; fi" >> ~/.bashrc
        echo "if [ -f '/opt/intel/oneapi/compiler/latest/env/vars.sh' ]; then . '/opt/intel/oneapi/compiler/latest/env/vars.sh'; fi" >> ~/.bashrc
    fi
    source ${ONEAPI_TBB_HOME}/latest/env/vars.sh
    source ${ONEAPI_COMPILER_HOME}/latest/env/vars.sh

    ONEAPI_MKL_HOME=/opt/intel/oneapi/mkl
    if [ ! -d "${ONEAPI_MKL_HOME}" ]; then
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
          intel-oneapi-mkl-2023.0.0 > /dev/null
        echo "if [ -f '/opt/intel/oneapi/mkl/latest/env/vars.sh' ]; then . '/opt/intel/oneapi/mkl/latest/env/vars.sh'; fi" >> ~/.bashrc
    fi
    source ${ONEAPI_MKL_HOME}/latest/env/vars.sh

    ONEAPI_CCL_HOME=/opt/intel/oneapi/ccl
    if [ ! -d "${ONEAPI_CCL_HOME}" ]; then
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install -y \
          intel-oneapi-ccl-2021.8.0 intel-oneapi-ccl-devel-2021.8.0 > /dev/null
        echo "if [ -f '/opt/intel/oneapi/ccl/latest/env/vars.sh' ]; then . '/opt/intel/oneapi/ccl/latest/env/vars.sh'; fi" >> ~/.bashrc
    fi
    source ${ONEAPI_CCL_HOME}/latest/env/vars.sh
    # Configure Horovod to use CCL
    export HOROVOD_CPU_OPERATIONS=CCL
}

install_ipex() {
    CLOUDTIK_ENV_ROOT=$(dirname $(dirname $(which cloudtik)))
    # Install Jemalloc and Intel OpenMP for better performance
    conda install jemalloc intel-openmp -p ${CLOUDTIK_ENV_ROOT} -y > /dev/null
    pip --no-cache-dir -qq install intel-extension-for-pytorch==1.13.100+cpu \
        oneccl_bind_pt==1.13.0+cpu -f https://developer.intel.com/ipex-whl-stable-cpu
}

install_ai_oneapi() {
    echo "Installing oneAPI libraries..."
    setup_oneapi_repository

    install_oneapi_mpi
    install_oneapi_ccl
    install_ipex

    cleanup_oneapi_repository
}

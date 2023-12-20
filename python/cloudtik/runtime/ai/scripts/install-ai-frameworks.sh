#!/bin/bash

install_openmpi() {
    echo "Installing Open MPI..."
    which mpirun > /dev/null \
      || (mkdir -p /tmp/openmpi \
        && PREV_CUR_DIR=$(pwd) \
        && cd /tmp/openmpi \
        && wget -q --show-progress \
          https://www.open-mpi.org/software/ompi/v4.1/downloads/openmpi-4.1.4.tar.gz -O openmpi.tar.gz  \
        && tar --extract --file openmpi.tar.gz --directory /tmp/openmpi --strip-components 1 --no-same-owner \
        && echo "Open MPI: configure..." \
        && sudo ./configure --enable-orterun-prefix-by-default CC=gcc-9 CXX=g++-9 > /dev/null 2>&1 \
        && echo "Open MPI: make..." \
        && sudo make -j $(nproc) all > /dev/null 2>&1 \
        && echo "Open MPI: make install..." \
        && sudo make install > /dev/null 2>&1 \
        && sudo ldconfig \
        && cd ${PREV_CUR_DIR} \
        && sudo rm -rf /tmp/openmpi)
}

#!/usr/bin/env bash
#
# Copyright (c) 2020 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
MODEL_DIR=${MODEL_DIR-$PWD}

source "${MODEL_DIR}/scripts/utils.sh"
_get_platform_type

CORES=`lscpu | grep Core | awk '{print $4}'`
SOCKETS=`lscpu | grep Socket | awk '{print $2}'`
RUN_ARGS="--memory-allocator=tcmalloc --num-proc ${SOCKETS} --ncores-per-proc ${CORES}"

echo "Running '${SOCKETS}' processes"

ARGS=""
if [[ "$USE_IPEX" == "true" ]]; then
  ARGS="$ARGS --ipex --jit"
fi
echo "Running using ${ARGS} args ..."

cloudtik-run \
    ${RUN_ARGS} \
    models/image_recognition/pytorch/common/main.py \
    --arch resnet50 ../ \
    --evaluate \
    ${ARGS} \
    --batch-size 128 \
    --dummy

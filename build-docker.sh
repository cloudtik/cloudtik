#!/bin/bash
SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Import the default vars
. "$SCRIPT_DIR"/dev/set-default-vars.sh

# This script is for users to build docker images locally. It is most useful for users wishing to edit the
# cloudtik-base, cloudtik-deps, or cloudtik images. 

set -x

CLOUDTIK_VERSION=$(sed -n 's/__version__ = \"\(..*\)\"/\1/p' ./python/cloudtik/__init__.py)
CONDA_ENV_NAME="cloudtik"
DEVICE_TYPE=""
DEVICE_TAG=""
BASE_IMAGE="ubuntu:focal"
IMAGE_TAG="nightly"
CLOUDTIK_REGION="GLOBAL"

# We can define a image to runtimes map.
# The universe image is for tests. It include all the runtimes except
# apisix (cannot coexist with kong) and metastore (log4j shell2 vulnerability)
declare -A IMAGE_RUNTIME_MAP=(\
    ["universe"]="ai, bind, consul, coredns, dnsmasq, etcd, flink, grafana, hadoop, haproxy, hdfs, kafka, kong, minio, mongodb, mount, mysql, nginx, nodex, postgres, presto, prometheus, ray, redis, spark, sshserver, trino, yarn, zookeeper" \
)

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    --gpu)
        DEVICE_TYPE="gpu"
        DEVICE_TAG="-gpu"
        BASE_IMAGE="nvidia/cuda:11.7.1-cudnn8-devel-ubuntu20.04"
        ;;
    --base-image)
        # Override for the base image.
        shift
        BASE_IMAGE=$1
        ;;
    --region)
        # The region for cloud instance.
        # If equals to PRC, the download server for apt will be replaced by "mirrors.aliyun.com"
        shift
        CLOUDTIK_REGION=$1
        ;;
    --no-cache-build)
        NO_CACHE="--no-cache"
        ;;
    --shas-only)
        # output the SHA sum of each build. This is useful for scripting tests,
        # especially when builds of different versions are running on the same machine.
        # It also can facilitate cleanup.
        OUTPUT_SHA=YES
        ;;
    --wheel-to-use)
        # Which wheel to use. This defaults to the latest nightly on minimum supported python version of CloudTik
        shift
        WHEEL_TO_USE="$1"
        ;;
    --python-version)
        # Python version
        shift
        PYTHON_VERSION=$1
        ;;
    --image-tag)
        shift
        IMAGE_TAG=$1
        ;;
    --build-all)
        BUILD_ALL=YES
        ;;
    --build-cloudtik)
        BUILD_CLOUDTIK=YES
        ;;
    --build-dev)
        BUILD_DEV=YES
        ;;
    --build-spark)
        BUILD_SPARK=YES
        ;;
    --build-spark-optimized)
        BUILD_SPARK_OPTIMIZED=YES
        ;;
    --build-spark-benchmark)
        BUILD_SPARK_BENCHMARK=YES
        ;;
    --build-spark-optimized-benchmark)
        BUILD_SPARK_OPTIMIZED_BENCHMARK=YES
        ;;
    --build-ai-base)
        BUILD_AI_BASE=YES
        ;;
    --build-ai)
        BUILD_AI=YES
        ;;
    --build-ai-oneapi)
        BUILD_AI_ONEAPI=YES
        ;;
    --build-ai-benchmark)
        BUILD_AI_BENCHMARK=YES
        ;;
    --build-spark-ai-base)
        BUILD_SPARK_AI_BASE=YES
        ;;
    --build-spark-ai)
        BUILD_SPARK_AI=YES
        ;;
    --build-spark-ai-oneapi)
        BUILD_SPARK_AI_ONEAPI=YES
        ;;
    --build-spark-ai-benchmark)
        BUILD_SPARK_AI_BENCHMARK=YES
        ;;
    --build-runtime-images)
        shift
        BUILD_RUNTIME_IMAGES=YES
        RUNTIME_IMAGES_TO_BUILD=$1
        ;;
    *)
        echo "Usage: build-docker.sh [ --gpu ] [ --base-image ] [ --region ] [ --no-cache-build ] [ --shas-only ] [ --wheel-to-use ] [ --python-version ] [ --image-tag ]"
        echo "Images to build options:"
        echo "[ --build-all ] [ --build-cloudtik ] [ --build-dev ] [ --build-spark ] [ --build-spark-optimized ]"
        echo "[ --build-spark-benchmark ] [ --build-spark-optimized-benchmark ]"
        echo "[ --build-ai-base ] [ --build-ai ] [ --build-ai-oneapi ] [ --build-ai-benchmark ]"
        echo "[ --build-spark-ai-base ] [ --build-spark-ai ] [ --build-spark-ai-oneapi ] [ --build-spark-ai-benchmark ]"
        echo "[ --build-runtime-images runtime-images ]"
        exit 1
    esac
    shift
done

PYTHON_TAG=${PYTHON_VERSION//./}
arch=$(uname -m)
WHEEL_DIR=$(mktemp -d)

if [ -z "$WHEEL_TO_USE" ]; then
    if [ "$IMAGE_TAG" == "nightly" ]; then
        WHEEL_URL="https://d30257nes7d4fq.cloudfront.net/downloads/cloudtik/cloudtik-${CLOUDTIK_VERSION}-cp${PYTHON_TAG}-cp${PYTHON_TAG}-manylinux2014_x86_64.nightly.whl"
    else
        WHEEL_URL="https://d30257nes7d4fq.cloudfront.net/downloads/cloudtik/cloudtik-${CLOUDTIK_VERSION}-cp${PYTHON_TAG}-cp${PYTHON_TAG}-manylinux2014_x86_64.whl"
    fi
else
    if [ "$WHEEL_TO_USE" == "local" ]; then
        WHEEL_URL=python/dist/cloudtik-${CLOUDTIK_VERSION}-cp${PYTHON_TAG}-cp${PYTHON_TAG}-manylinux2014_${arch}.whl
    else
        WHEEL_URL="$WHEEL_TO_USE"
    fi
fi

if [[ "$WHEEL_URL" =~ ^http.* ]]; then
    wget --quiet "$WHEEL_URL" -P "$WHEEL_DIR"
else
    cp "$WHEEL_URL" "$WHEEL_DIR"
fi

WHEEL="$WHEEL_DIR/$(basename "$WHEEL_DIR"/*.whl)"

if [ $BUILD_CLOUDTIK ] || [ $BUILD_ALL ]; then
    # Build cloudtik-base, cloudtik-deps, and cloudtik.
    for IMAGE in "cloudtik-base"
    do
        cp "$WHEEL" "docker/$IMAGE/$(basename "$WHEEL")"
        if [ $OUTPUT_SHA ]; then
            IMAGE_SHA=$(docker build $NO_CACHE --build-arg DEVICE_TYPE="$DEVICE_TYPE" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" --build-arg CONDA_ENV_NAME="$CONDA_ENV_NAME" -q -t cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG docker/$IMAGE)
            echo "cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG SHA:$IMAGE_SHA"
        else
            docker build $NO_CACHE --build-arg DEVICE_TYPE="$DEVICE_TYPE" --build-arg BASE_IMAGE="$BASE_IMAGE" --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" --build-arg CONDA_ENV_NAME="$CONDA_ENV_NAME" -t cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG docker/$IMAGE
        fi
        rm "docker/$IMAGE/$(basename "$WHEEL")"
    done

    for IMAGE in "cloudtik-deps" "cloudtik"
    do
        cp "$WHEEL" "docker/$IMAGE/$(basename "$WHEEL")"
        if [ $OUTPUT_SHA ]; then
            IMAGE_SHA=$(docker build $NO_CACHE --build-arg DEVICE_TYPE="$DEVICE_TYPE" --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" -q -t cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG docker/$IMAGE)
            echo "cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG SHA:$IMAGE_SHA"
        else
            docker build $NO_CACHE --build-arg DEVICE_TYPE="$DEVICE_TYPE" --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG --build-arg WHEEL_PATH="$(basename "$WHEEL")" --build-arg PYTHON_VERSION="$PYTHON_VERSION" -t cloudtik/$IMAGE:$IMAGE_TAG$DEVICE_TAG docker/$IMAGE
        fi
        rm "docker/$IMAGE/$(basename "$WHEEL")"
    done
fi

# Build the current source
if [ $BUILD_DEV ] || [ $BUILD_ALL ]; then
    git rev-parse HEAD > ./docker/cloudtik-dev/git-rev
    git archive -o ./docker/cloudtik-dev/cloudtik.tar "$(git rev-parse HEAD)"
    if [ $OUTPUT_SHA ]; then
        IMAGE_SHA=$(docker build --no-cache -q -t cloudtik/cloudtik-dev docker/cloudtik-dev)
        echo "cloudtik/cloudtik-dev:latest SHA:$IMAGE_SHA"
    else
        docker build --no-cache -t cloudtik/cloudtik-dev docker/cloudtik-dev
    fi
    rm ./docker/cloudtik-dev/cloudtik.tar ./docker/cloudtik-dev/git-rev
fi

rm -rf "$WHEEL_DIR"

registry_regions=('GLOBAL')
if [ "${CLOUDTIK_REGION}" == "PRC" ]; then
    registry_regions[1]='PRC'
fi

for registry_region in ${registry_regions[@]};
do
    DOCKER_REGISTRY=""
    DOCKER_FILE_PATH=""
    if [ "${registry_region}" == "PRC" ]; then
        DOCKER_REGISTRY="registry.cn-shanghai.aliyuncs.com/"
        DOCKER_FILE_PATH="localize/PRC/"
    fi

    if [ "${DOCKER_FILE_PATH}" != "" ]; then
        # for building localized cloudtik image
        if [ -d "docker/${DOCKER_FILE_PATH}cloudtik" ] && ([ $BUILD_CLOUDTIK ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/cloudtik:$IMAGE_TAG$DEVICE_TAG \
              docker/${DOCKER_FILE_PATH}cloudtik
        fi
    fi

    if [ -d "docker/${DOCKER_FILE_PATH}runtime/spark" ] && ([ $BUILD_SPARK ] || [ $BUILD_ALL ]); then
        docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
          -t ${DOCKER_REGISTRY}cloudtik/spark-runtime:$IMAGE_TAG$DEVICE_TAG \
          docker/${DOCKER_FILE_PATH}runtime/spark
    fi

    if [ "$DEVICE_TYPE" == "" ]; then
        # CPU only
        if [ -d "docker/${DOCKER_FILE_PATH}runtime/spark/optimized" ] && ([ $BUILD_SPARK_OPTIMIZED ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-optimized:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/spark/optimized
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime" ] && ([ $BUILD_RUNTIME_IMAGES ] || [ $BUILD_ALL ]); then
            IFS=', ' read -r -a image_names <<< "$RUNTIME_IMAGES_TO_BUILD"
            for image_name in ${image_names[@]}
            do
                image_runtimes=${IMAGE_RUNTIME_MAP[${image_name}]}
                if [ -z "$image_runtimes" ]; then
                    # Use the image name as runtime name if there is no map
                    image_runtimes="${image_name}"
                fi
                docker build $NO_CACHE --build-arg RUNTIME_IMAGE_NAME=${image_name} \
                  --build-arg BASE_TAG=$IMAGE_TAG \
                  --build-arg IMAGE_RUNTIMES="$image_runtimes" \
                  -t ${DOCKER_REGISTRY}cloudtik/${image_name}:$IMAGE_TAG \
                  docker/${DOCKER_FILE_PATH}runtime
            done
        fi
    fi

    # Build the AI base image which is needed as the base image for all other AI runtime image
    if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/base" ] && ([ $BUILD_AI_BASE ] || [ $BUILD_AI ] || [ $BUILD_AI_ONEAPI ] || [ $BUILD_ALL ]); then
        docker build $NO_CACHE --build-arg BASE_NAME=cloudtik-deps \
          --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
          -t ${DOCKER_REGISTRY}cloudtik/ai-base:$IMAGE_TAG$DEVICE_TAG \
          docker/${DOCKER_FILE_PATH}runtime/ai/base
    fi

    if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/base" ] && ([ $BUILD_SPARK_AI_BASE ] || [ $BUILD_SPARK_AI ] || [ $BUILD_SPARK_AI_ONEAPI ] || [ $BUILD_ALL ]); then
        docker build $NO_CACHE --build-arg BASE_NAME=spark-runtime \
          --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
          -t ${DOCKER_REGISTRY}cloudtik/spark-ai-base:$IMAGE_TAG$DEVICE_TAG \
          docker/${DOCKER_FILE_PATH}runtime/ai/base
    fi

    if [ "$DEVICE_TYPE" == "" ]; then
        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/cpu" ] && ([ $BUILD_AI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/ai-runtime:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/cpu
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/oneapi" ] && ([ $BUILD_AI_ONEAPI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/ai-oneapi:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/oneapi
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/cpu" ] && ([ $BUILD_SPARK_AI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_PREFIX=spark- \
             --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-ai-runtime:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/cpu
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/oneapi" ] && ([ $BUILD_SPARK_AI_ONEAPI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_PREFIX=spark- \
              --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-ai-oneapi:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/oneapi
        fi
    else
        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/$DEVICE_TYPE" ] && ([ $BUILD_AI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/ai-runtime:$IMAGE_TAG$DEVICE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/$DEVICE_TYPE
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/$DEVICE_TYPE" ] && ([ $BUILD_SPARK_AI ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_PREFIX=spark- \
              --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-ai-runtime:$IMAGE_TAG$DEVICE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/ai/$DEVICE_TYPE
        fi
    fi

    if [ "$DEVICE_TYPE" == "" ]; then
        # CPU only
        if [ -d "docker/${DOCKER_FILE_PATH}runtime/spark/benchmark" ] && ([ $BUILD_SPARK_BENCHMARK ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-runtime-benchmark:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/spark/benchmark
        fi

        if [ -d "docker/${DOCKER_FILE_PATH}runtime/spark/benchmark/optimized" ] && ([ $BUILD_SPARK_OPTIMIZED_BENCHMARK ] || [ $BUILD_ALL ]); then
            docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG \
              -t ${DOCKER_REGISTRY}cloudtik/spark-optimized-benchmark:$IMAGE_TAG \
              docker/${DOCKER_FILE_PATH}runtime/spark/benchmark/optimized
        fi
    fi

    if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/benchmark" ] && ([ $BUILD_AI_BENCHMARK ] || [ $BUILD_ALL ]); then
        docker build $NO_CACHE --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
          -t ${DOCKER_REGISTRY}cloudtik/ai-runtime-benchmark:$IMAGE_TAG$DEVICE_TAG \
          docker/${DOCKER_FILE_PATH}runtime/ai/benchmark
    fi

    if [ -d "docker/${DOCKER_FILE_PATH}runtime/ai/benchmark" ] && ([ $BUILD_SPARK_AI_BENCHMARK ] || [ $BUILD_ALL ]); then
        docker build $NO_CACHE --build-arg BASE_PREFIX=spark- \
          --build-arg BASE_TAG=$IMAGE_TAG$DEVICE_TAG \
          -t ${DOCKER_REGISTRY}cloudtik/spark-ai-runtime-benchmark:$IMAGE_TAG$DEVICE_TAG \
          docker/${DOCKER_FILE_PATH}runtime/ai/benchmark
    fi
done

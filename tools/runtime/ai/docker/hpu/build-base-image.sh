#!/usr/bin/env bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

BUILD_OS=ubuntu20.04
IMAGE_TAG="1.11.0-$BUILD_OS"

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    --image-tag)
        # Override for the image tag.
        shift
        IMAGE_TAG=$1
        ;;
    *)
        echo "Usage: build-base-image.sh [ --image-tag ]"
        exit 1
    esac
    shift
done

WORKING_DIR=/tmp/habanaai
rm -rf $WORKING_DIR
mkdir -p $WORKING_DIR
cd $WORKING_DIR
git clone https://github.com/HabanaAI/Setup_and_Install.git
cd $WORKING_DIR/Setup_and_Install/dockerfiles/base
cp $SCRIPT_DIR/Dockerfile.ubuntu20.04 ./Dockerfile.ubuntu20.04

make build BUILD_OS=$BUILD_OS

# tagging
RESULT_IMAGE_TAG=$(sudo docker images base-installer-$BUILD_OS --format='{{.Tag}}')
if [ "$RESULT_IMAGE_TAG" != "" ]; then
    sudo docker tag base-installer-$BUILD_OS:$RESULT_IMAGE_TAG habana/synapseai:${IMAGE_TAG}
else
    echo "The expected image is not found. Please check build errors."
fi

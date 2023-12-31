#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
source ${SCRIPT_DIR}/../configure.sh

RESNET50_HOME=$QUICKSTART_WORKING/resnet50
RESNET50_MODEL=$RESNET50_HOME/model
RESNET50_DATA=$RESNET50_HOME/data

PHASE="inference"

if [ ! -n "${QUICKSTART_HOME}" ]; then
  echo "Please set environment variable '\${QUICKSTART_HOME}'."
  exit 1
fi

usage(){
    echo "Usage: prepare-data.sh  [ --phase training | inference] "
    exit 1
}

while [[ $# -gt 0 ]]
do
    key="$1"
    case $key in
    --phase)
        # training or inference
        shift
        PHASE=$1
        ;;
    *)
        usage
    esac
    shift
done

download_inference_data() {
    mkdir -p $RESNET50_DATA/val
    cd $RESNET50_DATA
    wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_val.tar
}

download_training_data() {
    
    mkdir -p $RESNET50_DATA
    cd $RESNET50_DATA
    wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_train.tar
#    wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_train_t3.tar
#    wget https://image-net.org/data/ILSVRC/2012/ILSVRC2012_img_test_v10102019.tar

}


prepare_training_data() {
    mkdir -p $RESNET50_DATA/train
    cd $RESNET50_DATA
    tar -xvf ILSVRC2012_img_train.tar -C $RESNET50_DATA/train

    for tar_file in $RESNET50_DATA/train/*.tar; do
        mkdir -p $RESNET50_DATA/train/$(basename $tar_file .tar);
        tar -xvf $tar_file -C /home/cloudtik/mltest/resnet50/train/$(basename $tar_file .tar);
    done
    cd $RESNET50_DATA/train
    rm -rf ./*.tar
}


prepare_val_data() {
    mkdir -p $RESNET50_DATA/val
    cd $RESNET50_DATA
    tar -xvf ILSVRC2012_img_val.tar -C $RESNET50_DATA/val

    cd $RESNET50_DATA/val
    wget https://raw.githubusercontent.com/soumith/imagenetloader.torch/master/valprep.sh
    bash valprep.sh
}

if [ "${PHASE}" = "training" ]; then
    download_training_data
    download_inference_data
    prepare_training_data
    prepare_val_data
elif [ "${PHASE}" = "inference" ]; then
    download_inference_data
    prepare_val_data
else
    usage
fi
move_to_workspace $RESNET50_HOME

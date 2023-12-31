import argparse
import os

from cloudtik.runtime.ai.util.utils import load_config_from

from transformers import (
    TrainingArguments
)

from disease_prediction.utils import DEFAULT_TRAIN_OUTPUT, DEFAULT_PREDICT_OUTPUT
from disease_prediction.vision.run import (
    run as run_train_vision,
    TrainerArguments as VisionTrainerArguments
)
from disease_prediction.dlsa.run import (
    run as run_train_dlsa,
    TrainerArguments as DLSATrainerArguments
)
from disease_prediction.consult.run import (
    run as run_train_consult,
    TrainerArguments as ConsultTrainerArguments)
from disease_prediction.dlsa.utils import DatasetConfig

from disease_prediction.dlsa.data.split import \
    get_split_output_dir as get_dlsa_split_output_dir
from disease_prediction.vision.data.split import \
    get_split_output_dir as get_vision_split_output_dir


def get_output_dir(args, task):
    return os.path.join(args.output_dir, task)


def get_model_output_dir(args, task):
    if args.model_path:
        return os.path.join(args.model_path, task)
    else:
        return os.path.join(get_output_dir(args, task), "model")


def get_dlsa_output_dir(args):
    return get_output_dir(args, "dlsa")


def get_dlsa_model_output_dir(args):
    return get_model_output_dir(args, "dlsa")


def get_vision_output_dir(args):
    return get_output_dir(args, "vision")


def get_vision_model_output_dir(args):
    return get_model_output_dir(args, "vision")


def get_consult_output_dir(args):
    return get_output_dir(args, "consult")


def get_consult_model_output_dir(args):
    return get_model_output_dir(args, "consult")


def get_config_dir():
    this_dir = os.path.dirname(__file__)
    return os.path.join(
        os.path.dirname(this_dir), "config")


def _run_dlsa(args):
    config_dir = get_config_dir()
    dlsa_args = DLSATrainerArguments()

    dlsa_modeling_config_file = os.path.join(
        config_dir, "dlsa-modeling-config.yaml")
    load_config_from(dlsa_modeling_config_file, dlsa_args)

    dlsa_args.output_dir = get_dlsa_output_dir(args)
    os.makedirs(dlsa_args.output_dir, exist_ok=True)

    dlsa_args.no_train = args.no_train
    dlsa_args.no_predict = args.no_predict

    # load dataset config from dataset_config file
    dlsa_args.dataset = "local"
    dataset_config = DatasetConfig()
    dlsa_dataset_config_file = os.path.join(
        config_dir, "dlsa-dataset-config.yaml")
    load_config_from(dlsa_dataset_config_file, dataset_config)

    # TODO: handle if this is a pure predict run
    dlsa_split_output_dir = get_dlsa_split_output_dir(args.processed_data_path)
    dataset_config.train = os.path.join(dlsa_split_output_dir, "train.csv")
    dataset_config.test = os.path.join(dlsa_split_output_dir, "test.csv")
    dlsa_args.dataset_config = dataset_config

    # training arguments for transformer
    dlsa_args.model_dir = get_dlsa_model_output_dir(args)
    os.makedirs(dlsa_args.model_dir, exist_ok=True)

    # load training arguments or set automatically
    training_args = TrainingArguments(output_dir=dlsa_args.model_dir)
    dlsa_training_arguments_file = os.path.join(
        config_dir, "dlsa-training-arguments.yaml")
    load_config_from(dlsa_training_arguments_file, training_args)
    training_args.output_dir = dlsa_args.model_dir

    dlsa_args.training_args = training_args

    run_train_dlsa(dlsa_args)


def _run_vision(args):
    # run vision and doc training
    vision_args = VisionTrainerArguments()

    vision_split_output_dir = get_vision_split_output_dir(args.processed_data_path)
    vision_args.data_path = vision_split_output_dir

    vision_args.output_dir = get_vision_output_dir(args)
    os.makedirs(vision_args.output_dir, exist_ok=True)
    vision_args.model_dir = get_vision_model_output_dir(args)
    os.makedirs(vision_args.model_dir, exist_ok=True)

    vision_args.no_train = args.no_train
    vision_args.no_predict = args.no_predict

    config_dir = get_config_dir()
    vision_training_arguments_file = os.path.join(
        config_dir, "vision-training-arguments.yaml")
    load_config_from(vision_training_arguments_file, vision_args)

    run_train_vision(vision_args)


def _run_consult(args):
    consult_args = ConsultTrainerArguments()

    # set the inputs from the previous steps
    dlsa_split_output_dir = get_dlsa_split_output_dir(
        args.processed_data_path)
    dlsa_output_dir = get_dlsa_output_dir(args)
    vision_output_dir = get_vision_output_dir(args)

    consult_args.dlsa_train_input = os.path.join(
        dlsa_split_output_dir, "train.csv")
    consult_args.dlsa_train_output = os.path.join(
        dlsa_output_dir, DEFAULT_TRAIN_OUTPUT)
    consult_args.vision_train_output = os.path.join(
        vision_output_dir, DEFAULT_TRAIN_OUTPUT)

    consult_args.dlsa_predict_input = os.path.join(
        dlsa_split_output_dir, "test.csv")
    consult_args.dlsa_predict_output = os.path.join(
        dlsa_output_dir, DEFAULT_PREDICT_OUTPUT)
    consult_args.vision_predict_output = os.path.join(
        vision_output_dir, DEFAULT_PREDICT_OUTPUT)

    consult_args.output_dir = get_consult_output_dir(args)
    os.makedirs(consult_args.output_dir, exist_ok=True)

    model_dir = get_consult_model_output_dir(args)
    os.makedirs(model_dir, exist_ok=True)
    consult_args.model_file = os.path.join(
        model_dir, "model.csv")

    consult_args.no_train = args.no_train
    consult_args.no_predict = args.no_predict

    run_train_consult(consult_args)


def run(args):
    if not args.no_dlsa:
        _run_dlsa(args)

    if not args.no_vision:
        _run_vision(args)

    if not args.no_consult:
        _run_consult(args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Disease Prediction Training")
    parser.add_argument(
        "--no-dlsa", "--no_dlsa",
        default=False, action="store_true",
        help="whether to run dlsa.")
    parser.add_argument(
        "--no-vision", "--no_vision",
        default=False, action="store_true",
        help="whether to run vision.")
    parser.add_argument(
        "--no-consult", "--no_consult",
        default=False, action="store_true",
        help="whether to run consult.")
    parser.add_argument(
        "--no-train", "--no_train",
        default=False, action="store_true",
        help="whether to train on train the data.")
    parser.add_argument(
        "--no-predict", "--no_predict",
        default=False, action="store_true",
        help="whether to predict on test the data.")

    parser.add_argument(
        "--processed-data-path", "--processed_data_path",
        type=str,
        help="Path to the processed data directory",
    )
    parser.add_argument(
        "--model-path", "--model_path",
        type=str,
        help="Path to the trained model for predict",
    )
    parser.add_argument(
        "--output-dir", "--output_dir",
        type=str,
        help="Path to the output directory",
    )
    parser.add_argument(
        "--temp-dir", "--temp_dir",
        type=str,
        help="Path to the intermediate directory",
    )

    args = parser.parse_args()
    print(args)

    run(args)

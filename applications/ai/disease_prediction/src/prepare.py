#
# Copyright (c) 2023 Intel Corporation
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
import argparse
import os

from cloudtik.runtime.ai.util.utils import load_config_from
from disease_prediction.data.download import download
from disease_prediction.data.process import process
from disease_prediction.data.split import split
from disease_prediction.utils import _get_data_api


def _get_dataset_config():
    # load dataset config from dataset_config file
    this_dir = os.path.dirname(__file__)
    config_dir = os.path.join(
        os.path.dirname(this_dir), "config")

    dlsa_dataset_config_file = os.path.join(
        config_dir, "dlsa-dataset-config.yaml")
    return load_config_from(dlsa_dataset_config_file)


def run(args):
    if not args.dataset_path:
        raise ValueError(
            "Please specify dataset-path for storing the download dataset.")

    if not args.no_download:
        download(args.dataset_path)

    data_api = _get_data_api(args)
    if not args.no_process:
        if not os.path.isdir(args.dataset_path):
            raise ValueError("Dataset directory {} doesn't exist.".format(args.dataset_path))

        if not args.image_path:
            raise ValueError("Please specify image-path of the images.")

        if not args.output_dir:
            args.output_dir = args.dataset_path
            print("output-dir is not specified. Default to: {}".format(
                args.output_dir))
        process(
            args.dataset_path,
            args.image_path,
            args.output_dir,
            data_api=data_api)
        print("Data process completed.")

    if not args.no_split:
        if not args.output_dir:
            args.output_dir = args.dataset_path
            print("output-dir is not specified. Default to: {}".format(
                args.output_dir))

        dataset_config = _get_dataset_config()
        split(
            processed_data_dir=args.output_dir,
            output_dir=args.output_dir,
            test_size=args.test_size,
            dataset_config=dataset_config,
            data_api=data_api)
        print("Data split completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data process for BRCA dataset")

    parser.add_argument(
        "--no-download", "--no_download",
        default=False, action="store_true",
        help="whether to do download the data.")
    parser.add_argument(
        "--no-process", "--no_process",
        default=False, action="store_true",
        help="whether to process the data.")
    parser.add_argument(
        "--no-split", "--no_split",
        default=False, action="store_true",
        help="whether to split to the train and test data.")
    parser.add_argument(
        "--dataset-path", "--dataset_path",
        type=str,
        help="Path to the dataset directory",
    )
    parser.add_argument(
        "--image-path", "--image_path",
        type=str,
        help="Path to the image directory",
    )
    parser.add_argument(
        "--output-dir", "--output_dir",
        type=str,
        help="Path to the output directory",
    )
    parser.add_argument(
        "--test_size",
        type=float, default=0.1)

    args = parser.parse_args()
    print(args)

    run(args)

import argparse

from cloudtik.core._private.util.runtime_utils import get_runtime_value
from cloudtik.core.api import configure_logging
from cloudtik.runtime.common.utils import SERVICE_COMMAND_START
from cloudtik.runtime.elasticsearch.scripting import configure_clustering
from cloudtik.runtime.elasticsearch.utils import ELASTICSEARCH_CLUSTER_MODE_CLUSTER


def main():
    parser = argparse.ArgumentParser(
        description="Start or stop runtime services")
    parser.add_argument(
        '--head', action='store_true', default=False,
        help='Start or stop services for head node.')
    # positional
    parser.add_argument(
        "command", type=str,
        help="The service command to execute: start or stop")
    parser.add_argument(
        "command_args",
        nargs=argparse.REMAINDER,
    )
    args = parser.parse_args()
    cluster_mode = get_runtime_value("ELASTICSEARCH_CLUSTER_MODE")
    if (cluster_mode == ELASTICSEARCH_CLUSTER_MODE_CLUSTER and
            args.command == SERVICE_COMMAND_START):
        configure_logging(verbosity=0)
        configure_clustering(args.head)


if __name__ == "__main__":
    main()

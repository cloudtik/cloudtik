import argparse

from cloudtik.core._private.runtime_utils import get_runtime_value
from cloudtik.core.api import configure_logging
from cloudtik.runtime.redis.scripting import init_cluster_service
from cloudtik.runtime.redis.utils import REDIS_CLUSTER_MODE_SHARDING


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
    configure_logging(verbosity=0)

    cluster_mode = get_runtime_value("REDIS_CLUSTER_MODE")
    if (cluster_mode == REDIS_CLUSTER_MODE_SHARDING and
            args.command == "start"):
        init_cluster_service(args.head)


if __name__ == "__main__":
    main()

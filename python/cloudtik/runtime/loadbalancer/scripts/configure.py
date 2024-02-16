import argparse

from cloudtik.core._private.util.runtime_utils import get_runtime_bool
from cloudtik.core.api import configure_logging
from cloudtik.runtime.loadbalancer.scripting import configure_backend


def main():
    parser = argparse.ArgumentParser(
        description="Configuring runtime.")
    parser.add_argument(
        '--head', action='store_true', default=False,
        help='Configuring for head node.')
    args = parser.parse_args()
    configure_logging(verbosity=0)

    high_availability = get_runtime_bool("LOAD_BALANCER_HIGH_AVAILABILITY")
    if high_availability or args.head:
        configure_backend(args.head)


if __name__ == "__main__":
    main()

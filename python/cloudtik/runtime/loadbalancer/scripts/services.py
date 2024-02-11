import argparse

from cloudtik.core._private.util.runtime_utils import get_runtime_value, get_runtime_bool
from cloudtik.runtime.loadbalancer.scripting import start_controller, stop_controller
from cloudtik.runtime.loadbalancer.utils import LOAD_BALANCER_CONFIG_MODE_DYNAMIC


def start_service(head):
    config_mode = get_runtime_value("LOAD_BALANCER_CONFIG_MODE")
    if config_mode == LOAD_BALANCER_CONFIG_MODE_DYNAMIC:
        # needed pull service only for dynamic backend
        start_controller(head)


def stop_service():
    config_mode = get_runtime_value("LOAD_BALANCER_CONFIG_MODE")
    if config_mode == LOAD_BALANCER_CONFIG_MODE_DYNAMIC:
        stop_controller()


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

    high_availability = get_runtime_bool("LOAD_BALANCER_HIGH_AVAILABILITY")
    if high_availability or args.head:
        if args.command == "start":
            start_service(args.head)
        elif args.command == "stop":
            stop_service()


if __name__ == "__main__":
    main()

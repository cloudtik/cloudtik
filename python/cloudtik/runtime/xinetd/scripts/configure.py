import argparse

from cloudtik.runtime.xinetd.scripting import configure_health_checks


def main():
    parser = argparse.ArgumentParser(
        description="Configuring runtime.")
    parser.add_argument(
        '--head', action='store_true', default=False,
        help='Configuring for head node.')
    args = parser.parse_args()

    configure_health_checks(args.head)


if __name__ == "__main__":
    main()

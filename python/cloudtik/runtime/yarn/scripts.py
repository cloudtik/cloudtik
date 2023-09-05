import logging

import click

from cloudtik.core._private import constants
from cloudtik.core._private import logging_utils
from cloudtik.core._private.cli_logger import (cli_logger, add_click_logging_options)
from cloudtik.runtime.yarn.utils import print_request_rest_yarn

logger = logging.getLogger(__name__)


@click.group()
@click.option(
    "--logging-level",
    required=False,
    default=constants.LOGGER_LEVEL_INFO,
    type=str,
    help=constants.LOGGER_LEVEL_HELP)
@click.option(
    "--logging-format",
    required=False,
    default=constants.LOGGER_FORMAT,
    type=str,
    help=constants.LOGGER_FORMAT_HELP)
@click.version_option()
def cli(logging_level, logging_format):
    level = logging.getLevelName(logging_level.upper())
    logging_utils.setup_logger(level, logging_format)
    cli_logger.set_format(format_tmpl=logging_format)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--endpoint",
    required=False,
    type=str,
    help="The resource endpoint for the YARN rest API")
@add_click_logging_options
def yarn(cluster_config_file, cluster_name, endpoint):
    print_request_rest_yarn(cluster_config_file, cluster_name, endpoint)


cli.add_command(yarn)


def main():
    return cli()


if __name__ == "__main__":
    main()

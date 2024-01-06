import logging

import click

from cloudtik.core._private.cli_logger import (add_click_logging_options)
from cloudtik.core._private.cluster.cluster_config import _load_cluster_config
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_YARN
from cloudtik.core._private.utils import load_head_cluster_config, print_json_formatted
from cloudtik.runtime.yarn.utils import request_rest_yarn
from cloudtik.scripts.utils import NaturalOrderGroup

logger = logging.getLogger(__name__)


@click.group(cls=NaturalOrderGroup)
def yarn():
    """
    Commands for YARN runtime.
    """
    pass


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
def rest(cluster_config_file, cluster_name, endpoint):
    """Make a REST API request to the endpoint."""
    config = _load_cluster_config(cluster_config_file, cluster_name)
    _rest(config, endpoint)


def _rest(config, endpoint, on_head=False):
    response = request_rest_yarn(
        config, endpoint, on_head=on_head)
    print_json_formatted(response)


yarn.add_command(rest)


@click.group(name=BUILT_IN_RUNTIME_YARN, cls=NaturalOrderGroup)
def yarn_on_head():
    """
    Commands running on head for YARN runtime.
    """
    pass


@click.command(name='rest')
@click.option(
    "--endpoint",
    required=False,
    type=str,
    help="The resource endpoint for the YARN rest API")
@add_click_logging_options
def rest_on_head(endpoint):
    """Make a REST API request to the endpoint."""
    config = load_head_cluster_config()
    _rest(config, endpoint, on_head=True)


yarn_on_head.add_command(rest_on_head)

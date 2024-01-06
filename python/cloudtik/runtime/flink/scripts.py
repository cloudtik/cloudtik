import logging

import click

from cloudtik.core._private.cli_logger import (add_click_logging_options)
from cloudtik.core._private.cluster.cluster_config import _load_cluster_config
from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_FLINK
from cloudtik.core._private.utils import print_json_formatted, load_head_cluster_config
from cloudtik.runtime.flink.utils import get_runtime_default_storage, request_rest_jobs
from cloudtik.scripts.utils import NaturalOrderGroup

logger = logging.getLogger(__name__)


@click.group(cls=NaturalOrderGroup)
def flink():
    """
    Commands for Flink runtime.
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
    help="The resource endpoint for the history server rest API")
@add_click_logging_options
def jobs(cluster_config_file, cluster_name, endpoint):
    """Make a REST API request to list jobs."""
    config = _load_cluster_config(cluster_config_file, cluster_name)
    _jobs(config, endpoint)


def _jobs(config, endpoint, on_head=False):
    response = request_rest_jobs(
        config, endpoint, on_head=on_head)
    print_json_formatted(response)


@click.command()
@click.argument("cluster_config_file", required=True, type=str)
@click.option(
    "--cluster-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured cluster name.")
@click.option(
    "--default-storage",
    is_flag=True,
    default=False,
    help="Show the default storage of the cluster.")
@add_click_logging_options
def info(cluster_config_file, cluster_name, default_storage):
    """Show info."""
    config = _load_cluster_config(cluster_config_file, cluster_name)
    _info(config, default_storage)


def _info(config, default_storage):
    if default_storage:
        # show default storage
        default_storage_uri = get_runtime_default_storage(config)
        if default_storage_uri:
            click.echo(default_storage_uri)


flink.add_command(jobs)
flink.add_command(info)


@click.group(name=BUILT_IN_RUNTIME_FLINK, cls=NaturalOrderGroup)
def flink_on_head():
    """
    Commands running on head for Flink runtime.
    """
    pass


@click.command(name='jobs')
@click.option(
    "--endpoint",
    required=False,
    type=str,
    help="The resource endpoint for the history server rest API")
@add_click_logging_options
def jobs_on_head(endpoint):
    """Make a REST API request to the list jobs."""
    config = load_head_cluster_config()
    _jobs(config, endpoint, on_head=True)


@click.command(name='info')
@click.option(
    "--default-storage",
    is_flag=True,
    default=False,
    help="Show the default storage of the cluster.")
@add_click_logging_options
def info_on_head(default_storage):
    """Show info."""
    config = load_head_cluster_config()
    _info(config, default_storage)


flink_on_head.add_command(jobs_on_head)
flink_on_head.add_command(info_on_head)

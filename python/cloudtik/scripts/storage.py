import copy
import click
import logging
import urllib
import urllib.error
import urllib.parse

from cloudtik.core._private.storage.storage_operator import (
    create_storage, delete_storage, show_storage_info)
from cloudtik.core._private.cli_logger import (add_click_logging_options, cli_logger)
from cloudtik.core._private.util.core_utils import url_read
from cloudtik.scripts.utils import NaturalOrderGroup

logger = logging.getLogger(__name__)


@click.group(cls=NaturalOrderGroup)
def storage():
    """
    Commands for working with storage.
    """


@storage.command()
@click.argument("storage_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--storage-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured storage name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local storage config cache.")
@add_click_logging_options
def create(
        storage_config_file, yes, storage_name, no_config_cache):
    """Create a storage on cloud using the storage configuration file."""
    if urllib.parse.urlparse(storage_config_file).scheme in ("http", "https"):
        try:
            content = url_read(storage_config_file, timeout=5)
            file_name = storage_config_file.split("/")[-1]
            with open(file_name, "wb") as f:
                f.write(content)
                storage_config_file = file_name
        except urllib.error.HTTPError as e:
            cli_logger.warning("{}", str(e))
            cli_logger.warning(
                "Could not download remote storage configuration file.")

    create_storage(
        config_file=storage_config_file,
        yes=yes,
        override_storage_name=storage_name,
        no_config_cache=no_config_cache)


@storage.command()
@click.argument("storage_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--storage-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured storage name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local storage config cache.")
@add_click_logging_options
def delete(
        storage_config_file, yes, storage_name, no_config_cache):
    """Delete a storage and the associated cloud resources."""
    delete_storage(
        storage_config_file, yes, storage_name,
        no_config_cache=no_config_cache)


@storage.command()
@click.argument("storage_config_file", required=True, type=str)
@click.option(
    "--storage-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured storage name.")
@add_click_logging_options
def info(storage_config_file, storage_name):
    """Show storage status."""
    show_storage_info(storage_config_file, storage_name)


def _add_command_alias(command, name, hidden):
    new_command = copy.deepcopy(command)
    new_command.hidden = hidden
    storage.add_command(new_command, name=name)


# core commands working on storage
storage.add_command(create)
storage.add_command(delete)

# commands for storage info
storage.add_command(info)

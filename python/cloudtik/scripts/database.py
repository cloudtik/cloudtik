import copy
import click
import logging
import urllib
import urllib.error
import urllib.parse

from cloudtik.core._private.database.database_operator import (
    create_database, delete_database, show_database_info)
from cloudtik.core._private.cli_logger import (add_click_logging_options, cli_logger)
from cloudtik.core._private.util.core_utils import url_read
from cloudtik.scripts.utils import NaturalOrderGroup

logger = logging.getLogger(__name__)


@click.group(cls=NaturalOrderGroup)
def database():
    """
    Commands for working with database.
    """


@database.command()
@click.argument("database_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--database-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured database name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local database config cache.")
@add_click_logging_options
def create(
        database_config_file, yes, database_name, no_config_cache):
    """Create a database on cloud using the database configuration file."""
    if urllib.parse.urlparse(database_config_file).scheme in ("http", "https"):
        try:
            content = url_read(database_config_file, timeout=5)
            file_name = database_config_file.split("/")[-1]
            with open(file_name, "wb") as f:
                f.write(content)
                database_config_file = file_name
        except urllib.error.HTTPError as e:
            cli_logger.warning("{}", str(e))
            cli_logger.warning(
                "Could not download remote database configuration file.")

    create_database(
        config_file=database_config_file,
        yes=yes,
        override_database_name=database_name,
        no_config_cache=no_config_cache)


@database.command()
@click.argument("database_config_file", required=True, type=str)
@click.option(
    "--yes",
    "-y",
    is_flag=True,
    default=False,
    help="Don't ask for confirmation.")
@click.option(
    "--database-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured database name.")
@click.option(
    "--no-config-cache",
    is_flag=True,
    default=False,
    help="Disable the local database config cache.")
@add_click_logging_options
def delete(
        database_config_file, yes, database_name, no_config_cache):
    """Delete a database and the associated cloud resources."""
    delete_database(
        database_config_file, yes, database_name,
        no_config_cache=no_config_cache)


@database.command()
@click.argument("database_config_file", required=True, type=str)
@click.option(
    "--database-name",
    "-n",
    required=False,
    type=str,
    help="Override the configured database name.")
@add_click_logging_options
def info(database_config_file, database_name):
    """Show database status."""
    show_database_info(database_config_file, database_name)


def _add_command_alias(command, name, hidden):
    new_command = copy.deepcopy(command)
    new_command.hidden = hidden
    database.add_command(new_command, name=name)


# core commands working on database
database.add_command(create)
database.add_command(delete)

# commands for database info
database.add_command(info)

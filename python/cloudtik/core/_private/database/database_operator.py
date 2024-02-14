import logging
import os
from typing import Any, Dict, Optional

from cloudtik.core._private.util.core_utils import get_cloudtik_temp_dir, get_json_object_hash
from cloudtik.core._private.util.schema_utils import DATABASE_SCHEMA_NAME, \
    DATABASE_SCHEMA_REFS, validate_schema_by_name
from cloudtik.core._private.utils import print_dict_info, \
    load_yaml_config, handle_cli_override, save_config_cache, load_config_from_cache, merge_config_hierarchy, \
    get_database_provider_of
from cloudtik.core._private.provider_factory import _PROVIDER_PRETTY_NAMES
from cloudtik.core._private.database_provider_factory import _DATABASE_PROVIDERS, _get_database_provider_cls
from cloudtik.core._private.cli_logger import cli_logger, cf


logger = logging.getLogger(__name__)

CONFIG_CACHE_VERSION = 1


def delete_database(
        config_file: str, yes: bool,
        override_database_name: Optional[str] = None,
        no_config_cache: bool = False):
    """Destroys the database."""
    config = _load_database_config(
        config_file, override_database_name,
        no_config_cache=no_config_cache)
    _delete_database(config, yes)


def _delete_database(
        config: Dict[str, Any],
        yes: bool = False):
    database_name = config["database_name"]
    provider = get_database_provider_of(config)
    database_info = provider.get_info(config)
    if not database_info:
        raise RuntimeError(
            f"Database with the name {database_name} doesn't exist!")
    else:
        cli_logger.confirm(
            yes, "Are you sure that you want to delete database {}?",
            config["database_name"], _abort=True)
        provider.delete(config)


def create_database(
        config_file: str, yes: bool,
        override_database_name: Optional[str] = None,
        no_config_cache: bool = False):
    """Creates a new database from a config json."""
    config = load_yaml_config(config_file)
    importer = _DATABASE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        cli_logger.abort(
            "Unknown provider type " + cf.bold("{}") + "\n"
            "Available providers are: {}", config["provider"]["type"],
            cli_logger.render_list([
                k for k in _DATABASE_PROVIDERS.keys()
                if _DATABASE_PROVIDERS[k] is not None
            ]))

    overrides = 0
    overrides += handle_cli_override(
        config, "database_name", override_database_name)
    if overrides:
        cli_logger.newline()

    cli_logger.labeled_value("Database", config["database_name"])
    cli_logger.newline()

    config = _bootstrap_database_config(
        config, no_config_cache=no_config_cache)
    _create_database(config, yes=yes)


def _create_database(
        config: Dict[str, Any], yes: bool = False):
    database_name = config["database_name"]
    provider = get_database_provider_of(config)
    database_info = provider.get_info(config)
    if database_info:
        raise RuntimeError(
            f"A database with the name {database_name} already exists!")
    else:
        cli_logger.confirm(
            yes, "Are you sure that you want to create database {}?",
            database_name, _abort=True)
        provider.create(config)


def get_database_info(
        config_file: str,
        override_database_name: Optional[str] = None):
    config = _load_database_config(config_file, override_database_name)
    return _get_database_info(config)


def _get_database_info(
        config: Dict[str, Any]):
    provider = get_database_provider_of(config)
    return provider.get_info(config)


def show_database_info(
        config_file: str,
        override_database_name: Optional[str] = None):
    config = _load_database_config(config_file, override_database_name)
    database_info = _get_database_info(config)
    if not database_info:
        cli_logger.print(
            "Database instance {} doesn't exist.", config["database_name"])
    else:
        print_dict_info(database_info)


def _bootstrap_database_config(
        config: Dict[str, Any],
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = prepare_database_config(config)
    # Note: delete database only need to contain database_name
    provider_cls = _get_database_provider_cls(config["provider"])

    config_hash = get_json_object_hash([config])
    config_cache_dir = os.path.join(get_cloudtik_temp_dir(), "configs")
    cache_key = os.path.join(
        config_cache_dir,
        "cloudtik-database-config-{}".format(config_hash))
    cached_config = load_config_from_cache(
        cache_key, CONFIG_CACHE_VERSION, no_config_cache)
    if cached_config is not None:
        return cached_config

    cli_logger.print(
        "Checking {} environment settings",
        _PROVIDER_PRETTY_NAMES.get(config["provider"]["type"]))

    try:
        validate_database_config(config)
    except (ModuleNotFoundError, ImportError):
        cli_logger.abort(
            "Not all dependencies were found. Please "
            "update your install command.")

    resolved_config = provider_cls.bootstrap_config(config)
    save_config_cache(
        resolved_config, cache_key,
        CONFIG_CACHE_VERSION, no_config_cache)
    return resolved_config


def _load_database_config(
        config_file: str,
        override_database_name: Optional[str] = None,
        should_bootstrap: bool = True,
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = load_yaml_config(config_file)
    if override_database_name is not None:
        config["database_name"] = override_database_name
    if should_bootstrap:
        config = _bootstrap_database_config(
            config, no_config_cache=no_config_cache)
    return config


def prepare_database_config(config: Dict[str, Any]) -> Dict[str, Any]:
    with_defaults = fill_with_database_defaults(config)
    return with_defaults


def fill_with_database_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    # Merge the config with user inheritance hierarchy and system defaults hierarchy
    merged_config = merge_config_hierarchy(
        config["provider"], config, False, "database-defaults")
    return merged_config


def validate_database_config(config: Dict[str, Any]) -> None:
    """Required Dicts indicate that no extra fields can be introduced."""
    if not isinstance(config, dict):
        raise ValueError(
            "Config {} is not a dictionary".format(config))

    validate_schema_by_name(
        config, DATABASE_SCHEMA_NAME, DATABASE_SCHEMA_REFS)
    provider = get_database_provider_of(config)
    provider.validate_config(config["provider"])

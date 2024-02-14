import logging
import os
from typing import Any, Dict, Optional

from cloudtik.core._private.util.core_utils import get_cloudtik_temp_dir, get_json_object_hash
from cloudtik.core._private.util.schema_utils import STORAGE_SCHEMA_NAME, STORAGE_SCHEMA_REFS, \
    validate_schema_by_name
from cloudtik.core._private.utils import print_dict_info, \
    load_yaml_config, handle_cli_override, save_config_cache, load_config_from_cache, merge_config_hierarchy, \
    get_storage_provider_of
from cloudtik.core._private.provider_factory import _PROVIDER_PRETTY_NAMES
from cloudtik.core._private.storage_provider_factory import _STORAGE_PROVIDERS, _get_storage_provider_cls
from cloudtik.core._private.cli_logger import cli_logger, cf


logger = logging.getLogger(__name__)

CONFIG_CACHE_VERSION = 1


def delete_storage(
        config_file: str, yes: bool,
        override_storage_name: Optional[str] = None,
        no_config_cache: bool = False):
    """Destroys the storage."""
    config = _load_storage_config(
        config_file, override_storage_name,
        no_config_cache=no_config_cache)
    _delete_storage(config, yes)


def _delete_storage(
        config: Dict[str, Any],
        yes: bool = False):
    storage_name = config["storage_name"]
    provider = get_storage_provider_of(config)
    storage_info = provider.get_info(config)
    if not storage_info:
        raise RuntimeError(
            f"Storage with the name {storage_name} doesn't exist!")
    else:
        cli_logger.confirm(
            yes, "Are you sure that you want to delete storage {}?",
            storage_name, _abort=True)
        provider.delete(config)


def create_storage(
        config_file: str, yes: bool,
        override_storage_name: Optional[str] = None,
        no_config_cache: bool = False):
    """Creates a new storage from a config json."""
    config = load_yaml_config(config_file)
    importer = _STORAGE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        cli_logger.abort(
            "Unknown provider type " + cf.bold("{}") + "\n"
            "Available providers are: {}", config["provider"]["type"],
            cli_logger.render_list([
                k for k in _STORAGE_PROVIDERS.keys()
                if _STORAGE_PROVIDERS[k] is not None
            ]))

    overrides = 0
    overrides += handle_cli_override(
        config, "storage_name", override_storage_name)
    if overrides:
        cli_logger.newline()

    cli_logger.labeled_value(
        "Storage", config["storage_name"])
    cli_logger.newline()

    config = _bootstrap_storage_config(
        config, no_config_cache=no_config_cache)
    _create_storage(config, yes=yes)


def _create_storage(
        config: Dict[str, Any], yes: bool = False):
    storage_name = config["storage_name"]
    provider = get_storage_provider_of(config)
    storage_info = provider.get_info(config)
    if storage_info:
        raise RuntimeError(
            f"A storage with the name {storage_name} already exists!")
    else:
        cli_logger.confirm(
            yes, "Are you sure that you want to create storage {}?",
            storage_name, _abort=True)
        provider.create(config)


def get_storage_info(
        config_file: str,
        override_storage_name: Optional[str] = None):
    config = _load_storage_config(config_file, override_storage_name)
    return _get_storage_info(config)


def _get_storage_info(
        config: Dict[str, Any]):
    provider = get_storage_provider_of(config)
    return provider.get_info(config)


def show_storage_info(
        config_file: str,
        override_storage_name: Optional[str] = None):
    config = _load_storage_config(config_file, override_storage_name)
    storage_info = _get_storage_info(config)
    if not storage_info:
        cli_logger.print(
            "Object storage {} doesn't exist.", config["storage_name"])
    else:
        print_dict_info(storage_info)


def _bootstrap_storage_config(
        config: Dict[str, Any],
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = prepare_storage_config(config)
    # Note: delete storage only need to contain storage_name
    provider_cls = _get_storage_provider_cls(config["provider"])

    config_hash = get_json_object_hash([config])
    config_cache_dir = os.path.join(get_cloudtik_temp_dir(), "configs")
    cache_key = os.path.join(
        config_cache_dir,
        "cloudtik-storage-config-{}".format(config_hash))
    cached_config = load_config_from_cache(
        cache_key, CONFIG_CACHE_VERSION, no_config_cache)
    if cached_config is not None:
        return cached_config

    cli_logger.print(
        "Checking {} environment settings",
        _PROVIDER_PRETTY_NAMES.get(config["provider"]["type"]))

    try:
        validate_storage_config(config)
    except (ModuleNotFoundError, ImportError):
        cli_logger.abort(
            "Not all dependencies were found. Please "
            "update your install command.")

    resolved_config = provider_cls.bootstrap_config(config)
    save_config_cache(
        resolved_config, cache_key,
        CONFIG_CACHE_VERSION, no_config_cache)
    return resolved_config


def _load_storage_config(
        config_file: str,
        override_storage_name: Optional[str] = None,
        should_bootstrap: bool = True,
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = load_yaml_config(config_file)
    if override_storage_name is not None:
        config["storage_name"] = override_storage_name
    if should_bootstrap:
        config = _bootstrap_storage_config(
            config, no_config_cache=no_config_cache)
    return config


def prepare_storage_config(config: Dict[str, Any]) -> Dict[str, Any]:
    with_defaults = fill_with_storage_defaults(config)
    return with_defaults


def fill_with_storage_defaults(
        config: Dict[str, Any]) -> Dict[str, Any]:
    # Merge the config with user inheritance hierarchy and system defaults hierarchy
    merged_config = merge_config_hierarchy(
        config["provider"], config, False, "storage-defaults")
    return merged_config


def validate_storage_config(config: Dict[str, Any]) -> None:
    """Required Dicts indicate that no extra fields can be introduced."""
    if not isinstance(config, dict):
        raise ValueError(
            "Config {} is not a dictionary".format(config))

    validate_schema_by_name(config, STORAGE_SCHEMA_NAME, STORAGE_SCHEMA_REFS)
    provider = get_storage_provider_of(config)
    provider.validate_config(config["provider"])

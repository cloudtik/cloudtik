import copy
import logging
import os
from typing import Any, Dict, Optional
import prettytable as pt

from cloudtik.core._private.cluster.cluster_operator import _get_cluster_info
from cloudtik.core._private.util.core_utils import get_cloudtik_temp_dir, get_json_object_hash
from cloudtik.core._private.util.schema_utils import WORKSPACE_SCHEMA_REFS, WORKSPACE_SCHEMA_NAME, \
    validate_schema_by_name
from cloudtik.core.tags import CLOUDTIK_TAG_NODE_STATUS
from cloudtik.core.workspace_provider import Existence, CLOUDTIK_MANAGED_CLOUD_STORAGE, \
    CLOUDTIK_MANAGED_CLOUD_STORAGE_URI, CLOUDTIK_MANAGED_CLOUD_DATABASE_ENDPOINT, \
    CLOUDTIK_MANAGED_CLOUD_DATABASE_PORT, CLOUDTIK_MANAGED_CLOUD_DATABASE_ENGINE, \
    CLOUDTIK_MANAGED_CLOUD_DATABASE_ADMIN_USER, CLOUDTIK_MANAGED_CLOUD_DATABASE_NAME
from cloudtik.core._private.utils import \
    is_managed_cloud_database, is_managed_cloud_storage, print_dict_info, \
    NODE_INFO_NODE_IP, handle_cli_override, load_yaml_config, save_config_cache, load_config_from_cache, \
    merge_config_hierarchy, get_workspace_provider_of, get_workspace_name
from cloudtik.core._private.provider_factory import _PROVIDER_PRETTY_NAMES, _get_node_provider_cls
from cloudtik.core._private.workspace_provider_factory import _WORKSPACE_PROVIDERS, _get_workspace_provider_cls
from cloudtik.core._private.cli_logger import cli_logger, cf

logger = logging.getLogger(__name__)

CONFIG_CACHE_VERSION = 1


def _get_existence_name(existence):
    if existence == Existence.NOT_EXIST:
        return "NOT EXIST"
    elif existence == Existence.STORAGE_ONLY:
        return "STORAGE ONLY"
    elif existence == Existence.DATABASE_ONLY:
        return "DATABASE ONLY"
    elif existence == Existence.STORAGE_AND_DATABASE_ONLY:
        return "STORAGE AND DATABASE ONLY"
    elif existence == Existence.IN_COMPLETED:
        return "NOT COMPLETED"
    else:
        return "COMPLETED"


def delete_workspace(
        config_file: str, yes: bool,
        override_workspace_name: Optional[str] = None,
        no_config_cache: bool = False,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False):
    """Destroys the workspace and associated Cloud resources."""
    config = _load_workspace_config(
        config_file, override_workspace_name,
        no_config_cache=no_config_cache)
    _delete_workspace(
        config, yes, delete_managed_storage, delete_managed_database)


def _delete_workspace(
        config: Dict[str, Any],
        yes: bool = False,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False):
    workspace_name = get_workspace_name(config)
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.NOT_EXIST:
        raise RuntimeError(
            f"Workspace with the name {workspace_name} doesn't exist!")
    else:
        if existence == Existence.COMPLETED:
            # Only check the running cluster when the workspace is at completed status
            running_clusters = provider.list_clusters(config)
            if running_clusters is not None and len(running_clusters) > 0:
                cluster_names = ",".join(list(running_clusters.keys()))
                raise RuntimeError(
                    "Workspace {} has clusters ({}) in running. Please stop the clusters first.".format(
                        workspace_name, cluster_names
                    ))

        managed_cloud_storage = is_managed_cloud_storage(config)
        if managed_cloud_storage:
            if delete_managed_storage:
                cli_logger.warning(
                    "WARNING: The managed cloud storage associated with this workspace "
                    "and the data in it will all be deleted!")
            else:
                cli_logger.print(
                    cf.bold("The managed cloud storage associated with this workspace will not be deleted."))

        managed_cloud_database = is_managed_cloud_database(config)
        if managed_cloud_database:
            if delete_managed_database:
                cli_logger.warning(
                    "WARNING: The managed cloud database associated with this workspace "
                    "and the data in it will all be deleted!")
            else:
                # check whether there are managed database instances
                # cannot delete workspace if there is any
                managed_databases = provider.list_databases(config)
                if managed_databases is not None and len(managed_databases) > 0:
                    managed_database_names = ",".join(list(managed_databases.keys()))
                    raise RuntimeError(
                        "Workspace {} has managed databases ({}) in running. Cannot not delete workspace.".format(
                            workspace_name, managed_database_names
                        ))

        cli_logger.confirm(
            yes, "Are you sure that you want to delete workspace {}?",
            config["workspace_name"], _abort=True)
        provider.delete_workspace(
            config, delete_managed_storage, delete_managed_database)


def create_workspace(
        config_file: str, yes: bool,
        override_workspace_name: Optional[str] = None,
        no_config_cache: bool = False,
        delete_incomplete: bool = True):
    """Creates a new workspace from a config json."""
    config = load_yaml_config(config_file)
    importer = _WORKSPACE_PROVIDERS.get(config["provider"]["type"])
    if not importer:
        cli_logger.abort(
            "Unknown provider type " + cf.bold("{}") + "\n"
            "Available providers are: {}", config["provider"]["type"],
            cli_logger.render_list([
                k for k in _WORKSPACE_PROVIDERS.keys()
                if _WORKSPACE_PROVIDERS[k] is not None
            ]))

    overrides = 0
    overrides += handle_cli_override(
        config, "workspace_name", override_workspace_name)
    if overrides:
        cli_logger.newline()

    cli_logger.labeled_value("Workspace", config["workspace_name"])
    cli_logger.newline()

    config = _bootstrap_workspace_config(
        config,
        no_config_cache=no_config_cache)
    _create_workspace(
        config, yes=yes, delete_incomplete=delete_incomplete)


def _create_workspace(
        config: Dict[str, Any], yes: bool = False,
        delete_incomplete: bool = False):
    workspace_name = get_workspace_name(config)
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.COMPLETED:
        raise RuntimeError(
            f"A completed workspace with the name {workspace_name} already exists!")
    elif existence == Existence.IN_COMPLETED:
        if delete_incomplete:
            cli_logger.confirm(
                yes, "An incomplete workspace with the same name exists.\n"
                     "Do you want to delete and then create workspace {}?",
                config["workspace_name"], _abort=True)
            provider.delete_workspace(
                config, delete_managed_storage=False)
            cli_logger.newline()
            provider.create_workspace(config)
        else:
            raise RuntimeError(
                f"A workspace with the name {workspace_name} already exists but not completed!")
    else:
        cli_logger.confirm(
            yes, "Are you sure that you want to create workspace {}?",
            config["workspace_name"], _abort=True)
        provider.create_workspace(config)


def update_workspace(
        config_file: str, yes: bool,
        override_workspace_name: Optional[str] = None,
        no_config_cache: bool = False,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False
):
    """Update the workspace from a config json."""
    config = _load_workspace_config(
        config_file, override_workspace_name,
        no_config_cache=no_config_cache)
    _update_workspace(
        config, yes,
        delete_managed_storage=delete_managed_storage,
        delete_managed_database=delete_managed_database
    )


def _update_workspace(
        config: Dict[str, Any],
        yes: bool = False,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False
):
    workspace_name = get_workspace_name(config)
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.NOT_EXIST:
        raise RuntimeError(
            f"Workspace with the name {workspace_name} doesn't exist!")
    else:
        # Only workspace in completed or in-completed status can be possibly updated.
        if existence != Existence.COMPLETED and existence != Existence.IN_COMPLETED:
            status_name = _get_existence_name(existence)
            raise RuntimeError(
                "Workspace {} in ({}) status cannot be updated.".format(
                    workspace_name, status_name
                ))

        cli_logger.confirm(
            yes, "Are you sure that you want to update workspace {}?",
            config["workspace_name"], _abort=True)
        provider.update_workspace(
            config, delete_managed_storage, delete_managed_database)


def list_workspace_clusters(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    """List clusters of the workspace name."""
    config = _load_workspace_config(config_file, override_workspace_name)
    clusters = _list_workspace_clusters(config)
    if clusters is None:
        cli_logger.error(
            "Workspace {} is not correctly configured.",
            config["workspace_name"])
    elif len(clusters) == 0:
        cli_logger.print(
            cf.bold("Workspace {} has no cluster in running."),
            config["workspace_name"])
    else:
        # Get cluster info by the cluster name
        clusters_info = _get_clusters_info(config, clusters)
        _show_clusters(clusters_info)


def _get_clusters_info(config: Dict[str, Any], clusters):
    clusters_info = []
    for cluster_name in clusters:
        cluster_info = {
            "cluster_name": cluster_name,
            "head_node": clusters[cluster_name]}

        # Retrieve other information through cluster operator
        # This is a trick that use the workspace config to act some part of cluster config
        # The provider implementation must be careful that this is working
        cluster_config = copy.deepcopy(config)
        cluster_config["cluster_name"] = cluster_name

        # Needs to do a provider bootstrap of the config for fill the missing configurations
        provider_cls = _get_node_provider_cls(cluster_config["provider"])
        cluster_config = provider_cls.bootstrap_config_for_api(cluster_config)

        info = _get_cluster_info(cluster_config, simple_config=True)
        cluster_info["total-workers"] = info.get("total-workers", 0)
        cluster_info["total-workers-ready"] = info.get("total-workers-ready", 0)
        cluster_info["total-workers-failed"] = info.get("total-workers-failed", 0)

        clusters_info.append(cluster_info)

    # sort cluster info based cluster name
    def cluster_info_sort(cluster_info):
        return cluster_info["cluster_name"]

    clusters_info.sort(key=cluster_info_sort)
    return clusters_info


def _show_clusters(clusters_info):
    tb = pt.PrettyTable()
    tb.field_names = [
        "cluster-name", "head-node-ip", "head-status", "head-public-ip",
        "total-workers", "workers-ready", "workers-failed"]
    for cluster_info in clusters_info:
        tb.add_row(
            [cluster_info["cluster_name"], cluster_info["head_node"][NODE_INFO_NODE_IP],
             cluster_info["head_node"][CLOUDTIK_TAG_NODE_STATUS], cluster_info["head_node"]["public_ip"],
             cluster_info["total-workers"], cluster_info["total-workers-ready"],
             cluster_info["total-workers-failed"]
             ])

    cli_logger.print(
        cf.bold("{} cluster(s) are running."), len(clusters_info))
    cli_logger.print(tb)


def _list_workspace_clusters(
        config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.NOT_EXIST:
        return None
    elif existence != Existence.COMPLETED:
        # show a warning here
        cli_logger.warning(
            "Workspace {} is not in completed status. Going forward may cause errors.",
            config["workspace_name"])

    return provider.list_clusters(config)


def list_workspace_storages(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    """List cloud storages created for the workspace name."""
    config = _load_workspace_config(config_file, override_workspace_name)
    storages = _list_workspace_storages(config)
    if storages is None:
        cli_logger.error(
            "Workspace {} is not correctly configured.",
            config["workspace_name"])
    elif len(storages) == 0:
        cli_logger.print(
            cf.bold("Workspace {} has no managed cloud storages."),
            config["workspace_name"])
    else:
        _show_storages(storages)


def _list_workspace_storages(
        config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.NOT_EXIST:
        return None
    elif existence != Existence.COMPLETED:
        # show a warning here
        cli_logger.warning(
            "Workspace {} is not in completed status. Going forward may cause errors.",
            config["workspace_name"])

    return provider.list_storages(config)


def _show_storages(storages):
    tb = pt.PrettyTable()
    tb.field_names = ["storage-name", "storage-uri"]
    for storage_name, storage_info in storages.items():
        tb.add_row([storage_name, storage_info[CLOUDTIK_MANAGED_CLOUD_STORAGE_URI]
                    ])

    cli_logger.print(
        cf.bold("{} object storage(s)."), len(storages))
    cli_logger.print(tb)


def list_workspace_databases(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    """List cloud databases created for the workspace name."""
    config = _load_workspace_config(config_file, override_workspace_name)
    databases = _list_workspace_databases(config)
    if databases is None:
        cli_logger.error(
            "Workspace {} is not correctly configured.",
            config["workspace_name"])
    elif len(databases) == 0:
        cli_logger.print(
            cf.bold("Workspace {} has no managed cloud databases."),
            config["workspace_name"])
    else:
        _show_databases(databases)


def _list_workspace_databases(
        config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider = get_workspace_provider_of(config)
    existence = provider.check_workspace_existence(config)
    if existence == Existence.NOT_EXIST:
        return None
    elif existence != Existence.COMPLETED:
        # show a warning here
        cli_logger.warning(
            "Workspace {} is not in completed status. Going forward may cause errors.",
            config["workspace_name"])

    return provider.list_databases(config)


def _show_databases(databases):
    tb = pt.PrettyTable()
    tb.field_names = [
        "instance-name", "engine", "host", "port", "admin-user"]
    for database_name, database_info in databases.items():
        instance_name = database_info.get(CLOUDTIK_MANAGED_CLOUD_DATABASE_NAME)
        if not instance_name:
            instance_name = database_name
        tb.add_row(
            [instance_name, database_info[CLOUDTIK_MANAGED_CLOUD_DATABASE_ENGINE],
             database_info[CLOUDTIK_MANAGED_CLOUD_DATABASE_ENDPOINT],
             database_info[CLOUDTIK_MANAGED_CLOUD_DATABASE_PORT],
             database_info.get(CLOUDTIK_MANAGED_CLOUD_DATABASE_ADMIN_USER, "-")
             ])

    cli_logger.print(
        cf.bold("{} database instance(s)."), len(databases))
    cli_logger.print(tb)


def show_status(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    """Show workspace status."""
    config = _load_workspace_config(config_file, override_workspace_name)
    workspace_name = get_workspace_name(config)
    existence = _get_workspace_status(config)
    existence_name = _get_existence_name(existence)
    cli_logger.labeled_value(
        f"Workspace {workspace_name}", existence_name)


def _get_workspace_status(config):
    provider = get_workspace_provider_of(config)
    return provider.check_workspace_existence(config)


def get_workspace_info(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    config = _load_workspace_config(config_file, override_workspace_name)
    return _get_workspace_info(config)


def _get_workspace_info(
        config: Dict[str, Any]):
    provider = get_workspace_provider_of(config)
    return provider.get_workspace_info(config)


def show_workspace_info(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    show_status(config_file, override_workspace_name)
    workspace_info = get_workspace_info(
        config_file, override_workspace_name)
    print_dict_info(workspace_info)


def show_managed_cloud_storage(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    workspace_info = get_workspace_info(config_file, override_workspace_name)
    managed_cloud_storage = workspace_info.get(CLOUDTIK_MANAGED_CLOUD_STORAGE)
    if managed_cloud_storage is not None:
        print_dict_info(managed_cloud_storage)


def show_managed_cloud_storage_uri(
        config_file: str,
        override_workspace_name: Optional[str] = None):
    workspace_info = get_workspace_info(config_file, override_workspace_name)
    managed_cloud_storage = workspace_info.get(CLOUDTIK_MANAGED_CLOUD_STORAGE)
    if managed_cloud_storage is not None:
        cli_logger.print(managed_cloud_storage[CLOUDTIK_MANAGED_CLOUD_STORAGE_URI])


def _bootstrap_workspace_config(
        config: Dict[str, Any],
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = prepare_workspace_config(config)
    # Note: delete workspace only need to contain workspace_name
    provider_cls = _get_workspace_provider_cls(config["provider"])

    config_hash = get_json_object_hash([config])
    config_cache_dir = os.path.join(get_cloudtik_temp_dir(), "configs")
    cache_key = os.path.join(
        config_cache_dir,
        "cloudtik-workspace-config-{}".format(config_hash))
    cached_config = load_config_from_cache(
        cache_key, CONFIG_CACHE_VERSION, no_config_cache)
    if cached_config is not None:
        return cached_config

    cli_logger.print(
        "Checking {} environment settings",
        _PROVIDER_PRETTY_NAMES.get(config["provider"]["type"]))

    try:
        validate_workspace_config(config)
    except (ModuleNotFoundError, ImportError):
        cli_logger.abort(
            "Not all dependencies were found. Please "
            "update your install command.")

    resolved_config = provider_cls.bootstrap_workspace_config(config)
    save_config_cache(
        resolved_config, cache_key,
        CONFIG_CACHE_VERSION, no_config_cache)
    return resolved_config


def _load_workspace_config(
        config_file: str,
        override_workspace_name: Optional[str] = None,
        should_bootstrap: bool = True,
        no_config_cache: bool = False) -> Dict[str, Any]:
    config = load_yaml_config(config_file)
    if override_workspace_name is not None:
        config["workspace_name"] = override_workspace_name
    if should_bootstrap:
        config = _bootstrap_workspace_config(
            config, no_config_cache=no_config_cache)
    return config


def prepare_workspace_config(config: Dict[str, Any]) -> Dict[str, Any]:
    with_defaults = fill_with_workspace_defaults(config)
    return with_defaults


def fill_with_workspace_defaults(
        config: Dict[str, Any]) -> Dict[str, Any]:
    # Merge the config with user inheritance hierarchy and system defaults hierarchy
    merged_config = merge_config_hierarchy(
        config["provider"], config, False, "workspace-defaults")
    return merged_config


def validate_workspace_config(config: Dict[str, Any]) -> None:
    """Required Dicts indicate that no extra fields can be introduced."""
    if not isinstance(config, dict):
        raise ValueError(
            "Config {} is not a dictionary".format(config))

    validate_schema_by_name(
        config, WORKSPACE_SCHEMA_NAME, WORKSPACE_SCHEMA_REFS)
    provider = get_workspace_provider_of(config)
    provider.validate_config(config["provider"])

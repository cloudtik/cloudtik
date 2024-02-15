import logging
import json
from typing import Any, Dict

from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.provider_factory import _get_provider_config_object, _import_external

logger = logging.getLogger(__name__)

# For caching database provider instantiations across API calls of one python session
_database_provider_instances = ConcurrentObjectCache()


def _import_aws_database(provider_config):
    from cloudtik.providers._private.aws.database_provider import AWSDatabaseProvider
    return AWSDatabaseProvider


def _import_gcp_database(provider_config):
    from cloudtik.providers._private.gcp.database_provider import GCPDatabaseProvider
    return GCPDatabaseProvider


def _import_azure_database(provider_config):
    from cloudtik.providers._private._azure.database_provider import AzureDatabaseProvider
    return AzureDatabaseProvider


def _import_kubernetes_database(provider_config):
    from cloudtik.providers._private._kubernetes.database_provider import \
        KubernetesDatabaseProvider
    return KubernetesDatabaseProvider


_DATABASE_PROVIDERS = {
    "aws": _import_aws_database,
    "gcp": _import_gcp_database,
    "azure": _import_azure_database,
    "kubernetes": _import_kubernetes_database,
    "external": _import_external  # Import an external module
}


def _get_database_provider_cls(provider_config: Dict[str, Any]):
    """Get the database provider class for a given provider config.

    Note that this may be used by private database providers that proxy methods to
    built-in database providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the database config.

    Returns:
        DatabaseProvider class
    """
    importer = _DATABASE_PROVIDERS.get(provider_config["type"])
    if importer is None:
        raise NotImplementedError(
            "Unsupported database provider: {}".format(
                provider_config["type"]))
    return importer(provider_config)


def _get_database_provider(
        provider_config: Dict[str, Any],
        workspace_name: str,
        database_name: str,
        use_cache: bool = True) -> Any:
    """Get the instantiated database provider for a given provider config.

    Note that this may be used by private database providers that proxy methods to
    built-in database providers, so we should maintain backwards compatibility.

    Args:
        provider_config: provider section of the cluster config.
        workspace_name: workspace name from the cluster config.
        database_name: database name from the cluster config.
        use_cache: whether or not to use a cached definition if available. If
            False, the returned object will also not be stored in the cache.

    Returns:
        DatabaseProvider
    """
    def load_database_provider(
            provider_config: Dict[str, Any], workspace_name: str, database_name: str):
        provider_cls = _get_database_provider_cls(provider_config)
        return provider_cls(provider_config, workspace_name, database_name)

    if not use_cache:
        return load_database_provider(
            provider_config, workspace_name, database_name)

    provider_key = (json.dumps(provider_config, sort_keys=True), workspace_name, database_name)
    return _database_provider_instances.get(
        provider_key, load_database_provider,
        provider_config=provider_config,
        workspace_name=workspace_name,
        database_name=database_name)


def _clear_database_provider_cache():
    _database_provider_instances.clear()


def _get_default_database_config(provider_config):
    return _get_provider_config_object(provider_config, "database-defaults")

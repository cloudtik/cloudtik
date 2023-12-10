import os
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_PRESTO


def configure_connectors(runtime_config: Dict[str, Any]):
    if runtime_config is None:
        return

    presto_config = runtime_config.get(BUILT_IN_RUNTIME_PRESTO)
    if presto_config is None:
        return

    catalogs = presto_config.get("catalogs")
    if catalogs is None:
        return

    for catalog in catalogs:
        catalog_config = catalogs[catalog]
        configure_connector(catalog, catalog_config)


def configure_connector(catalog: str, catalog_config: Dict[str, Any]):
    catalog_filename = f"{catalog}.properties"
    catalog_properties_file = os.path.join(
        os.getenv("PRESTO_HOME"), "etc/catalog", catalog_filename)

    mode = 'a' if os.path.exists(catalog_properties_file) else 'w'
    with open(catalog_properties_file, mode) as f:
        for key, value in catalog_config.items():
            f.write("{}={}\n".format(key, value))

import os

from cloudtik.core._private.util.runtime_utils import subscribe_runtime_config
from cloudtik.core._private.utils import \
    load_properties_file, save_properties_file
from cloudtik.runtime.kafka.utils import _get_server_config


def update_configurations():
    # Merge user specified configuration and default configuration
    runtime_config = subscribe_runtime_config()
    server_config = _get_server_config(runtime_config)
    if not server_config:
        return

    server_properties_file = os.path.join(
        os.getenv("KAFKA_HOME"), "config/server.properties")

    # Read in the existing configurations
    server_properties, comments = load_properties_file(
        server_properties_file)

    # Merge with the user configurations
    server_properties.update(server_config)

    # Write back the configuration file
    save_properties_file(
        server_properties_file, server_properties, comments=comments)

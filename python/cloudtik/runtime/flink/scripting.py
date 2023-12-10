import os

from cloudtik.core._private.utils import load_head_cluster_config, \
    load_properties_file, save_properties_file
from cloudtik.runtime.flink.utils import _get_flink_config


def update_flink_configurations():
    # Merge user specified configuration and default configuration
    config = load_head_cluster_config()
    flink_config = _get_flink_config(config)
    if not flink_config:
        return

    flink_conf_file = os.path.join(os.getenv("FLINK_HOME"), "conf/flink-conf.yaml")

    # Read in the existing configurations
    flink_conf, comments = load_properties_file(flink_conf_file, ':')

    # Merge with the user configurations
    flink_conf.update(flink_config)

    # Write back the configuration file
    save_properties_file(flink_conf_file, flink_conf, separator=': ', comments=comments)

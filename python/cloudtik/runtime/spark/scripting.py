import os

from cloudtik.core._private.utils import \
    load_head_cluster_config, \
    load_properties_file, save_properties_file
from cloudtik.runtime.spark.utils import _get_spark_config


def update_spark_configurations():
    # Merge user specified configuration and default configuration
    config = load_head_cluster_config()
    spark_config = _get_spark_config(config)
    if not spark_config:
        return

    spark_conf_file = os.path.join(
        os.getenv("SPARK_HOME"), "conf/spark-defaults.conf")

    # Read in the existing configurations
    spark_conf, comments = load_properties_file(spark_conf_file, ' ')

    # Merge with the user configurations
    spark_conf.update(spark_config)

    # Write back the configuration file
    save_properties_file(
        spark_conf_file, spark_conf, separator=' ', comments=comments)

#!/bin/bash

# Current bin directory
BIN_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
ROOT_DIR="$(dirname "$(dirname "$BIN_DIR")")"

args=$(getopt -a -o h:: -l head:: -- "$@")
eval set -- "${args}"

USER_HOME=/home/$(whoami)
RUNTIME_PATH=$USER_HOME/runtime

# Util functions
. "$ROOT_DIR"/common/scripts/util-functions.sh

prepare_base_conf() {
    OUTPUT_DIR=/tmp/spark/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_spark_installed() {
    if [ ! -n "${HADOOP_HOME}" ]; then
        echo "Hadoop is not installed."
        exit 1
    fi

    if [ ! -n "${SPARK_HOME}" ]; then
        echo "Spark is not installed."
        exit 1
    fi
}

set_resources_for_spark() {
    # For Head Node
    if [ "$IS_HEAD_NODE" == "true" ]; then
        local -r bootstrap_config="~/cloudtik_bootstrap_config.yaml"
        spark_executor_cores=$(cat "$bootstrap_config" | jq '."runtime"."spark"."spark_executor_resource"."spark_executor_cores"')
        spark_executor_memory=$(cat "$bootstrap_config" | jq '."runtime"."spark"."spark_executor_resource"."spark_executor_memory"')M
        spark_driver_memory=$(cat "$bootstrap_config" | jq '."runtime"."spark"."spark_executor_resource"."spark_driver_memory"')M
    fi
}

update_spark_credential_config_for_aws() {
    if [ "$AWS_WEB_IDENTITY" == "true" ]; then
        if [ ! -z "${AWS_ROLE_ARN}" ] && [ ! -z "${AWS_WEB_IDENTITY_TOKEN_FILE}" ]; then
            WEB_IDENTITY_ENVS="spark.yarn.appMasterEnv.AWS_ROLE_ARN ${AWS_ROLE_ARN}\nspark.yarn.appMasterEnv.AWS_WEB_IDENTITY_TOKEN_FILE ${AWS_WEB_IDENTITY_TOKEN_FILE}\nspark.executorEnv.AWS_ROLE_ARN ${AWS_ROLE_ARN}\nspark.executorEnv.AWS_WEB_IDENTITY_TOKEN_FILE ${AWS_WEB_IDENTITY_TOKEN_FILE}\n"
            sed -i "$ a ${WEB_IDENTITY_ENVS}" ${SPARK_DEFAULTS}
        fi
    fi
}

update_spark_credential_config() {
    # We need do some specific config for AWS kubernetes web identity environment variables
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        update_spark_credential_config_for_aws
    fi
}

update_spark_storage_dirs() {
    HADOOP_FS_DEFAULT_FILE=${HADOOP_HOME}/etc/hadoop/hadoop-fs-default
    if [ -f "${HADOOP_FS_DEFAULT_FILE}" ]; then
        . ${HADOOP_FS_DEFAULT_FILE}
    fi

    # event log dir
    if [ -z "${HADOOP_FS_DEFAULT}" ]; then
        event_log_dir="file:///tmp/spark-events"
        sql_warehouse_dir="$USER_HOME/shared/spark-warehouse"
    else
        event_log_dir="${HADOOP_FS_DEFAULT}/shared/spark-events"
        sql_warehouse_dir="${HADOOP_FS_DEFAULT}/shared/spark-warehouse"
    fi

    sed -i "s!{%spark.eventLog.dir%}!${event_log_dir}!g" ${SPARK_DEFAULTS}
    sed -i "s!{%spark.sql.warehouse.dir%}!${sql_warehouse_dir}!g" ${SPARK_DEFAULTS}
}

update_spark_runtime_config() {
    if [ "$IS_HEAD_NODE" == "true" ];then
        sed -i "s/{%spark.executor.cores%}/${spark_executor_cores}/g" ${SPARK_DEFAULTS}
        sed -i "s/{%spark.executor.memory%}/${spark_executor_memory}/g" ${SPARK_DEFAULTS}
        sed -i "s/{%spark.driver.memory%}/${spark_driver_memory}/g" ${SPARK_DEFAULTS}
    fi
}

update_spark_local_dir() {
    # set spark local dir
    spark_local_dir=$(get_data_disk_dirs)
    if [ -z "$spark_local_dir" ]; then
        spark_local_dir="/tmp"
    fi
    sed -i "s!{%spark.local.dir%}!${spark_local_dir}!g" ${SPARK_DEFAULTS}
}

update_metastore_config() {
    # To be improved for external metastore cluster
    if [ ! -z "$HIVE_METASTORE_URI" ] || [ "$METASTORE_ENABLED" == "true" ]; then
        if [ ! -z "$HIVE_METASTORE_URI" ]; then
            hive_metastore_uris="$HIVE_METASTORE_URI"
        else
            METASTORE_HOST=${HEAD_HOST_ADDRESS}
            hive_metastore_uris="thrift://${METASTORE_HOST}:9083"
        fi

        hive_metastore_version="3.1.2"

        if [ ! -n "${HIVE_HOME}" ]; then
            hive_metastore_jars=maven
        else
            hive_metastore_jars="${HIVE_HOME}/lib/*"
        fi

        sed -i "s!{%spark.hadoop.hive.metastore.uris%}!spark.hadoop.hive.metastore.uris ${hive_metastore_uris}!g" ${SPARK_DEFAULTS}
        sed -i "s!{%spark.sql.hive.metastore.version%}!spark.sql.hive.metastore.version ${hive_metastore_version}!g" ${SPARK_DEFAULTS}
        sed -i "s!{%spark.sql.hive.metastore.jars%}!spark.sql.hive.metastore.jars ${hive_metastore_jars}!g" ${SPARK_DEFAULTS}
    else
        # replace with empty
        sed -i "s/{%spark.hadoop.hive.metastore.uris%}//g" ${SPARK_DEFAULTS}
        sed -i "s/{%spark.sql.hive.metastore.version%}//g" ${SPARK_DEFAULTS}
        sed -i "s/{%spark.sql.hive.metastore.jars%}//g" ${SPARK_DEFAULTS}
    fi
}

configure_spark_shuffle() {
    # We assume other modifications to this list follow the same pattern:
    # Always add its value before the mapreduce_shuffle value.
    (! grep -Fq 'spark_shuffle,' ${HADOOP_HOME}/etc/hadoop/yarn-site.xml) \
      && sed -i "s#mapreduce_shuffle</value>#spark_shuffle,mapreduce_shuffle</value>#g" \
        ${HADOOP_HOME}/etc/hadoop/yarn-site.xml
}

configure_spark() {
    prepare_base_conf
    SPARK_DEFAULTS=${OUTPUT_DIR}/spark/spark-defaults.conf

    configure_spark_shuffle
    update_spark_runtime_config
    update_spark_local_dir
    update_spark_storage_dirs
    update_spark_credential_config

    if [ "$IS_HEAD_NODE" == "true" ];then
        update_metastore_config
        cp -r ${OUTPUT_DIR}/spark/* ${SPARK_HOME}/conf
    fi
}

configure_jupyter_for_spark() {
  if [ "$IS_HEAD_NODE" == "true" ]; then
      mkdir -p ${RUNTIME_PATH}/jupyter/logs

      echo Y | jupyter lab --generate-config;
      # Set default password(cloudtik) for JupyterLab
      sed -i  "1 ic.NotebookApp.password = 'argon2:\$argon2id\$v=19\$m=10240,t=10,p=8\$Y+sBd6UhAyKNsI+/mHsy9g\$WzJsUujSzmotUkblSTpMwCFoOBVSwm7S5oOPzpC+tz8'" \
        ~/.jupyter/jupyter_lab_config.py

      # Set default notebook_dir for JupyterLab
      export JUPYTER_WORKSPACE=${RUNTIME_PATH}/jupyter/notebooks
      mkdir -p $JUPYTER_WORKSPACE
      sed -i  "1 ic.NotebookApp.notebook_dir = '${JUPYTER_WORKSPACE}'" ~/.jupyter/jupyter_lab_config.py
      sed -i  "1 ic.NotebookApp.ip = '${NODE_IP_ADDRESS}'" ~/.jupyter/jupyter_lab_config.py
  fi
}

set_head_option "$@"
check_spark_installed
set_head_address
set_node_address
set_resources_for_spark
configure_spark
configure_jupyter_for_spark

exit 0

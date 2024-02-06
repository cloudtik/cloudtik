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
    OUTPUT_DIR=/tmp/presto/conf
    local source_dir=$(dirname "${BIN_DIR}")/conf
    rm -rf  ${OUTPUT_DIR}
    mkdir -p ${OUTPUT_DIR}
    cp -r $source_dir/* ${OUTPUT_DIR}
}

check_presto_installed() {
    if [ ! -n "${PRESTO_HOME}" ]; then
        echo "Presto is not installed."
        exit 1
    fi
}

retrieve_resources() {
    jvm_max_memory=$(awk -v total_physical_memory=$(cloudtik node resources --memory --in-mb) 'BEGIN{print 0.8 * total_physical_memory}')
    jvm_max_memory=${jvm_max_memory%.*}
    query_max_memory_per_node=$(echo $jvm_max_memory | awk '{print $1*0.5}')
    query_max_memory_per_node=${query_max_memory_per_node%.*}
    query_max_total_memory_per_node=$(echo $jvm_max_memory | awk '{print $1*0.7}')
    query_max_total_memory_per_node=${query_max_total_memory_per_node%.*}
    memory_heap_headroom_per_node=$(echo $jvm_max_memory | awk '{print $1*0.25}')
    memory_heap_headroom_per_node=${memory_heap_headroom_per_node%.*}
}

update_presto_data_disks_config() {
    local data_disk_dir=$(get_first_data_disk_dir)
    if [ -z "$data_disk_dir" ]; then
        presto_data_dir="${PRESTO_HOME}/data"
    else
        presto_data_dir="$data_disk_dir/presto/data"
    fi

    mkdir -p $presto_data_dir
    sed -i "s!{%node.data-dir%}!${presto_data_dir}!g" ${OUTPUT_DIR}/presto/node.properties
}

update_storage_config_for_aws() {
    # AWS_S3_ACCESS_KEY_ID
    # AWS_S3_SECRET_ACCESS_KEY
    # Since hive.s3.use-instance-credentials is default true
    if [ ! -z "$AWS_S3_ACCESS_KEY_ID" ]; then
        sed -i "s#{%s3.aws-access-key%}#${AWS_S3_ACCESS_KEY_ID}#g" $catalog_dir/hive.s3.properties
        sed -i "s#{%s3.aws-secret-key%}#${AWS_S3_SECRET_ACCESS_KEY}#g" $catalog_dir/hive.s3.properties
        cat $catalog_dir/hive.s3.properties >> $catalog_dir/hive.properties
    fi
}

update_credential_config_for_azure() {
    AZURE_ENDPOINT="blob"
    sed -i "s#{%azure.storage.account%}#${AZURE_STORAGE_ACCOUNT}#g" $catalog_dir/hive-azure-core-site.xml
    sed -i "s#{%storage.endpoint%}#${AZURE_ENDPOINT}#g" $catalog_dir/hive-azure-core-site.xml
    sed -i "s#{%azure.account.key%}#${AZURE_ACCOUNT_KEY}#g" $catalog_dir/hive-azure-core-site.xml
}

update_storage_config_for_azure() {
    if [ "$AZURE_STORAGE_TYPE" == "blob" ];then
        update_credential_config_for_azure

        HIVE_AZURE_CORE_SITE="${PRESTO_HOME}/etc/catalog/hive-azure-core-site.xml"
        cp $catalog_dir/hive-azure-core-site.xml ${HIVE_AZURE_CORE_SITE}
        sed -i "s!{%hive.config.resources%}!${HIVE_AZURE_CORE_SITE}!g" $catalog_dir/hive.config.properties
        cat $catalog_dir/hive.config.properties >> $catalog_dir/hive.properties
    else
        # datalake is not supported
        echo "WARNING: Azure Data Lake Storage Gen 2 is not supported for this version."
    fi
}

update_storage_config_for_gcp() {
    # GCP_PROJECT_ID
    # GCP_GCS_SERVICE_ACCOUNT_CLIENT_EMAIL
    # GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID
    # GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY
    if [ ! -z "$GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID" ]; then
        sed -i "s#{%project_id%}#${GCP_PROJECT_ID}#g" $catalog_dir/gcs.key-file.json
        sed -i "s#{%private_key_id%}#${GCP_GCS_SERVICE_ACCOUNT_CLIENT_EMAIL}#g" $catalog_dir/gcs.key-file.json
        sed -i "s#{%private_key%}#${GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID}#g" $catalog_dir/gcs.key-file.json
        sed -i "s#{%client_email%}#${GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY}#g" $catalog_dir/gcs.key-file.json

        cp $catalog_dir/gcs.key-file.json ${PRESTO_HOME}/etc/catalog/gcs.key-file.json

        sed -i "s#{%gcs.use-access-token%}#false#g" $catalog_dir/hive.gcs.properties
        sed -i "s!{%gcs.json-key-file-path%}!${PRESTO_HOME}/etc/catalog/gcs.key-file.json!g" $catalog_dir/hive.gcs.properties
    else
        sed -i "s#{%gcs.use-access-token%}#true#g" $catalog_dir/hive.gcs.properties
        sed -i "s#{%gcs.json-key-file-path%}##g" $catalog_dir/hive.gcs.properties
    fi

    cat $catalog_dir/hive.gcs.properties >> $catalog_dir/hive.properties
}

set_cloud_storage_provider() {
    cloud_storage_provider="none"
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="aws"
    elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="azure"
    elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="gcp"
    fi
}

update_storage_config() {
    set_cloud_storage_provider
    if [ "${cloud_storage_provider}" == "aws" ]; then
        update_storage_config_for_aws
    elif [ "${cloud_storage_provider}" == "azure" ]; then
        update_storage_config_for_azure
    elif [ "${cloud_storage_provider}" == "gcp" ]; then
        update_storage_config_for_gcp
    fi
}

update_hive_metastore_config() {
    # To be improved for external metastore cluster
    catalog_dir=${OUTPUT_DIR}/presto/catalog
    hive_properties=${catalog_dir}/hive.properties
    if [ ! -z "$HIVE_METASTORE_URI" ] || [ "$METASTORE_ENABLED" == "true" ]; then
        if [ ! -z "$HIVE_METASTORE_URI" ]; then
            hive_metastore_uri="$HIVE_METASTORE_URI"
        else
            METASTORE_HOST=${HEAD_HOST_ADDRESS}
            hive_metastore_uri="thrift://${METASTORE_HOST}:9083"
        fi

        sed -i "s!{%HIVE_METASTORE_URI%}!${hive_metastore_uri}!g" ${hive_properties}
        mkdir -p ${PRESTO_HOME}/etc/catalog
        update_storage_config
        cp ${hive_properties}  ${PRESTO_HOME}/etc/catalog/hive.properties
    fi
}

update_metastore_config() {
    update_hive_metastore_config
}

update_presto_memory_config() {
    if [ ! -z "$PRESTO_JVM_MAX_MEMORY" ]; then
        jvm_max_memory=$PRESTO_JVM_MAX_MEMORY
    fi
    if [ ! -z "$PRESTO_MAX_MEMORY_PER_NODE" ]; then
        query_max_memory_per_node=$PRESTO_MAX_MEMORY_PER_NODE
    fi
    if [ ! -z "$PRESTO_MAX_TOTAL_MEMORY_PER_NODE" ]; then
        query_max_total_memory_per_node=$PRESTO_MAX_TOTAL_MEMORY_PER_NODE
    fi

    if [ ! -z "$PRESTO_HEAP_HEADROOM_PER_NODE" ]; then
        memory_heap_headroom_per_node=$PRESTO_HEAP_HEADROOM_PER_NODE
    fi

    query_max_memory="50GB"
    if [ ! -z "$PRESTO_QUERY_MAX_MEMORY" ]; then
        query_max_memory=$PRESTO_QUERY_MAX_MEMORY
    fi

    sed -i "s/{%jvm.max-memory%}/${jvm_max_memory}m/g" `grep "{%jvm.max-memory%}" -rl ${OUTPUT_DIR}`
    sed -i "s/{%query.max-memory-per-node%}/${query_max_memory_per_node}MB/g" `grep "{%query.max-memory-per-node%}" -rl ${OUTPUT_DIR}`
    sed -i "s/{%query.max-total-memory-per-node%}/${query_max_total_memory_per_node}MB/g" `grep "{%query.max-total-memory-per-node%}" -rl ${OUTPUT_DIR}`
    sed -i "s/{%memory.heap-headroom-per-node%}/${memory_heap_headroom_per_node}MB/g" `grep "{%memory.heap-headroom-per-node%}" -rl ${OUTPUT_DIR}`

    sed -i "s/{%query.max-memory%}/${query_max_memory}/g" `grep "{%query.max-memory%}" -rl ${OUTPUT_DIR}`
}

configure_presto() {
    prepare_base_conf
    update_metastore_config

    node_id=$(uuid)

    sed -i "s/{%coordinator.host%}/${HEAD_HOST_ADDRESS}/g" `grep "{%coordinator.host%}" -rl ${OUTPUT_DIR}`
    sed -i "s/{%node.environment%}/presto/g" ${OUTPUT_DIR}/presto/node.properties
    sed -i "s/{%node.id%}/${node_id}/g" ${OUTPUT_DIR}/presto/node.properties

    update_presto_memory_config
    update_presto_data_disks_config

    mkdir -p ${PRESTO_HOME}/etc
    if [ "$IS_HEAD_NODE" == "true" ]; then
        cp ${OUTPUT_DIR}/presto/config.properties  ${PRESTO_HOME}/etc/config.properties
    else
        cp ${OUTPUT_DIR}/presto/config.worker.properties  ${PRESTO_HOME}/etc/config.properties
    fi

    cp ${OUTPUT_DIR}/presto/jvm.config  ${PRESTO_HOME}/etc/jvm.config
    cp ${OUTPUT_DIR}/presto/node.properties  ${PRESTO_HOME}/etc/node.properties
}

set_head_option "$@"
check_presto_installed
set_head_address
retrieve_resources
configure_presto

exit 0

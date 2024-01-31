#!/bin/bash

# assumptions for using the functions of this script:
# 1. cloud_storage_provider variable is set to the name of supported public providers. For example, "aws", "gcp" or "azure"
# 2. Credential values are exported through the environment variables through provider.with_environment_variables.
# 3. HADOOP_CORE_SITE is set to the core-site file to update.
# 4. HADOOP_HOME is set to the hadoop installation home.
# 5. HADOOP_CREDENTIAL_HOME and HADOOP_CREDENTIAL_NAME can be used to set a different credential path

update_hadoop_credential_property() {
    if [ "${HAS_HADOOP_CREDENTIAL}" == "true" ]; then
        HADOOP_CREDENTIAL_PROPERTY="<property>\n      <name>hadoop.security.credential.provider.path</name>\n      <value>jceks://file@${HADOOP_CREDENTIAL_FILE}</value>\n    </property>"
        sed -i "s#{%hadoop.credential.property%}#${HADOOP_CREDENTIAL_PROPERTY}#g" $HADOOP_CORE_SITE
    else
        sed -i "s#{%hadoop.credential.property%}#""#g" $HADOOP_CORE_SITE
    fi
}

update_credential_config_for_aws() {
    if [ "$AWS_WEB_IDENTITY" == "true" ]; then
        # Replace with InstanceProfileCredentialsProvider with WebIdentityTokenCredentialsProvider for Kubernetes
        sed -i "s#InstanceProfileCredentialsProvider#WebIdentityTokenCredentialsProvider#g" $HADOOP_CORE_SITE
    fi

    sed -i "s#{%fs.s3a.access.key%}#${AWS_S3_ACCESS_KEY_ID}#g" $HADOOP_CORE_SITE

    HAS_HADOOP_CREDENTIAL=false

    if [ ! -z "${AWS_S3_SECRET_ACCESS_KEY}" ]; then
        ${HADOOP_HOME}/bin/hadoop credential create fs.s3a.secret.key \
            -value ${AWS_S3_SECRET_ACCESS_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

update_credential_config_for_gcp() {
    sed -i "s#{%fs.gs.project.id%}#${GCP_PROJECT_ID}#g" $HADOOP_CORE_SITE

    sed -i "s#{%fs.gs.auth.service.account.email%}#${GCP_GCS_SERVICE_ACCOUNT_CLIENT_EMAIL}#g" $HADOOP_CORE_SITE
    sed -i "s#{%fs.gs.auth.service.account.private.key.id%}#${GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY_ID}#g" $HADOOP_CORE_SITE

    HAS_HADOOP_CREDENTIAL=false

    if [ ! -z "${GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY}" ]; then
        ${HADOOP_HOME}/bin/hadoop credential create fs.gs.auth.service.account.private.key \
            -value ${GCP_GCS_SERVICE_ACCOUNT_PRIVATE_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

update_credential_config_for_azure() {
    sed -i "s#{%azure.storage.account%}#${AZURE_STORAGE_ACCOUNT}#g" $HADOOP_CORE_SITE

    if [ "$AZURE_STORAGE_TYPE" == "blob" ];then
        AZURE_ENDPOINT="blob"
    else
        # Default to datalake
        AZURE_ENDPOINT="dfs"
    fi
    sed -i "s#{%storage.endpoint%}#${AZURE_ENDPOINT}#g" $HADOOP_CORE_SITE

    if [ "$AZURE_STORAGE_TYPE" != "blob" ];then
        # datalake
        if [ -n  "${AZURE_ACCOUNT_KEY}" ];then
            sed -i "s#{%auth.type%}#SharedKey#g" $HADOOP_CORE_SITE
        else
            sed -i "s#{%auth.type%}##g" $HADOOP_CORE_SITE
        fi
    fi

    HAS_HADOOP_CREDENTIAL=false

    if [ "$AZURE_WORKLOAD_IDENTITY" == "true" ]; then
        # Replace with MsiTokenProvider with WorkloadIdentityTokenProvider for Kubernetes
        sed -i "s#MsiTokenProvider#WorkloadIdentityTokenProvider#g" $HADOOP_CORE_SITE

        if [ ! -z "${AZURE_TENANT_ID}" ]; then
            # Update AZURE_MANAGED_IDENTITY_TENANT_ID from the projected AZURE_TENANT_ID env in pod
            export AZURE_MANAGED_IDENTITY_TENANT_ID=${AZURE_TENANT_ID}
        fi

        if [ ! -z "${AZURE_CLIENT_ID}" ]; then
            # Update AZURE_MANAGED_IDENTITY_CLIENT_ID from the projected AZURE_CLIENT_ID env in pod
            export AZURE_MANAGED_IDENTITY_CLIENT_ID=${AZURE_CLIENT_ID}
        fi

        if [ ! -z "${AZURE_AUTHORITY_HOST}" ]; then
            FS_KEY_NAME_AUTHORITY="fs.azure.account.oauth2.msi.authority"
            ${HADOOP_HOME}/bin/hadoop credential create ${FS_KEY_NAME_AUTHORITY} \
                -value ${AZURE_AUTHORITY_HOST} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
            HAS_HADOOP_CREDENTIAL=true
        fi

        if [ ! -z "${AZURE_FEDERATED_TOKEN_FILE}" ]; then
            FS_KEY_NAME_TOKEN_FILE="fs.azure.account.oauth2.token.file"
            ${HADOOP_HOME}/bin/hadoop credential create ${FS_KEY_NAME_TOKEN_FILE} \
                -value ${AZURE_FEDERATED_TOKEN_FILE} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
            HAS_HADOOP_CREDENTIAL=true
        fi
    fi

    if [ ! -z "${AZURE_ACCOUNT_KEY}" ]; then
        FS_KEY_NAME_ACCOUNT_KEY="fs.azure.account.key.${AZURE_STORAGE_ACCOUNT}.${AZURE_ENDPOINT}.core.windows.net"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_KEY_NAME_ACCOUNT_KEY} \
            -value ${AZURE_ACCOUNT_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    if [ ! -z "${AZURE_MANAGED_IDENTITY_TENANT_ID}" ]; then
        FS_KEY_NAME_TENANT_ID="fs.azure.account.oauth2.msi.tenant"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_KEY_NAME_TENANT_ID} \
            -value ${AZURE_MANAGED_IDENTITY_TENANT_ID} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    if [ ! -z "${AZURE_MANAGED_IDENTITY_CLIENT_ID}" ]; then
        FS_KEY_NAME_CLIENT_ID="fs.azure.account.oauth2.client.id"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_KEY_NAME_CLIENT_ID} \
            -value ${AZURE_MANAGED_IDENTITY_CLIENT_ID} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

update_credential_config_for_aliyun() {
    if [ ! -z "${ALIYUN_OSS_ACCESS_KEY_ID}" ] && [ ! -z "${ALIYUN_OSS_ACCESS_KEY_SECRET}" ]; then
        sed -i "s#{%fs.oss.credentials.provider%}##g" $HADOOP_CORE_SITE
    else
        sed -i "s#{%fs.oss.credentials.provider%}#org.apache.hadoop.fs.aliyun.oss.AliyunEcsRamRoleCredentialsProvider#g" $HADOOP_CORE_SITE
    fi

    HAS_HADOOP_CREDENTIAL=false

    if [ ! -z "${ALIYUN_OSS_ACCESS_KEY_ID}" ]; then
        FS_OSS_ACCESS_KEY_ID="fs.oss.accessKeyId"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_OSS_ACCESS_KEY_ID} \
            -value ${ALIYUN_OSS_ACCESS_KEY_ID} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    if [ ! -z "${ALIYUN_OSS_ACCESS_KEY_SECRET}" ]; then
        FS_OSS_ACCESS_KEY_SECRET="fs.oss.accessKeySecret"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_OSS_ACCESS_KEY_SECRET} \
            -value ${ALIYUN_OSS_ACCESS_KEY_SECRET} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    if [ ! -z "${ALIYUN_ECS_RAM_ROLE_NAME}" ]; then
        FS_OSS_ECS_RAM_ROLE_NAME="fs.oss.ecs.ramRoleName"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_OSS_ECS_RAM_ROLE_NAME} \
            -value ${ALIYUN_ECS_RAM_ROLE_NAME} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

update_credential_config_for_huaweicloud() {
    if [ ! -z "${HUAWEICLOUD_OBS_ACCESS_KEY}" ] && [ ! -z "${HUAWEICLOUD_OBS_SECRET_KEY}" ]; then
        sed -i "s#{%fs.obs.security.provider.property%}#""#g" $HADOOP_CORE_SITE
    else
        FS_OBS_SECURITY_PROVIDER_PROPERTY_FOR_ECS="<property>\n      <name>fs.obs.security.provider</name>\n      <value>com.obs.services.EcsObsCredentialsProvider</value>\n    </property>"
        sed -i "s#{%fs.obs.security.provider.property%}#${FS_OBS_SECURITY_PROVIDER_PROPERTY_FOR_ECS}#g" $HADOOP_CORE_SITE
    fi

    HAS_HADOOP_CREDENTIAL=false

    if [ ! -z "${HUAWEICLOUD_OBS_ACCESS_KEY}" ]; then
        FS_OBS_ACCESS_KEY="fs.obs.access.key"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_OBS_ACCESS_KEY} \
            -value ${HUAWEICLOUD_OBS_ACCESS_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    if [ ! -z "${HUAWEICLOUD_OBS_SECRET_KEY}" ]; then
        FS_OBS_SECRET_KEY="fs.obs.secret.key"
        ${HADOOP_HOME}/bin/hadoop credential create ${FS_OBS_SECRET_KEY} \
            -value ${HUAWEICLOUD_OBS_SECRET_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

update_credential_config_for_minio() {
    sed -i "s#{%fs.s3a.access.key%}#${MINIO_ACCESS_KEY}#g" $HADOOP_CORE_SITE

    HAS_HADOOP_CREDENTIAL=false
    if [ ! -z "${MINIO_SECRET_KEY}" ]; then
        ${HADOOP_HOME}/bin/hadoop credential create fs.s3a.secret.key \
            -value ${MINIO_SECRET_KEY} -provider ${HADOOP_CREDENTIAL_TMP_PROVIDER_PATH} > /dev/null
        HAS_HADOOP_CREDENTIAL=true
    fi

    update_hadoop_credential_property
}

set_cloud_storage_provider() {
    cloud_storage_provider="none"
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="aws"
    elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="azure"
    elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="gcp"
    elif [ "$ALIYUN_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="aliyun"
    elif [ "$HUAWEICLOUD_CLOUD_STORAGE" == "true" ]; then
        cloud_storage_provider="huaweicloud"
    fi
}

update_credential_config_for_provider() {
    if [ "${cloud_storage_provider}" == "aws" ]; then
        update_credential_config_for_aws
    elif [ "${cloud_storage_provider}" == "azure" ]; then
        update_credential_config_for_azure
    elif [ "${cloud_storage_provider}" == "gcp" ]; then
        update_credential_config_for_gcp
    elif [ "${cloud_storage_provider}" == "aliyun" ]; then
        update_credential_config_for_aliyun
    elif [ "${cloud_storage_provider}" == "huaweicloud" ]; then
        update_credential_config_for_huaweicloud
    fi
}

update_cloud_storage_credential_config() {
    if [ -z "${HADOOP_CREDENTIAL_HOME}" ]; then
        HADOOP_CREDENTIAL_HOME=${HADOOP_HOME}/etc/hadoop
    fi
    if [ -z "${HADOOP_CREDENTIAL_NAME}" ]; then
        HADOOP_CREDENTIAL_NAME=credential.jceks
    fi
    HADOOP_CREDENTIAL_FILE="${HADOOP_CREDENTIAL_HOME}/${HADOOP_CREDENTIAL_NAME}"
    HADOOP_CREDENTIAL_TMP_FILE="${OUTPUT_DIR}/${HADOOP_CREDENTIAL_NAME}"
    HADOOP_CREDENTIAL_TMP_PROVIDER_PATH="jceks://file@${HADOOP_CREDENTIAL_TMP_FILE}"

    # update hadoop credential config
    update_credential_config_for_provider

    if [ -f "$HADOOP_CREDENTIAL_TMP_FILE" ]; then
        cp ${HADOOP_CREDENTIAL_TMP_FILE} ${HADOOP_CREDENTIAL_FILE}
    fi
}

update_minio_storage_credential_config() {
    if [ -z "${HADOOP_CREDENTIAL_HOME}" ]; then
        HADOOP_CREDENTIAL_HOME=${HADOOP_HOME}/etc/hadoop
    fi
    if [ -z "${HADOOP_CREDENTIAL_NAME}" ]; then
        HADOOP_CREDENTIAL_NAME=credential.jceks
    fi
    HADOOP_CREDENTIAL_FILE="${HADOOP_CREDENTIAL_HOME}/${HADOOP_CREDENTIAL_NAME}"
    HADOOP_CREDENTIAL_TMP_FILE="${OUTPUT_DIR}/${HADOOP_CREDENTIAL_NAME}"
    HADOOP_CREDENTIAL_TMP_PROVIDER_PATH="jceks://file@${HADOOP_CREDENTIAL_TMP_FILE}"

    # update hadoop credential config
    update_credential_config_for_minio

    if [ -f "$HADOOP_CREDENTIAL_TMP_FILE" ]; then
        cp ${HADOOP_CREDENTIAL_TMP_FILE} ${HADOOP_CREDENTIAL_FILE}
    fi
}

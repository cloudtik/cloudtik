#!/bin/bash

# assumptions for using the functions of this script:
# Credential values are exported through the environment variables through provider.with_environment_variables.

export_credential_variable() {
    (! grep -Fxq "${EXPORT_VARIABLE}" ~/.bashrc) && echo "${EXPORT_VARIABLE}" >> ~/.bashrc
}

update_api_credential_for_aws() {
    :
}

update_api_credential_for_gcp() {
    :
}

update_api_credential_for_azure() {
    if [ ! -z "${AZURE_MANAGED_IDENTITY_CLIENT_ID}" ]; then
        echo "${AZURE_MANAGED_IDENTITY_CLIENT_ID}" > ~/azure_managed_identity.config
    fi
}

update_api_credential_for_aliyun() {
    :
}

update_api_credential_for_huaweicloud() {
    :
}

update_api_credential_for_provider() {
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        update_api_credential_for_aws
    elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
        update_api_credential_for_azure
    elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
        update_api_credential_for_gcp
    elif [ "$ALIYUN_CLOUD_STORAGE" == "true" ]; then
        update_api_credential_for_aliyun
    elif [ "$HUAWEICLOUD_CLOUD_STORAGE" == "true" ]; then
        update_api_credential_for_huaweicloud
    fi
}

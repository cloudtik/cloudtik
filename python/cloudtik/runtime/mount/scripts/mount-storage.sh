#!/bin/bash

# Assumptions for using the functions of this script:
# 1. HDFS_ENABLED, HDFS_NAMENODE_URI, $XXX_CLOUD_STORAGE variable is set correspondingly.
# 2. Credential values are exported through the environment variables through provider.with_environment_variables.
# 3. USER_HOME is set to the current user home.
# 4. For service functions, DEFAULT_FS_MOUNT_PATH is set to the target path for mounting.
#    For service functions for local hdfs, HEAD_HOST_ADDRESS is set.

# Cloud storage fuse mounts:
# 1. If cloud storage of provider configured:
#   The cloud storage of provider mounts to DEFAULT_FS_MOUNT_PATH
#   Any cluster local storage mounts to LOCAL_FS_MOUNT_PATH
# 2. If We are operating without cloud storage of provider:
#   a. If there is remote cluster storage, it will mount to DEFAULT_FS_MOUNT_PATH
#      Any cluster local storage mounts to LOCAL_FS_MOUNT_PATH
#   b. If there is no remote cluster storage
#      Any cluster local storage mounts to DEFAULT_FS_MOUNT_PATH

# Installing functions
install_hdfs_fuse() {
    if ! type fuse_dfs >/dev/null 2>&1; then
        arch=$(uname -m)
        sudo wget -q ${CLOUDTIK_DOWNLOADS}/hadoop/fuse_dfs-${HADOOP_VERSION}-${arch} \
          -O /usr/bin/fuse_dfs
        sudo wget -q ${CLOUDTIK_DOWNLOADS}/hadoop/fuse_dfs_wrapper-${HADOOP_VERSION}.sh \
          -O /usr/bin/fuse_dfs_wrapper.sh
        sudo chmod +x /usr/bin/fuse_dfs
        sudo chmod +x /usr/bin/fuse_dfs_wrapper.sh
    fi

    # nfs mount may needed
    which mount.nfs > /dev/null \
      || (sudo  apt-get -qq update -y > /dev/null; \
        sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install nfs-common -y > /dev/null)

    local HDFS_NFS_JAR=hadoop-hdfs-nfs-${HADOOP_VERSION}.jar
    local HDFS_NFS_JAR_FIX=${HADOOP_HOME}/fix/${HDFS_NFS_JAR}
    if [ ! -f "${HDFS_NFS_JAR_FIX}" ] \
      && [ -d "${HADOOP_HOME}/share/hadoop/hdfs" ]; then
        # install HDFS NFS fix if not installed
        mkdir -p ${HADOOP_HOME}/fix \
          && wget -q ${CLOUDTIK_DOWNLOADS}/hadoop/${HDFS_NFS_JAR} -O ${HDFS_NFS_JAR_FIX} \
          && cp ${HDFS_NFS_JAR_FIX} ${HADOOP_HOME}/share/hadoop/hdfs/${HDFS_NFS_JAR}
    fi
}

install_s3_fuse() {
    if ! type s3fs >/dev/null 2>&1; then
        echo "Installing S3 Fuse..."
        sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq s3fs -y > /dev/null
    fi
}

install_azure_blob_fuse() {
    if ! type blobfuse2 >/dev/null 2>&1; then
        echo "Installing Azure Blob Fuse..."
        wget -q -N https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb \
          && sudo dpkg -i packages-microsoft-prod.deb > /dev/null \
          && sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq libfuse3-dev fuse3 -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq blobfuse2 -y > /dev/null
    fi
}

install_gcs_fuse() {
    if ! type gcsfuse >/dev/null 2>&1; then
        echo "Installing GCS Fuse..."
        curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo tee /usr/share/keyrings/cloud.google.asc >/dev/null \
          && echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt gcsfuse-$(lsb_release -c -s) main" \
            | sudo tee /etc/apt/sources.list.d/gcsfuse.list >/dev/null \
          && sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq gcsfuse -y > /dev/null \
          && sudo rm -f /etc/apt/sources.list.d/gcsfuse.list
    fi
}

install_aliyun_oss_fuse() {
    if ! type ossfs >/dev/null 2>&1; then
        echo "Installing Aliyun OSS Fuse..."
        OSS_PACKAGE="ossfs_1.80.7_ubuntu20.04_amd64.deb"
        wget -q -N https://gosspublic.alicdn.com/ossfs/${OSS_PACKAGE} \
          && sudo apt-get -qq update -y > /dev/null \
          && sudo DEBIAN_FRONTEND=noninteractive apt-get install -qq gdebi-core -y > /dev/null \
          && sudo gdebi --q --n ${OSS_PACKAGE} > /dev/null
        rm -f ${OSS_PACKAGE}
    fi
}

install_cloud_storage_fs() {
    if [ "$CLOUDTIK_INSTALL_ALL" == "true" ]; then
        install_s3_fuse
        install_azure_blob_fuse
        install_gcs_fuse
        install_aliyun_oss_fuse
    else
        # cloud storage from provider
        if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
            install_s3_fuse
        elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
            install_azure_blob_fuse
        elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
            install_gcs_fuse
        elif [ "$ALIYUN_CLOUD_STORAGE" == "true" ]; then
            install_aliyun_oss_fuse
        fi
    fi
}

install_local_storage_fs() {
    # always install local storage fs
    install_hdfs_fuse
    install_s3_fuse
}

install_storage_fs() {
    install_cloud_storage_fs
    install_local_storage_fs
}

# Configuring functions
configure_fuse_options() {
    FUSE_CONF_FILE="/etc/fuse.conf"
    FIND_STR="^user_allow_other"
    if [ `grep -c "$FIND_STR" $FUSE_CONF_FILE` -eq '0' ];then
        sudo sed -i '$auser_allow_other' $FUSE_CONF_FILE
    fi
}

get_fuse_cache_path() {
  fuse_cache_dir=""
  if [ -d "/mnt/cloudtik" ]; then
      for data_disk in /mnt/cloudtik/*; do
          [ -d "$data_disk" ] || continue
          fuse_cache_dir=$data_disk
          break
      done
  fi

  if [ -z $fuse_cache_dir ]; then
      fuse_cache_dir="/tmp/.cache"
  fi
  echo $fuse_cache_dir
}

configure_hdfs_fs() {
    configure_fuse_options
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_local_hdfs_fs() {
    configure_fuse_options
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_minio() {
    # TODO: handle different credentials for local and remote service
    if [ -z "${MINIO_BUCKET}" ]; then
        echo "MINIO_BUCKET environment variable is not set."
        return
    fi

    echo "${MINIO_ACCESS_KEY}:${MINIO_SECRET_KEY}" > ${USER_HOME}/.passwd-s3fs-minio
    chmod 600 ${USER_HOME}/.passwd-s3fs-minio
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_minio_fs() {
    configure_minio
}

configure_s3_fs() {
    if [ -z "${AWS_S3_BUCKET}" ]; then
        echo "AWS_S3A_BUCKET environment variable is not set."
        return
    fi

    if [ ! -z "${AWS_S3_ACCESS_KEY_ID}" ] && [ ! -z "${AWS_S3_SECRET_ACCESS_KEY}" ]; then
        echo "${AWS_S3_ACCESS_KEY_ID}:${AWS_S3_SECRET_ACCESS_KEY}" > ${USER_HOME}/.passwd-s3fs
        chmod 600 ${USER_HOME}/.passwd-s3fs
    fi
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_azure_blob_fs() {
    if [ "$AZURE_STORAGE_TYPE" == "blob" ];then
        AZURE_ENDPOINT="blob"
        BLOBFUSE_STORAGE_TYPE="block"
    else
        # Default to datalake
        AZURE_ENDPOINT="dfs"
        BLOBFUSE_STORAGE_TYPE="adls"
    fi

    if [ -z "${AZURE_CONTAINER}" ]; then
        echo "AZURE_CONTAINER environment variable is not set."
        return
    fi

    if [ -n "$AZURE_MANAGED_IDENTITY_CLIENT_ID" ]; then
        AUTH_TYPE="msi"
        AUTH_KEY_NAME="appid"
        AUTH_VALUE=$AZURE_MANAGED_IDENTITY_CLIENT_ID
    elif [ -n "$AZURE_ACCOUNT_KEY" ]; then
        AUTH_TYPE="key"
        AUTH_KEY_NAME="account-key"
        AUTH_VALUE=$AZURE_ACCOUNT_KEY
    else
        echo "AZURE_MANAGED_IDENTITY_CLIENT_ID or AZURE_ACCOUNT_KEY environment variable is not set."
        return
    fi

    if [ -z "${AZURE_STORAGE_ACCOUNT}" ]; then
        echo "AZURE_STORAGE_ACCOUNT environment variable is not set."
        return
    fi

    BLOBFUSE_FILE_CACHE_PATH="$(get_fuse_cache_path)/blobfuse2"
    sudo mkdir -p ${BLOBFUSE_FILE_CACHE_PATH}
    sudo chown $(whoami) ${BLOBFUSE_FILE_CACHE_PATH}

    fuse_connection_cfg=${USER_HOME}/blobfuse2_config.yaml
    cat>${fuse_connection_cfg}<<EOF
allow-other: true
logging:
    type: syslog
libfuse:
    attribute-expiration-sec: 240
    entry-expiration-sec: 240
    negative-entry-expiration-sec: 120
file_cache:
    path: ${BLOBFUSE_FILE_CACHE_PATH}
attr_cache:
  timeout-sec: 7200
azstorage:
    type: ${BLOBFUSE_STORAGE_TYPE}
    account-name: ${AZURE_STORAGE_ACCOUNT}
    ${AUTH_KEY_NAME}: ${AUTH_VALUE}
    endpoint: https://${AZURE_STORAGE_ACCOUNT}.${AZURE_ENDPOINT}.core.windows.net
    mode: ${AUTH_TYPE}
    container: ${AZURE_CONTAINER}
    update-md5: false
    validate-md5: false
    virtual-directory: true
components:
    - libfuse
    - file_cache
    - attr_cache
    - azstorage
EOF
    chmod 600 ${fuse_connection_cfg}
    configure_fuse_options
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_gcs_fs() {
    if [ -z "${GCP_GCS_BUCKET}" ]; then
        echo "GCP_GCS_BUCKET environment variable is not set."
        return
    fi
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_aliyun_oss_fs() {
    if [ -z "${ALIYUN_OSS_BUCKET}" ]; then
        echo "ALIYUN_OSS_BUCKET environment variable is not set."
        return
    fi

    if [ ! -z "${ALIYUN_OSS_ACCESS_KEY_ID}" ] && [ ! -z "${ALIYUN_OSS_ACCESS_KEY_SECRET}" ]; then
        echo "${ALIYUN_OSS_BUCKET}:${ALIYUN_OSS_ACCESS_KEY_ID}:${ALIYUN_OSS_ACCESS_KEY_SECRET}" > ${USER_HOME}/.passwd-ossfs
        chmod 600 ${USER_HOME}/.passwd-ossfs
    fi
    CONFIGURED_FOR_DEFAULT_FS=true
}

configure_cloud_storage_fs() {
    # cloud storage from provider
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        configure_s3_fs
    elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
        configure_azure_blob_fs
    elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
        configure_gcs_fs
    elif [ "$ALIYUN_CLOUD_STORAGE" == "true" ]; then
        configure_aliyun_oss_fs
    fi
}

configure_local_storage_fs() {
    # In cluster local MinIO storage is not supported because
    # it is not ready to use during head node starting process.

    # cluster local storage
    if [ "${CONFIGURED_FOR_DEFAULT_FS}" != "true" ]; then
        # Two of them will be configured for default and local
        # Only the needed URI or local runtime will be set

        # cluster local storage from remote cluster
        if [ ! -z "${HDFS_NAMENODE_URI}" ]; then
            configure_hdfs_fs
        elif [ ! -z "${MINIO_ENDPOINT_URI}" ]; then
            configure_minio_fs
        fi

        # cluster local storage from local cluster
        if [ "$HDFS_ENABLED" == "true" ]; then
            configure_local_hdfs_fs
        fi
    else
        # default fs already be configured, only one of them will be configured for local
        if [ ! -z "${HDFS_NAMENODE_URI}" ]; then
            configure_hdfs_fs
        elif [ ! -z "${MINIO_ENDPOINT_URI}" ]; then
            configure_minio_fs
        elif [ "$HDFS_ENABLED" == "true" ]; then
            configure_local_hdfs_fs
        fi
    fi
}

configure_storage_fs() {
    sudo mkdir -p /cloudtik
    sudo chown $(whoami) /cloudtik

    CONFIGURED_FOR_DEFAULT_FS=false
    configure_cloud_storage_fs
    configure_local_storage_fs
}

# Service functions

mount_hdfs_fs() {
    fs_default_dir="${HDFS_NAMENODE_URI:1}"
    if [ -z "${MOUNTED_DEFAULT_FS}" ]; then
        FS_MOUNT_PATH=${DEFAULT_FS_MOUNT_PATH}
        MOUNTED_DEFAULT_FS=${FS_MOUNT_PATH}
    else
        FS_MOUNT_PATH=${LOCAL_FS_MOUNT_PATH}
    fi
    # Mount remote hdfs here
    mkdir -p ${FS_MOUNT_PATH}

    # only one NFS Gateway per node is supported
    if [ "${HDFS_NFS_MOUNTED}" != "true" ] && [ "${HDFS_MOUNT_METHOD}" == "nfs" ]; then
        HDFS_NFS_MOUNTED=true

        # Use the remote HDFS dedicated core-site.xml and hdfs-site.xml
        REMOTE_HDFS_CONF_DIR=${HADOOP_HOME}/etc/remote
        if [ -d "${REMOTE_HDFS_CONF_DIR}" ]; then
            export HADOOP_CONF_DIR=${REMOTE_HDFS_CONF_DIR}
        fi

        echo "Staring HDFS NFS Gateway..."
        sudo -E $HADOOP_HOME/bin/hdfs --daemon start portmap
        $HADOOP_HOME/bin/hdfs --daemon start nfs3
        sleep 3

        echo "Mounting HDFS ${fs_default_dir} with NFS Gateway ${CLOUDTIK_NODE_IP} to ${FS_MOUNT_PATH}..."
        sudo mount -t nfs -o vers=3,proto=tcp,nolock,noacl,sync ${CLOUDTIK_NODE_IP}:/ ${FS_MOUNT_PATH}
    else
        echo "Mounting HDFS ${fs_default_dir} with fuse to ${FS_MOUNT_PATH}..."
        fuse_dfs_wrapper.sh -oinitchecks ${fs_default_dir} ${FS_MOUNT_PATH} > /dev/null
    fi
}

mount_local_hdfs_fs() {
    fs_default_dir="dfs://${HEAD_HOST_ADDRESS}:${HDFS_SERVICE_PORT}"
    if [ -z "${MOUNTED_DEFAULT_FS}" ]; then
        FS_MOUNT_PATH=${DEFAULT_FS_MOUNT_PATH}
        MOUNTED_DEFAULT_FS=${FS_MOUNT_PATH}
    else
        FS_MOUNT_PATH=${LOCAL_FS_MOUNT_PATH}
    fi
    # Mount local hdfs here
    mkdir -p ${FS_MOUNT_PATH}

    # only one NFS Gateway per node is supported
    if [ "${HDFS_NFS_MOUNTED}" != "true" ] && [ "${HDFS_MOUNT_METHOD}" == "nfs" ]; then
        HDFS_NFS_MOUNTED=true

        # Use the local HDFS dedicated core-site.xml and hdfs-site.xml if exists
        # if it is not exists, it will use the default Hadoop conf
        LOCAL_HDFS_CONF_DIR=${HADOOP_HOME}/etc/local
        if [ -d "${LOCAL_HDFS_CONF_DIR}" ]; then
            export HADOOP_CONF_DIR=${LOCAL_HDFS_CONF_DIR}
        fi

        echo "Staring HDFS NFS Gateway..."
        # Please note that portmap needs to run with root privilege
        sudo -E ${HADOOP_HOME}/bin/hdfs --daemon start portmap
        ${HADOOP_HOME}/bin/hdfs --daemon start nfs3
        sleep 3

        echo "Mounting HDFS ${fs_default_dir} with NFS Gateway ${CLOUDTIK_NODE_IP} to ${FS_MOUNT_PATH}..."
        sudo mount -t nfs -o vers=3,proto=tcp,nolock,noacl,sync ${CLOUDTIK_NODE_IP}:/ ${FS_MOUNT_PATH}
    else
        echo "Mounting HDFS ${fs_default_dir} with fuse to ${FS_MOUNT_PATH}..."
        fuse_dfs_wrapper.sh -oinitchecks ${fs_default_dir} ${FS_MOUNT_PATH} > /dev/null
    fi
}

mount_minio() {
    local endpoint_url=$1
    if [ -z "${MINIO_BUCKET}" ]; then
        echo "MINIO_BUCKET environment variable is not set."
        return
    fi

    if [ -z "${MOUNTED_DEFAULT_FS}" ]; then
        FS_MOUNT_PATH=${DEFAULT_FS_MOUNT_PATH}
        MOUNTED_DEFAULT_FS=${FS_MOUNT_PATH}
    else
        FS_MOUNT_PATH=${LOCAL_FS_MOUNT_PATH}
    fi
    # Mount remote MinIO here
    mkdir -p ${FS_MOUNT_PATH}

    echo "Mounting MinIO bucket ${MINIO_BUCKET} to ${FS_MOUNT_PATH}..."
    s3fs ${MINIO_BUCKET} ${FS_MOUNT_PATH} -o mp_umask=002 \
        -o passwd_file=~/.passwd-s3fs-minio \
        -o use_path_request_style \
        -o url=${endpoint_url} > /dev/null
}

mount_minio_fs() {
    mount_minio $MINIO_ENDPOINT_URI
}

mount_s3_fs() {
    if [ -z "${AWS_S3_BUCKET}" ]; then
        echo "AWS_S3_BUCKET environment variable is not set."
        return
    fi

    IAM_FLAG=""
    if [ -z "${AWS_S3_ACCESS_KEY_ID}" ] || [ -z "${AWS_S3_SECRET_ACCESS_KEY}" ]; then
        IAM_FLAG="-o iam_role=auto"
    fi

    mkdir -p ${DEFAULT_FS_MOUNT_PATH}
    echo "Mounting S3 bucket ${AWS_S3_BUCKET} to ${DEFAULT_FS_MOUNT_PATH}..."
    s3fs ${AWS_S3_BUCKET} -o use_cache=/tmp -o mp_umask=002 -o multireq_max=5 ${IAM_FLAG} ${DEFAULT_FS_MOUNT_PATH} > /dev/null
    MOUNTED_DEFAULT_FS=${DEFAULT_FS_MOUNT_PATH}
}

mount_azure_blob_fs() {
    if [ -z "${AZURE_CONTAINER}" ]; then
        echo "AZURE_CONTAINER environment variable is not set."
        return
    fi

    if [ -z "${AZURE_MANAGED_IDENTITY_CLIENT_ID}" ]; then
        echo "AZURE_MANAGED_IDENTITY_CLIENT_ID environment variable is not set."
        return
    fi

    if [ -z "${AZURE_STORAGE_ACCOUNT}" ]; then
        echo "AZURE_STORAGE_ACCOUNT environment variable is not set."
        return
    fi

    mkdir -p ${DEFAULT_FS_MOUNT_PATH}
    echo "Mounting Azure blob container ${AZURE_CONTAINER}@${AZURE_STORAGE_ACCOUNT} to ${DEFAULT_FS_MOUNT_PATH}..."
    blobfuse2 mount ${DEFAULT_FS_MOUNT_PATH} --config-file=${USER_HOME}/blobfuse2_config.yaml > /dev/null
    MOUNTED_DEFAULT_FS=${DEFAULT_FS_MOUNT_PATH}
}

mount_gcs_fs() {
    if [ ! -n "${GCP_GCS_BUCKET}" ]; then
        echo "GCP_GCS_BUCKET environment variable is not set."
        return
    fi

    mkdir -p ${DEFAULT_FS_MOUNT_PATH}
    echo "Mounting GCS bucket ${GCP_GCS_BUCKET} to ${DEFAULT_FS_MOUNT_PATH}..."
    gcsfuse ${GCP_GCS_BUCKET} ${DEFAULT_FS_MOUNT_PATH} > /dev/null
    MOUNTED_DEFAULT_FS=${DEFAULT_FS_MOUNT_PATH}
}

mount_aliyun_oss_fs() {
    if [ -z "${ALIYUN_OSS_BUCKET}" ]; then
        echo "ALIYUN_OSS_BUCKET environment variable is not set."
        return
    fi

    if [ -z "${ALIYUN_OSS_INTERNAL_ENDPOINT}" ]; then
        echo "ALIYUN_OSS_INTERNAL_ENDPOINT environment variable is not set."
        return
    fi

    PASSWD_FILE_FLAG=""
    RAM_ROLE_FLAG=""
    if [ ! -z "${ALIYUN_OSS_ACCESS_KEY_ID}" ] && [ ! -z "${ALIYUN_OSS_ACCESS_KEY_SECRET}" ]; then
        PASSWD_FILE_FLAG="-o passwd_file=${USER_HOME}/.passwd-ossfs"
    else
        RAM_ROLE_FLAG="-o ram_role=http://100.100.100.200/latest/meta-data/ram/security-credentials/${ALIYUN_ECS_RAM_ROLE_NAME}"
    fi

    mkdir -p ${DEFAULT_FS_MOUNT_PATH}
    echo "Mounting Aliyun OSS bucket ${ALIYUN_OSS_BUCKET} to ${DEFAULT_FS_MOUNT_PATH}..."
    # TODO: Endpoint setup for ECS for network going internally (for example, oss-cn-hangzhou-internal.aliyuncs.com)
    ossfs ${ALIYUN_OSS_BUCKET} ${DEFAULT_FS_MOUNT_PATH} -o use_cache=/tmp -o mp_umask=002 \
      -o url=${ALIYUN_OSS_INTERNAL_ENDPOINT} ${PASSWD_FILE_FLAG} ${RAM_ROLE_FLAG} > /dev/null
    MOUNTED_DEFAULT_FS=${DEFAULT_FS_MOUNT_PATH}
}

mount_cloud_storage_fs() {
    # cloud storage from provider
    if [ "$AWS_CLOUD_STORAGE" == "true" ]; then
        mount_s3_fs
    elif [ "$AZURE_CLOUD_STORAGE" == "true" ]; then
        mount_azure_blob_fs
    elif [ "$GCP_CLOUD_STORAGE" == "true" ]; then
        mount_gcs_fs
    elif [ "$ALIYUN_CLOUD_STORAGE" == "true" ]; then
        mount_aliyun_oss_fs
    fi
}

mount_local_storage_fs() {
    # cluster local storage
    HDFS_NFS_MOUNTED=false
    if [ -z "${MOUNTED_DEFAULT_FS}" ]; then
        # Two of them will mounted to default and local
        # Only the needed URI or local runtime will be set

        # cluster local storage from remote cluster
        if [ ! -z "${HDFS_NAMENODE_URI}" ]; then
            mount_hdfs_fs
        elif [ ! -z "${MINIO_ENDPOINT_URI}" ]; then
            mount_minio_fs
        fi

        # cluster local storage from local cluster
        if [ "$HDFS_ENABLED" == "true" ]; then
            mount_local_hdfs_fs
        fi
    else
        # default fs already mounted, only one of them will mounted to local
        if [ ! -z "${HDFS_NAMENODE_URI}" ]; then
            mount_hdfs_fs
        elif [ ! -z "${MINIO_ENDPOINT_URI}" ]; then
            mount_minio_fs
        elif [ "$HDFS_ENABLED" == "true" ]; then
            mount_local_hdfs_fs
        fi
    fi
}

mount_storage_fs() {
    MOUNTED_DEFAULT_FS=""
    mount_cloud_storage_fs
    mount_local_storage_fs
}

unmount_fs() {
    local fs_mount_path="$1"
    if findmnt -o fstype -l -n ${fs_mount_path} >/dev/null 2>&1; then
        echo "Unmounting cloud fs at ${fs_mount_path}..."
        local fstype=$(findmnt -o fstype -l -n ${fs_mount_path})
        if [ "${fstype}" == "nfs" ]; then
            sudo umount -f ${fs_mount_path} > /dev/null

            # stopping the NFS gateway services
            ${HADOOP_HOME}/bin/hdfs --daemon stop nfs3
            # Please note that portmap needs to run with root privilege
            sudo -E ${HADOOP_HOME}/bin/hdfs --daemon stop portmap
        else
            fusermount -u ${fs_mount_path} > /dev/null
        fi
    fi
}

unmount_storage_fs() {
    # use findmnt to check the existence and type of the mount
    # if findmnt doesn't exist, install it
    which findmnt > /dev/null || (sudo  apt-get -qq update -y > /dev/null; \
      sudo DEBIAN_FRONTEND=noninteractive apt-get -qq install util-linux -y > /dev/null)

    if [ "${DEFAULT_FS_MOUNT_PATH}" != "" ]; then
        unmount_fs "${DEFAULT_FS_MOUNT_PATH}"
    fi

    if [ "${LOCAL_FS_MOUNT_PATH}" != "" ]; then
        unmount_fs "${LOCAL_FS_MOUNT_PATH}"
    fi
}

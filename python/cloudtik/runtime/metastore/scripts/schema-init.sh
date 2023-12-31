#!/bin/bash

init_or_upgrade_schema() {
    METASTORE_SCHEMA_OK=true
    ${METASTORE_HOME}/bin/schematool -validate \
        -dbType ${DATABASE_ENGINE} > ${METASTORE_HOME}/logs/configure.log 2>&1
    if [ $? != 0 ]; then
        # Either we need to initSchema or we need upgradeSchema
        echo "Trying to initialize the metastore schema..."
        ${METASTORE_HOME}/bin/schematool -initSchema \
            -dbType ${DATABASE_ENGINE} > ${METASTORE_HOME}/logs/configure.log 2>&1
        if [ $? != 0 ]; then
            # Failed to init the schema, it may already exists
            echo "Trying to upgrade the metastore schema..."
            ${METASTORE_HOME}/bin/schematool -upgradeSchema \
                -dbType ${DATABASE_ENGINE} > ${METASTORE_HOME}/logs/configure.log 2>&1
            if [ $? != 0 ]; then
                echo "Metastore schema initialization or upgrade failed."
                METASTORE_SCHEMA_OK=false
            else
                echo "Successfully upgraded the metastore schema."
            fi
        else
            echo "Successfully initialized the metastore schema."
        fi
    fi
}

init_schema() {
    if [ "${SQL_DATABASE}" != "true" ]; then
        echo "SQL database is not configured."
        exit 1
    fi

    DATABASE_NAME=hive_metastore
    DATABASE_ENGINE=${SQL_DATABASE_ENGINE}
    DATABASE_USER=${SQL_DATABASE_USERNAME}
    DATABASE_PASSWORD=${SQL_DATABASE_PASSWORD}

    if [ "${DATABASE_ENGINE}" == "postgres" ]; then
        # create database for postgresql if not exists
        echo "SELECT 'CREATE DATABASE ${DATABASE_NAME}' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DATABASE_NAME}')\gexec" | PGPASSWORD=${SQL_DATABASE_PASSWORD} \
            psql \
            --host=${SQL_DATABASE_HOST} \
            --port=${SQL_DATABASE_PORT} \
            --username=${SQL_DATABASE_USERNAME} > ${METASTORE_HOME}/logs/configure.log
    fi

    # validate and initialize the metastore database schema (can be done multiple times)
    init_or_upgrade_schema

    if [ "${METASTORE_SCHEMA_OK}" != "true" ]; then
        exit 1
    fi
}

import json
import os
from typing import Dict, Any

import cloudtik
from cloudtik.core._private.cli_logger import cli_logger

CLOUDTIK_SCHEMA_PATH = os.path.join(os.path.dirname(cloudtik.__file__), "schema")

WORKSPACE_SCHEMA_NAME = "workspace.json"
WORKSPACE_SCHEMA_REFS = [
    "cloud-credentials.json",
    "cloud-database.json"
]

CLUSTER_SCHEMA_NAME = "cluster.json"
CLUSTER_SCHEMA_REFS = [
    "cloud-credentials.json",
    "cloud-storage-client.json",
    "cloud-database-client.json",
    "runtime.json",
]

STORAGE_SCHEMA_NAME = "storage.json"
STORAGE_SCHEMA_REFS = [
    "cloud-credentials.json"
]

DATABASE_SCHEMA_NAME = "database.json"
DATABASE_SCHEMA_REFS = [
    "cloud-credentials.json",
    "cloud-database.json"
]


def validate_schema(
        config: Dict[str, Any], schema_file, schema_refs=None):
    with open(schema_file) as f:
        schema = json.load(f)

    try:
        import jsonschema
        from jsonschema import RefResolver
        from jsonschema.validators import validator_for
    except (ModuleNotFoundError, ImportError) as e:
        # Don't log a warning message here. Logging be handled by upstream.
        raise e from None

    schema_name = os.path.basename(schema_file)
    schema_store = {
        schema.get('$id', schema_name): schema,
    }

    # add schema refs to the schema store
    if schema_refs:
        for schema_ref in schema_refs:
            ref_name = os.path.basename(schema_ref)
            with open(schema_ref) as f:
                ref = json.load(f)
                schema_store[ref.get('$id', ref_name)] = ref

    resolver = RefResolver.from_schema(schema, store=schema_store)
    validator_class = validator_for(schema)
    validator = validator_class(schema, resolver=resolver)
    try:
        validator.validate(config)
    except jsonschema.ValidationError as e:
        # The validate method show very long message of the schema
        # and the instance data, we need show this only at verbose mode
        if cli_logger.verbosity > 0:
            raise e from None
        else:
            # For none verbose mode, show short message
            raise RuntimeError(
                "JSON schema validation error: {}.".format(e.message)) from None


def validate_schema_by_name(
        config: Dict[str, Any], schema_name, schema_ref_names=None):
    schema_path = get_schema_path(schema_name)
    schema_refs = get_schema_paths(schema_ref_names) if schema_ref_names else None
    validate_schema(config, schema_path, schema_refs)


def get_schema_path(schema_name):
    return os.path.join(CLOUDTIK_SCHEMA_PATH, schema_name)


def get_schema_paths(schema_names):
    return [get_schema_path(schema_name) for schema_name in schema_names]

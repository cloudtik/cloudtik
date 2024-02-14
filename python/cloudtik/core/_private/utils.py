import collections
import collections.abc
import copy
import subprocess
import uuid
from datetime import datetime
import logging
import hashlib
import json
import os
from typing import Any, Dict, Optional, Tuple, List, Union
import sys
import time
import math
import click
import ipaddr
import re
from concurrent.futures import ThreadPoolExecutor
from shlex import quote

import yaml

import cloudtik
from cloudtik.core._private import constants
from cloudtik.core._private.call_context import CallContext
from cloudtik.core._private.cli_logger import cli_logger, cf
from cloudtik.core._private.cluster.cluster_metrics import ClusterMetricsSummary
from cloudtik.core._private.concurrent_cache import ConcurrentObjectCache
from cloudtik.core._private.constants import CLOUDTIK_WHEELS, \
    CLOUDTIK_DEFAULT_MAX_WORKERS, CLOUDTIK_NODE_SSH_INTERVAL_S, CLOUDTIK_NODE_START_WAIT_S, MAX_PARALLEL_EXEC_NODES, \
    CLOUDTIK_CLUSTER_URI_TEMPLATE, CLOUDTIK_RUNTIME_NAME, CLOUDTIK_RUNTIME_ENV_NODE_IP, CLOUDTIK_RUNTIME_ENV_HEAD_IP, \
    PRIVACY_REPLACEMENT_TEMPLATE, PRIVACY_REPLACEMENT, CLOUDTIK_CONFIG_SECRET, \
    CLOUDTIK_ENCRYPTION_PREFIX, CLOUDTIK_RUNTIME_ENV_SECRETS, CLOUDTIK_BOOTSTRAP_CONFIG_FILE
from cloudtik.core._private.util.core_utils import load_class, double_quote, check_process_exists, \
    get_cloudtik_temp_dir, \
    get_config_for_update, get_json_object_md5, to_hex_string, from_hex_string, get_node_ip_address, split_list, \
    open_with_mode
from cloudtik.core._private.crypto import AESCipher
from cloudtik.core._private.debug import log_once
from cloudtik.core._private.runtime_factory import _get_runtime, _get_runtime_cls, DEFAULT_RUNTIMES, \
    BUILT_IN_RUNTIME_ALL, BUILT_IN_RUNTIME_NONE
from cloudtik.core._private.util.schema_utils import CLUSTER_SCHEMA_NAME, CLUSTER_SCHEMA_REFS, \
    validate_schema_by_name
from cloudtik.core.node_provider import NodeProvider
from cloudtik.core._private.provider_factory import _get_default_config, _get_node_provider, \
    _get_provider_config_object, _get_node_provider_cls
from cloudtik.core._private.workspace_provider_factory import _get_workspace_provider
from cloudtik.core._private.storage_provider_factory import _get_storage_provider
from cloudtik.core._private.database_provider_factory import _get_database_provider
from cloudtik.core._private.docker import validate_docker_config
from cloudtik.core.scaling_policy import ScalingState
from cloudtik.core.tags import CLOUDTIK_TAG_USER_NODE_TYPE, CLOUDTIK_TAG_NODE_STATUS, STATUS_UP_TO_DATE, \
    STATUS_UPDATE_FAILED, CLOUDTIK_TAG_NODE_KIND, NODE_KIND_HEAD, NODE_KIND_WORKER

REQUIRED, OPTIONAL = True, False

# Internal kv keys for storing debug status.
CLOUDTIK_CLUSTER_SCALING_ERROR = "__cluster_scaling_error"
CLOUDTIK_CLUSTER_SCALING_STATUS = "__cluster_scaling_status"

# Internal kv key for publish runtime config.
CLOUDTIK_CLUSTER_RUNTIME_CONFIG = "__cluster_runtime_config"
CLOUDTIK_CLUSTER_RUNTIME_CONFIG_NODE_TYPE = "__cluster_runtime_config_{}"
CLOUDTIK_CLUSTER_NODES_INFO_NODE_TYPE = "__cluster_nodes_info_{}"
CLOUDTIK_CLUSTER_VARIABLE = "__cluster_variable_{}"

PLACEMENT_GROUP_RESOURCE_BUNDLED_PATTERN = re.compile(
    r"(.+)_group_(\d+)_([0-9a-zA-Z]+)")
PLACEMENT_GROUP_RESOURCE_PATTERN = re.compile(r"(.+)_group_([0-9a-zA-Z]+)")

ResourceBundle = Dict[str, Union[int, float]]

COMMAND_KEYS = ["initialization_commands",
                "head_initialization_commands",
                "worker_initialization_commands",
                "setup_commands",
                "head_setup_commands",
                "worker_setup_commands",
                "bootstrap_commands",
                "start_commands",
                "head_start_commands",
                "worker_start_commands",
                "stop_commands",
                "head_stop_commands",
                "worker_stop_commands"]

NODE_TYPE_COMMAND_KEYS = ["worker_initialization_commands",
                          "worker_setup_commands",
                          "bootstrap_commands",
                          "worker_start_commands",
                          "worker_stop_commands"]

DOCKER_COMMAND_KEYS = [
                "initialization_commands",
                "head_initialization_commands",
                "worker_initialization_commands"]

TEMPORARY_COMMAND_KEYS = [
                "initialization_commands",
                "setup_commands",
                "start_commands",
                "bootstrap_commands",
                "stop_commands"]

MERGED_COMMAND_KEY = "merged_commands"

OPTIONS_CONFIG_KEY = "options"
RUNTIME_CONFIG_KEY = "runtime"
DOCKER_CONFIG_KEY = "docker"
AUTH_CONFIG_KEY = "auth"
FILE_MOUNTS_CONFIG_KEY = "file_mounts"
RUNTIME_TYPES_CONFIG_KEY = "types"
ENCRYPTION_KEY_CONFIG_KEY = "encryption.key"

PROVIDER_CREDENTIALS_CONFIG_KEY = "credentials"
PROVIDER_STORAGE_CONFIG_KEY = "storage"
PROVIDER_DATABASE_CONFIG_KEY = "database"


PRIVACY_CONFIG_KEYS = ["credentials", "secret", "password", ".key", "_key"]

NODE_INFO_NODE_ID = "node_id"
NODE_INFO_NODE_IP = "private_ip"
NODE_INFO_PUBLIC_IP = "public_ip"

# Prefix for the node id resource that is automatically added to each node.
# For example, a node may have id `node-172.23.42.1`.
NODE_ID_PREFIX = "node-"


logger = logging.getLogger(__name__)


class HeadNotRunningError(RuntimeError):
    pass


class HeadNotHealthyError(RuntimeError):
    def __init__(self, head_node, message):
        self.head_node = head_node
        super().__init__(message)


class ParallelTaskSkipped(RuntimeError):
    pass


def make_node_id(node_ip):
    return NODE_ID_PREFIX + node_ip


def round_memory_size_to_gb(memory_size: int) -> int:
    gb = int(memory_size / 1024)
    if gb < 1:
        gb = 1
    return gb * 1024


def run_system_command(cmd: str):
    result = os.system(cmd)
    if result != 0:
        raise RuntimeError(f"Error happened in running: {cmd}")


def with_script_args(cmds, script_args):
    if script_args:
        cmds += [double_quote(script_arg) for script_arg in list(script_args)]


def run_bash_scripts(script_path: str, command: str, script_args):
    cmds = [
        "bash",
        quote(script_path),
    ]
    if command:
        cmds += [command]
    with_script_args(cmds, script_args)
    final_cmd = " ".join(cmds)

    run_system_command(final_cmd)


def load_yaml_config(config_file):
    def handle_yaml_error(e):
        cli_logger.error(
            "YAML configuration invalid")
        cli_logger.newline()
        cli_logger.error(
            "Failed to load YAML file " + cf.bold("{}"),
            config_file)
        cli_logger.newline()
        with cli_logger.verbatim_error_ctx("PyYAML error:"):
            cli_logger.error(e)
        cli_logger.abort()

    try:
        with open(config_file) as f:
            config = yaml.safe_load(f.read())
    except FileNotFoundError:
        cli_logger.abort(
            "Configuration file ({}) does not exist",
            cf.bold(config_file))
    except yaml.parser.ParserError as e:
        handle_yaml_error(e)
        raise
    except yaml.scanner.ScannerError as e:
        handle_yaml_error(e)
        raise

    return config


def handle_cli_override(config, key, value):
    override = 0
    if value is not None:
        if key in config:
            cli_logger.warning(
                "`{}` override provided on the command line.\n"
                "  Using " + cf.bold("{}") + cf.dimmed(
                    " [configuration file has " + cf.bold("{}") + "]"),
                key, value, config[key])
            override = 1
        config[key] = value
    return override


def load_config_from_cache(
        cache_key, cache_version, no_config_cache=False):
    if os.path.exists(cache_key) and not no_config_cache:
        with open(cache_key) as f:
            config_cache = json.loads(f.read())
        if config_cache.get("_version", -1) == cache_version:
            cached_config = decrypt_config(config_cache["config"])
            if log_once(
                    "_printed_cached_config_warning"):
                cli_logger.verbose_warning(
                    "Loaded cached configuration "
                    "from " + cf.bold("{}"), cache_key)
                cli_logger.verbose_warning(
                    "If you experience issues with "
                    "the cloud provider, try re-running "
                    "the command with {}.", cf.bold("--no-config-cache"))
            return cached_config
        else:
            cli_logger.warning(
                "Found cached config "
                "but the version " + cf.bold("{}") + " "
                "(expected " + cf.bold("{}") + ") does not match.\n"
                "This is normal if the software was updated.\n"
                "Config will be re-resolved.",
                config_cache.get("_version", "none"), cache_version)
    return None


def save_config_cache(
        config, cache_key, cache_version, no_config_cache=False):
    if no_config_cache:
        return

    config_cache_dir = os.path.dirname(cache_key)
    os.makedirs(config_cache_dir, exist_ok=True)
    with open_with_mode(cache_key, "w", os_mode=0o600) as f:
        encrypted_config = encrypt_config(config)
        config_cache = {
            "_version": cache_version,
            "config": encrypted_config
        }
        f.write(json.dumps(config_cache))


def format_error_message(exception_message, task_exception=False):
    """Improve the formatting of an exception thrown by a remote function.

    This method takes a traceback from an exception and makes it nicer by
    removing a few uninformative lines and adding some space to indent the
    remaining lines nicely.

    Args:
        exception_message (str): A message generated by traceback.format_exc().
        task_exception (bool): Whether the exception occurs inside of task

    Returns:
        A string of the formatted exception message.
    """
    lines = exception_message.split("\n")
    if task_exception:
        # For errors that occur inside of tasks, remove lines 1 and 2 which are
        # always the same, they just contain information about the worker code.
        lines = lines[0:1] + lines[3:]
        pass
    return "\n".join(lines)


def publish_error(
        error_type,
        message,
        redis_client=None):
    """Push an error message to Redis.

    Args:
        error_type (str): The type of the error.
        message (str): The message that will be printed in the background
            on the driver.
        redis_client: The redis client to use.
    """
    # TODO : improve to the right format, current we simply use the string
    if redis_client:
        message = (f"ERROR: {time.time()}: {error_type}: \n"
                   f"{message}")
        redis_client.publish(
            "ERROR_INFO",
            message)
    else:
        raise ValueError(
            "redis_client needs to be specified!")


def run_in_parallel_on_nodes(
        run_exec,
        call_context: CallContext,
        nodes,
        max_workers=MAX_PARALLEL_EXEC_NODES) -> Tuple[int, int, int]:
    # This is to ensure that the parallel SSH calls below do not mess with
    # the users terminal.
    output_redir = call_context.is_output_redirected()
    call_context.set_output_redirected(True)
    allow_interactive = call_context.does_allow_interactive()
    call_context.set_allow_interactive(False)

    _cli_logger = call_context.cli_logger

    failures = 0
    skipped = 0
    with ThreadPoolExecutor(
            max_workers=max_workers) as executor:
        futures = {}
        for node_id in nodes:
            futures[node_id] = executor.submit(
                run_exec, node_id=node_id, call_context=call_context.new_call_context())

        for node_id, future in futures.items():
            try:
                result = future.result()
            except ParallelTaskSkipped as se:
                skipped += 1
                _cli_logger.warning(
                    "Task skipped on node {}: {}", node_id, str(se)),
            except Exception as e:
                failures += 1
                _cli_logger.error(
                    "Task failed on node {}: {}", node_id, str(e))

    call_context.set_output_redirected(output_redir)
    call_context.set_allow_interactive(allow_interactive)

    if failures > 1 or skipped > 1:
        _cli_logger.print(
            "Total {} tasks failed. Total {} tasks skipped.", failures, skipped)

    return len(nodes) - failures - skipped, failures, skipped


def validate_config(
        config: Dict[str, Any], skip_runtime_validate: bool = False) -> None:
    """Required Dicts indicate that no extra fields can be introduced."""
    if not isinstance(config, dict):
        raise ValueError(
            "Config {} is not a dictionary".format(config))

    validate_schema_by_name(config, CLUSTER_SCHEMA_NAME, CLUSTER_SCHEMA_REFS)

    # Detect out of date defaults. This happens when the cluster scaler that filled
    # out the default values is older than the version of the cluster scaler that
    # is running on the cluster.
    if "cluster_synced_files" not in config:
        raise RuntimeError(
            "Missing 'cluster_synced_files' field in the cluster "
            "configuration. ")

    if "available_node_types" in config:
        if "head_node_type" not in config:
            raise ValueError(
                "You must specify `head_node_type` if `available_node_types "
                "is set.")
        if config["head_node_type"] not in config["available_node_types"]:
            raise ValueError(
                "`head_node_type` must be one of `available_node_types`.")

        sum_min_workers = sum(
            config["available_node_types"][node_type].get("min_workers", 0)
            for node_type in config["available_node_types"])
        if sum_min_workers > config["max_workers"]:
            raise ValueError(
                "The specified global `max_workers` is smaller than the "
                "sum of `min_workers` of all the available node types.")

    provider_cls = _get_node_provider_cls(config["provider"])
    provider_cls.validate_config(config["provider"])

    if not skip_runtime_validate:
        # add runtime config validate and testing
        runtime_validate_config(
            config.get(RUNTIME_CONFIG_KEY), config)


def verify_config(
        config: Dict[str, Any], skip_runtime_verify: bool = False):
    """Verify the configurations. Usually verify may mean to involve slow process"""
    provider = get_node_provider_of(config)
    provider_config = get_provider_config(config)
    provider.verify_config(provider_config)

    if not skip_runtime_verify:
        # add runtime config validate and testing
        runtime_verify_config(config.get(RUNTIME_CONFIG_KEY), config)


def prepare_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    The returned config has the following properties:
    - Uses the multi-node-type cluster scaler configuration.
    - Merged with the appropriate defaults.yaml
    - Has a valid Docker configuration if provided.
    - Has max_worker set for each node type.
    """
    provider_cls = _get_node_provider_cls(config["provider"])
    config = provider_cls.prepare_config(config)

    with_defaults = fill_with_defaults(config)
    merge_cluster_config(with_defaults)
    prepare_docker_config(with_defaults)
    set_node_type_min_max_workers(with_defaults)
    set_node_type_resources(with_defaults)
    return with_defaults


def prepare_docker_config(config: Dict[str, Any]):
    # Set docker is_gpu flag so that the right image can be used if not specified by user
    is_gpu = is_gpu_runtime(config)
    if is_gpu:
        if "docker" not in config:
            config["docker"] = {}
        docker_config = config["docker"]
        docker_config["is_gpu"] = True

    validate_docker_config(config)


def encrypt_config(config: Dict[str, Any]) -> Dict[str, Any]:
    encrypted_config = copy.deepcopy(config)
    cipher = get_config_cipher()
    process_config_with_privacy(
        encrypted_config, func=encrypt_config_value, param=cipher)
    return encrypted_config


def decrypt_config(config: Dict[str, Any]) -> Dict[str, Any]:
    decrypted_config = copy.deepcopy(config)
    cipher = get_config_cipher()
    process_config_with_privacy(
        decrypted_config, func=decrypt_config_value, param=cipher)
    return decrypted_config


def _get_user_template_file(template_name: str):
    if constants.CLOUDTIK_USER_TEMPLATES in os.environ:
        user_template_dirs_str = os.environ[constants.CLOUDTIK_USER_TEMPLATES]
        if user_template_dirs_str:
            user_template_dirs = [
                user_template_dir.strip() for user_template_dir in user_template_dirs_str.split(',')]
            for user_template_dir in user_template_dirs:
                template_file = os.path.join(user_template_dir, template_name)
                if os.path.exists(template_file):
                    return template_file

    return None


def _get_template_config(
        template_name: str, system: bool = False) -> Dict[str, Any]:
    """Load the template config"""
    import cloudtik as cloudtik_home

    # Append .yaml extension if the name doesn't include
    if not template_name.endswith(".yaml"):
        template_name += ".yaml"

    if system:
        # System templates
        template_file = os.path.join(
            os.path.dirname(cloudtik_home.__file__), "providers", template_name)
    else:
        # Check user templates
        template_file = _get_user_template_file(template_name)
        if not template_file:
            # Check built templates
            template_file = os.path.join(
                os.path.dirname(cloudtik_home.__file__), "templates", template_name)

    with open(template_file) as f:
        template_config = yaml.safe_load(f)

    return template_config


def merge_config(
        config: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    return update_nested_dict(config, updates)


def get_merged_base_config(
        provider, base_config_name: str,
        system: bool = False, object_name: str = None) -> Dict[str, Any]:
    template_config = _get_template_config(base_config_name, system=system)

    # if provider config exists, verify the provider.type are the same
    template_provider_type = template_config.get(
        "provider", {}).get("type", None)
    if template_provider_type and template_provider_type != provider["type"]:
        raise RuntimeError(
            "Template provider type ({}) doesn't match ({})!".format(
                template_provider_type, provider["type"]))

    merged_config = merge_config_hierarchy(
        provider, template_config,
        system=system, object_name=object_name)
    return merged_config


def get_merged_default_config(
        provider, object_name: str = None) -> Dict[str, Any]:
    if object_name is None:
        config_object = _get_default_config(provider)
    else:
        config_object = _get_provider_config_object(provider, object_name)
    return merge_config_hierarchy(
        provider, config_object, system=True, object_name=object_name)


def merge_config_hierarchy(
        provider, config: Dict[str, Any],
        system: bool = False, object_name: str = None) -> Dict[str, Any]:
    base_config_name = config.get("from", None)
    if base_config_name:
        # base config is provided, we need to merge with base configuration
        merged_base_config = get_merged_base_config(
            provider, base_config_name,
            system, object_name)
        merged_config = merge_config(merged_base_config, config)
    elif system:
        merged_config = config
    else:
        # no base, use the system defaults for specific provider as base
        merged_defaults = get_merged_default_config(provider, object_name)
        merged_defaults = merge_config(merged_defaults, config)
        merged_config = copy.deepcopy(merged_defaults)

    return merged_config


def _get_rooted_template_config(
        root: str, template_name: str) -> Dict[str, Any]:
    """Load the template config from root"""
    # Append .yaml extension if the name doesn't include
    if not template_name.endswith(".yaml"):
        template_name += ".yaml"

    template_file = os.path.join(root, template_name)
    with open(template_file) as f:
        template_config = yaml.safe_load(f)

    return template_config


def get_rooted_merged_base_config(
        root: str, base_config_name: str,
        object_name: str = None) -> Dict[str, Any]:
    template_config = _get_rooted_template_config(root, base_config_name)
    merged_config = merge_rooted_config_hierarchy(
        root, template_config, object_name=object_name)
    return merged_config


def merge_rooted_config_hierarchy(
        root: str, config: Dict[str, Any],
        object_name: str = None) -> Dict[str, Any]:
    base_config_name = config.get("from", None)
    if base_config_name:
        # base config is provided, we need to merge with base configuration
        merged_base_config = get_rooted_merged_base_config(
            root, base_config_name, object_name)
        merged_config = merge_config(merged_base_config, config)
    else:
        merged_config = config

    return merged_config


def fill_with_defaults(config: Dict[str, Any]) -> Dict[str, Any]:
    # Merge the config with user inheritance hierarchy and system defaults hierarchy
    merged_config = merge_config_hierarchy(config["provider"], config)

    # Fill auth field to avoid key errors.
    # This field is accessed when calling NodeUpdater but is not relevant to
    # certain node providers and is thus left out of some cluster launching
    # configs.
    merged_config["auth"] = merged_config.get("auth", {})

    # Take care of this here, in case a config does not specify any of head,
    # workers, node types, but does specify min workers:
    merged_config.pop("min_workers", None)

    return merged_config


def prepare_pre_internal_commands(config, built_in_commands):
    setup_commands = built_in_commands.get("setup_commands", [])
    cloudtik_setup_command = get_cloudtik_setup_command(config)
    setup_commands += [cloudtik_setup_command]
    built_in_commands["setup_commands"] = setup_commands

    head_setup_commands = built_in_commands.get("head_setup_commands", [])
    cloudtik_head_prepare_command = get_cloudtik_head_prepare_command(config)
    head_setup_commands += [cloudtik_head_prepare_command]
    built_in_commands["head_setup_commands"] = head_setup_commands

    cloudtik_stop_state_command = get_cloudtik_stop_state_command(config)

    head_start_commands = built_in_commands.get("head_start_commands", [])
    cloudtik_start_state_command = get_cloudtik_start_state_command(config)
    head_start_commands += [cloudtik_stop_state_command, cloudtik_start_state_command]
    built_in_commands["head_start_commands"] = head_start_commands

    head_stop_commands = built_in_commands.get("head_stop_commands", [])
    head_stop_commands += [cloudtik_stop_state_command]
    built_in_commands["head_stop_commands"] = head_stop_commands


def prepare_node_internal_commands(config, built_in_commands):
    cloudtik_stop_command = get_cloudtik_stop_command(config)

    head_start_commands = built_in_commands.get("head_start_commands", [])
    cloudtik_head_start_command = get_cloudtik_head_start_command(config)
    head_start_commands += [cloudtik_stop_command, cloudtik_head_start_command]
    built_in_commands["head_start_commands"] = head_start_commands

    head_stop_commands = built_in_commands.get("head_stop_commands", [])
    head_stop_commands += [cloudtik_stop_command]
    built_in_commands["head_stop_commands"] = head_stop_commands

    worker_start_commands = built_in_commands.get("worker_start_commands", [])
    cloudtik_worker_start_command = get_cloudtik_worker_start_command(config)
    worker_start_commands += [cloudtik_stop_command, cloudtik_worker_start_command]
    built_in_commands["worker_start_commands"] = worker_start_commands

    worker_stop_commands = built_in_commands.get("worker_stop_commands", [])
    worker_stop_commands += [cloudtik_stop_command]
    built_in_commands["worker_stop_commands"] = worker_stop_commands


def prepare_post_internal_commands(config, built_in_commands):
    no_controller_on_head = config.get("no_controller_on_head", False)
    if not no_controller_on_head:
        cloudtik_stop_controller_command = get_cloudtik_stop_controller_command(config)

        head_start_commands = built_in_commands.get("head_start_commands", [])
        cloudtik_start_controller_command = get_cloudtik_start_controller_command(config)
        head_start_commands += [cloudtik_stop_controller_command, cloudtik_start_controller_command]
        built_in_commands["head_start_commands"] = head_start_commands

        head_stop_commands = built_in_commands.get("head_stop_commands", [])
        head_stop_commands += [cloudtik_stop_controller_command]
        built_in_commands["head_stop_commands"] = head_stop_commands


def merge_command_key(merged_commands, group_name, from_config, command_key):
    if command_key not in merged_commands:
        merged_commands[command_key] = []

    commands = from_config.get(command_key, [])
    # Commands for this group, don't add the group
    if len(commands) == 0:
        return

    # Append a command group to the command key groups
    command_groups = merged_commands[command_key]
    command_group = {"group_name": group_name, "commands": commands}
    command_groups += [command_group]


def _is_system_runtime(runtime):
    dependencies = runtime.get_dependencies()
    if dependencies and BUILT_IN_RUNTIME_NONE in dependencies:
        return True
    return False


def merge_runtime_commands(config, commands_root, system=False):
    runtime_config = commands_root.get(RUNTIME_CONFIG_KEY)
    if runtime_config is None:
        return

    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        system_runtime = _is_system_runtime(runtime)
        if ((system and not system_runtime)
                or (not system and system_runtime)):
            continue

        runtime_commands = runtime.get_runtime_commands(config)
        if runtime_commands:
            merge_commands_from(commands_root, runtime_type, runtime_commands)


def merge_built_in_commands(config, commands_root, built_in_commands):
    # Merge runtime commands with built-in:
    # pre-built-in -> runtime-system ->  node-built-in - runtime-normal -> post-built-in
    (pre_built_in_commands,
     node_built_in_commands,
     post_built_in_commands) = built_in_commands
    merge_commands_from(
        commands_root, CLOUDTIK_RUNTIME_NAME, pre_built_in_commands)
    merge_runtime_commands(config, commands_root, system=True)
    merge_commands_from(
        commands_root, CLOUDTIK_RUNTIME_NAME, node_built_in_commands)
    merge_runtime_commands(config, commands_root)
    merge_commands_from(
        commands_root, CLOUDTIK_RUNTIME_NAME, post_built_in_commands)


def merge_commands_from(config, group_name, from_config):
    if MERGED_COMMAND_KEY not in config:
        config[MERGED_COMMAND_KEY] = {}
    merged_commands = config[MERGED_COMMAND_KEY]

    for command_key in COMMAND_KEYS:
        merge_command_key(merged_commands, group_name, from_config, command_key)

    merge_docker_initialization_commands(merged_commands, group_name, from_config)


def merge_docker_initialization_commands(merged_commands, group_name, from_config):
    if DOCKER_CONFIG_KEY not in merged_commands:
        merged_commands[DOCKER_CONFIG_KEY] = {}

    docker = merged_commands[DOCKER_CONFIG_KEY]
    from_docker = from_config.get(DOCKER_CONFIG_KEY, {})

    for command_key in DOCKER_COMMAND_KEYS:
        merge_command_key(docker, group_name, from_docker, command_key)


def merge_cluster_config(config):
    reorder_runtimes_for_dependency(config)
    merge_commands(config)
    merge_runtime_config(config)


def make_sure_dependency_order(
        reordered_runtimes: List[str],
        runtime: str, dependency: str):
    try:
        dependency_index = reordered_runtimes.index(dependency)
    except ValueError:
        # dependency not found
        return

    runtime_index = reordered_runtimes.index(runtime)
    if dependency_index > runtime_index:
        # Needs to move dependency ahead
        reordered_runtimes.pop(dependency_index)
        reordered_runtimes.insert(runtime_index, dependency)


def reorder_runtimes_for_dependency(config):
    _reorder_runtimes_for_dependency(config)
    _reorder_runtimes_for_dependency_of_node_types(config)


def _reorder_runtimes_for_dependency_of_node_types(config):
    global_runtime_types = get_enabled_runtimes(config)
    global_runtime_types_set = set(global_runtime_types)

    # Check and reorder node type specific runtime types
    node_types = get_available_node_types(config)
    for node_type in node_types:
        node_type_config = node_types[node_type]
        if RUNTIME_CONFIG_KEY not in node_type_config:
            continue
        node_type_runtime_config = node_type_config[RUNTIME_CONFIG_KEY]
        if RUNTIME_TYPES_CONFIG_KEY not in node_type_runtime_config:
            continue

        node_type_runtime_types = node_type_runtime_config.get(
            RUNTIME_TYPES_CONFIG_KEY, [])
        # Make sure it is a subset of the global runtime types
        node_type_runtime_types_set = set(node_type_runtime_types)

        if not node_type_runtime_types_set.issubset(global_runtime_types_set):
            raise ValueError(
                "Node type {} runtime types {} is not a subset of global runtime types {}.".format(
                    node_type, node_type_runtime_types, global_runtime_types))

        _reorder_runtimes_for_dependency(node_type_config)


def _get_runtimes_with_required(runtime_types, chain=None):
    all_required = set(runtime_types)
    for runtime_type in runtime_types:
        required = _get_required_runtimes_of(runtime_type, chain)
        if not required:
            continue
        all_required.update(required)
    return all_required


def _get_required_runtimes_of(runtime_type, chain=None):
    runtime_cls = _get_runtime_cls(runtime_type)
    required = runtime_cls.get_required()
    if not required:
        return None

    # for loop check
    if chain:
        loop = chain.intersection(required)
        if loop:
            raise RuntimeError(
                "There is a loop in the required runtimes.")
        chain = copy.deepcopy(chain)
        chain.add(runtime_type)
    else:
        chain = {runtime_type}

    return _get_runtimes_with_required(required, chain)


def _fix_runtimes_with_required(runtime_types):
    runtimes_with_required = _get_runtimes_with_required(runtime_types)
    existing_runtime_types = set(runtime_types)
    missing_required = runtimes_with_required.difference(
        existing_runtime_types)

    if missing_required:
        # Keep the original runtime order
        runtime_types += list(missing_required)


def _reorder_runtimes_for_dependency(config):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if runtime_config is None:
        return

    runtime_types = get_runtime_types(runtime_config)
    if len(runtime_types) == 0:
        return

    # fix the required runtimes before reorder
    _fix_runtimes_with_required(runtime_types)

    reordered_runtimes = copy.deepcopy(runtime_types)
    for runtime_type in runtime_types:
        runtime_cls = _get_runtime_cls(runtime_type)
        dependencies = runtime_cls.get_dependencies()
        required = runtime_cls.get_required()
        if not dependencies and not required:
            continue
        if not dependencies:
            dependencies = required
        elif required:
            dependencies += required

        # For each dependency, if it is appeared behind the current runtime, move it ahead
        for dependency in dependencies:
            # handle special runtime dependencies
            if dependency == BUILT_IN_RUNTIME_ALL:
                # move myself to last and ignore other dependencies
                # if there is multiple runtimes with all dependency,
                # user need to keep them ordered if they have dependencies
                runtime_index = reordered_runtimes.index(runtime_type)
                reordered_runtimes.pop(runtime_index)
                reordered_runtimes.append(runtime_type)
                break
            elif dependency == BUILT_IN_RUNTIME_NONE:
                # move myself to first and the remaining dependencies will also put to its front
                # if there is multiple runtimes with none dependency,
                # the higher priority ones should show after the lower ones.
                runtime_index = reordered_runtimes.index(runtime_type)
                reordered_runtimes.pop(runtime_index)
                reordered_runtimes.insert(0, runtime_type)
            else:
                make_sure_dependency_order(
                    reordered_runtimes, runtime=runtime_type, dependency=dependency)

    runtime_config[RUNTIME_TYPES_CONFIG_KEY] = reordered_runtimes


def merge_runtime_config(config):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if runtime_config is None:
        return

    merge_global_runtime_config(config)

    # Handle node type specific runtime config defaults (based on roles)
    merge_runtime_config_for_node_types(config)


def merge_global_runtime_config(config):
    runtime_config = config[RUNTIME_CONFIG_KEY]
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        defaults_config = runtime.get_defaults_config(config)
        if defaults_config is None:
            continue

        if runtime_type not in runtime_config:
            runtime_config[runtime_type] = {}
        user_config = runtime_config[runtime_type]
        merged_config = merge_config(defaults_config, user_config)
        runtime_config[runtime_type] = merged_config


def merge_runtime_config_for_node_types(config):
    node_types = get_available_node_types(config)
    for node_type in node_types:
        node_type_config = node_types[node_type]
        if RUNTIME_CONFIG_KEY not in node_type_config:
            continue

        runtime_config = node_type_config[RUNTIME_CONFIG_KEY]
        merge_runtime_config_for_node_type(config, runtime_config)


def merge_runtime_config_for_node_type(config, runtime_config):
    global_runtime_config = copy.deepcopy(config[RUNTIME_CONFIG_KEY])
    runtime_config_merged = merge_config(global_runtime_config, runtime_config)

    runtime_types = runtime_config_merged.get(RUNTIME_TYPES_CONFIG_KEY, [])
    for runtime_type in runtime_types:
        if runtime_type not in runtime_config:
            continue

        runtime = _get_runtime(runtime_type, runtime_config_merged)
        defaults_config = runtime.get_defaults_config(config)
        if defaults_config is None:
            continue

        user_config = runtime_config[runtime_type]
        merged_config = merge_config(defaults_config, user_config)
        runtime_config[runtime_type] = merged_config


def merge_user_commands(config):
    merge_commands_from(config, "user", config)


def merge_commands(config):
    # Load the built-in commands and merge with defaults
    pre_built_in_commands = merge_config_hierarchy(
        config["provider"], {}, False, "commands")
    # Populate some internal command which is generated on the fly
    prepare_pre_internal_commands(config, pre_built_in_commands)

    node_built_in_commands = {}
    prepare_node_internal_commands(config, node_built_in_commands)

    post_built_in_commands = {}
    prepare_post_internal_commands(config, post_built_in_commands)

    built_in_commands = (
        pre_built_in_commands,
        node_built_in_commands,
        post_built_in_commands)
    merge_global_commands(
        config, built_in_commands=built_in_commands)

    # Merge commands for node types if needed
    merge_commands_for_node_types(
        config, built_in_commands=built_in_commands)


def merge_global_commands(config, built_in_commands):
    merge_commands_for(
        config, config, built_in_commands=built_in_commands)


def merge_commands_for_node_types(config, built_in_commands):
    node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    for node_type in node_types:
        # No need for head
        if node_type == head_node_type:
            continue

        node_type_config = node_types[node_type]
        if not is_commands_merge_needed(config, node_type_config):
            continue

        # Special care needs to take here
        # For a command that if we don't have an override, we need copy it from global
        # If we don't specify the runtime types, we need copy it from global
        new_node_type_config = copy.deepcopy(node_type_config)
        inherit_commands_from(new_node_type_config, config)

        # inherit runtime config
        inherit_runtime_config_from(new_node_type_config, config)

        merge_commands_for(config, new_node_type_config,
                           built_in_commands=built_in_commands)

        node_type_config[MERGED_COMMAND_KEY] = new_node_type_config[MERGED_COMMAND_KEY]


def inherit_commands_from(target_config, from_config):
    for command_key in COMMAND_KEYS:
        inherit_key_from(target_config, from_config, command_key)


def inherit_runtime_config_from(target_config, from_config):
    if RUNTIME_CONFIG_KEY in from_config:
        if RUNTIME_CONFIG_KEY in target_config:
            runtime_config = target_config[RUNTIME_CONFIG_KEY]
        else:
            runtime_config = {}
        base_runtime_config = copy.deepcopy(from_config[RUNTIME_CONFIG_KEY])
        target_config[RUNTIME_CONFIG_KEY] = merge_config(
            base_runtime_config, runtime_config)


def inherit_key_from(target_config, from_config, config_key):
    if config_key not in target_config:
        if config_key in from_config:
            target_config[config_key] = from_config[config_key]


def is_commands_merge_needed(config, node_type_config) -> bool:
    # Check whether commands merge is needed
    # 1. If any of these commands override, we need merge
    for command_key in NODE_TYPE_COMMAND_KEYS:
        if command_key in node_type_config:
            return True

    # 2. If there is node type specific runtime config
    # Which may means to generate a different runtime commands for this node type
    if RUNTIME_CONFIG_KEY in node_type_config:
        return True

    return False


def merge_commands_for(config, commands_root, built_in_commands):
    merge_built_in_commands(
        config, commands_root,
        built_in_commands=built_in_commands)

    # Merge user commands after built-in
    merge_user_commands(commands_root)

    # Combine commands
    merged_commands = commands_root[MERGED_COMMAND_KEY]
    combine_initialization_commands_from_docker(config, merged_commands)

    combine_initialization_commands(merged_commands)
    combine_setup_commands(merged_commands)
    combine_start_commands(merged_commands)
    combine_stop_commands(merged_commands)

    clean_temporary_commands(merged_commands)
    return commands_root


def clean_temporary_commands(merged_commands):
    for command_key in TEMPORARY_COMMAND_KEYS:
        if command_key in merged_commands:
            merged_commands.pop(command_key, None)

    # docker / initialization commands
    if DOCKER_CONFIG_KEY in merged_commands:
        merged_commands.pop(DOCKER_CONFIG_KEY, None)


def combine_initialization_commands_from_docker(config, merged_commands):
    for command_key in DOCKER_COMMAND_KEYS:
        combine_commands_from_docker(
            config, merged_commands, command_key)


def combine_commands_from_docker(config, merged_commands, command_key):
    # Check if docker enabled
    commands = merged_commands[command_key]
    if is_docker_enabled(config):
        docker_commands = merged_commands.get(
            DOCKER_CONFIG_KEY, {}).get(command_key)
        if docker_commands:
            commands += docker_commands

    merged_commands[command_key] = commands


def _get_node_specific_runtime_types(config, node_id: str):
    provider = get_node_provider_of(config)
    node_tags = provider.node_tags(node_id)
    node_type = node_tags.get(CLOUDTIK_TAG_USER_NODE_TYPE)
    return _get_node_type_specific_runtime_types(config, node_type)


def _get_node_type_specific_runtime_types(config, node_type: str):
    node_type_config = _get_node_type_specific_config(config, node_type)
    if (node_type_config is not None) and (
            RUNTIME_CONFIG_KEY in node_type_config) and (
            RUNTIME_TYPES_CONFIG_KEY in node_type_config[RUNTIME_CONFIG_KEY]):
        return node_type_config[RUNTIME_CONFIG_KEY][RUNTIME_TYPES_CONFIG_KEY]

    return get_enabled_runtimes(config)


def _get_node_specific_commands(
        config, provider, node_id: str,
        command_key: str) -> Any:
    node_type = get_node_type(provider, node_id)
    return _get_node_type_specific_commands(config, node_type, command_key)


def _get_node_type_config(config, node_type: str) -> Any:
    if node_type is None:
        return None

    available_node_types = get_available_node_types(config)
    if node_type not in available_node_types:
        raise ValueError(
            f"Unknown node type: {node_type}.")
    return available_node_types[node_type]


def _get_node_type_specific_fields(
        config, node_type: str,
        fields_key: str) -> Any:
    fields = None
    node_type_config = _get_node_type_config(config, node_type)
    if (node_type_config is not None) and (
            fields_key in node_type_config):
        fields = node_type_config[fields_key]

    return fields


def _has_node_type_specific_commands(config, node_type: str):
    return _has_node_type_specific_config(
        config, node_type, MERGED_COMMAND_KEY)


def _get_node_type_specific_commands(
        config, node_type: str,
        command_key: str) -> Any:
    commands = get_commands_to_run(config, command_key)
    node_type_config = _get_node_type_config(config, node_type)
    if (node_type_config is not None) and (
            MERGED_COMMAND_KEY in node_type_config):
        commands = get_commands_to_run(node_type_config, command_key)
    return commands


def _get_node_specific_config(config, provider, node_id: str) -> Any:
    node_tags = provider.node_tags(node_id)
    node_type = node_tags.get(CLOUDTIK_TAG_USER_NODE_TYPE)
    return _get_node_type_specific_config(config, node_type)


def _get_node_type_specific_config(config, node_type: str) -> Any:
    if not node_type:
        return None
    available_node_types = get_available_node_types(config)
    if node_type not in available_node_types:
        raise ValueError(
            f"Unknown node type tag: {node_type}.")
    return available_node_types[node_type]


def _get_node_specific_fields(
        config, provider, node_id: str,
        fields_key: str) -> Any:
    fields = None
    node_specific_config = _get_node_specific_config(
        config, provider, node_id)
    if (node_specific_config is not None) and (
            fields_key in node_specific_config):
        fields = node_specific_config[fields_key]

    return fields


def _merge_node_type_specific_config(
        global_config, node_type_config):
    if node_type_config is not None:
        global_config = copy.deepcopy(global_config)
        return merge_config(global_config, node_type_config)
    return global_config


def _get_node_specific_docker_config(config, provider, node_id):
    node_type = get_node_type(provider, node_id)
    return _get_node_type_specific_docker_config(config, node_type)


def _get_node_type_specific_docker_config(config, node_type: str):
    if DOCKER_CONFIG_KEY not in config:
        return {}
    docker_config = config[DOCKER_CONFIG_KEY]
    node_specific_docker = _get_node_type_specific_fields(
        config, node_type, DOCKER_CONFIG_KEY)
    return _merge_node_type_specific_config(
        docker_config, node_specific_docker)


def _get_node_specific_runtime_config(config, provider, node_id):
    node_type = get_node_type(provider, node_id)
    return _get_node_type_specific_runtime_config(config, node_type)


def _has_node_type_specific_config(config, node_type: str, config_key: str):
    node_type_config = _get_node_type_config(config, node_type)
    if (node_type_config is not None) and (
            config_key in node_type_config):
        return True
    return False


def _has_node_type_specific_runtime_config(config, node_type: str):
    return _has_node_type_specific_config(config, node_type, RUNTIME_CONFIG_KEY)


def _get_node_type_specific_runtime_config(config, node_type: str):
    if RUNTIME_CONFIG_KEY not in config:
        return None
    runtime_config = config[RUNTIME_CONFIG_KEY]
    node_specific_runtime = _get_node_type_specific_fields(
        config, node_type, RUNTIME_CONFIG_KEY)
    return _merge_node_type_specific_config(
        runtime_config, node_specific_runtime)


def get_commands_to_run(config, commands_key):
    merged_commands = config[MERGED_COMMAND_KEY]
    return merged_commands.get(commands_key, [])


def get_node_specific_commands_of_runtimes(
        config, provider, node_id: str,
        command_key: str, runtimes: Optional[List[str]] = None) -> Any:
    commands_to_run = _get_node_specific_commands(
        config, provider, node_id=node_id, command_key=command_key)
    return filter_commands_of_runtimes(commands_to_run, runtimes)


def get_commands_of_runtimes(
        config, command_key,
        runtimes: Optional[List[str]] = None):
    commands_to_run = get_commands_to_run(config, command_key)
    return filter_commands_of_runtimes(commands_to_run, runtimes)


def filter_commands_of_runtimes(
        commands_to_run, runtimes: Optional[List[str]] = None):
    if runtimes is None:
        return commands_to_run

    runtime_commands = []
    for command_group in commands_to_run:
        group_name = command_group.get("group_name", "")
        if group_name in runtimes:
            runtime_commands += [command_group]

    return runtime_commands


def get_default_python_version(config):
    python_version = config.get("python_version")
    if not python_version:
        # if python version is not specified explicitly, use the current running version
        python_version = ".".join(map(str, sys.version_info[:3]))
    return python_version


def get_python_tag_from_version(version):
    parts = version.split('.')
    if len(parts) < 2 or len(parts) > 3:
        raise ValueError(
            "Invalid python version: {}. "
            "Must specify the major and minor version.".format(version))
    return "{}{}".format(parts[0], parts[1])


def get_default_cloudtik_wheel_url(config) -> str:
    python_version = get_default_python_version(config)
    python_tag = get_python_tag_from_version(python_version)
    wheel_url = CLOUDTIK_WHEELS
    wheel_url += "/cloudtik-"
    wheel_url += cloudtik.__version__
    wheel_url += "-"
    wheel_url += "cp{}".format(python_tag)
    wheel_url += "-"
    wheel_url += "cp{}".format(python_tag)
    wheel_url += "-manylinux2014_${arch}.whl"
    return wheel_url


def get_pip_install_command(provider_type, wheel_url):
    if wheel_url:
        setup_command = "(arch=$(uname -m) && pip -qq install -U \"cloudtik["
        setup_command += provider_type
        setup_command += "] @ "
        setup_command += wheel_url
        setup_command += "\")"
    else:
        setup_command = "pip -qq install -U cloudtik["
        setup_command += provider_type
        setup_command += "]=="
        setup_command += cloudtik.__version__
    return setup_command


def get_cloudtik_setup_command(config) -> str:
    provider_type = config["provider"]["type"]
    wheel_url = config.get("cloudtik_wheel_url")

    setup_command = "which cloudtik >/dev/null 2>&1 || "
    setup_command += get_pip_install_command(provider_type, wheel_url)
    if not wheel_url:
        default_wheel_url = get_default_cloudtik_wheel_url(config)
        setup_command += " || "
        setup_command += get_pip_install_command(provider_type, default_wheel_url)

    return setup_command


def get_cloudtik_head_prepare_command(config) -> str:
    return "cloudtik head prepare"


def get_cloudtik_head_start_command(config) -> str:
    start_command = "ulimit -n 65536; cloudtik node start --head"
    return start_command


def get_cloudtik_start_state_command(config) -> str:
    # Start the Redis service for state
    start_command = "ulimit -n 65536; cloudtik node start --head"
    start_command += " --state"
    return start_command


def get_cloudtik_start_controller_command(config) -> str:
    start_command = "ulimit -n 65536; cloudtik node start --head"
    start_command += " --controller"
    return start_command


def get_cloudtik_worker_start_command(config) -> str:
    start_command = "ulimit -n 65536; cloudtik node start"
    return start_command


def get_cloudtik_stop_command(config) -> str:
    stop_command = "cloudtik node stop"
    return stop_command


def get_cloudtik_stop_state_command(config) -> str:
    stop_command = "cloudtik node stop --state"
    return stop_command


def get_cloudtik_stop_controller_command(config) -> str:
    stop_command = "cloudtik node stop --controller"
    return stop_command


def combine_initialization_commands(config):
    initialization_commands = config["initialization_commands"]
    config["head_initialization_commands"] = (
            initialization_commands + config["head_initialization_commands"])
    config["worker_initialization_commands"] = (
            initialization_commands + config["worker_initialization_commands"])


def combine_setup_commands(config):
    setup_commands = config["setup_commands"]
    bootstrap_commands = config["bootstrap_commands"]

    config["head_setup_commands"] = (
            setup_commands + config["head_setup_commands"] + bootstrap_commands)
    config["worker_setup_commands"] = (
            setup_commands + config["worker_setup_commands"] + bootstrap_commands)


def combine_start_commands(config):
    start_commands = config["start_commands"]
    config["head_start_commands"] = (start_commands + config["head_start_commands"])
    config["worker_start_commands"] = (start_commands + config["worker_start_commands"])


def combine_stop_commands(config):
    stop_commands = config["stop_commands"]
    config["head_stop_commands"] = (stop_commands + config["head_stop_commands"])
    config["worker_stop_commands"] = (stop_commands + config["worker_stop_commands"])


def _sum_min_workers(config: Dict[str, Any]):
    sum_min_workers = 0
    if "available_node_types" in config:
        sum_min_workers = sum(
            config["available_node_types"][node_type].get("min_workers", 0)
            for node_type in config["available_node_types"])
    return sum_min_workers


def _get_min_workers(config: Dict[str, Any], node_type: str):
    available_node_types = get_available_node_types(config)
    if node_type not in available_node_types:
        return 0
    return available_node_types[node_type].get("min_workers", 0)


def set_default_max_workers(config):
    if "max_workers" not in config:
        logger.debug(
            "Global max workers not set. "
            "Will set to the sum of min workers or {} which is larger.",
            CLOUDTIK_DEFAULT_MAX_WORKERS)
        sum_min_workers = 0
        node_types = get_available_node_types(config)
        for node_type_name in node_types:
            node_type_data = node_types[node_type_name]
            sum_min_workers += node_type_data.get("min_workers", 0)
        config["max_workers"] = max(sum_min_workers, CLOUDTIK_DEFAULT_MAX_WORKERS)


def set_node_type_min_max_workers(config):
    """Sets default per-node max workers to global max_workers.
    This equivalent to setting the default per-node max workers to infinity,
    with the only upper constraint coming from the global max_workers.
    Sets default per-node min workers to zero.
    Also sets default max_workers for the head node to zero.
    """
    set_default_max_workers(config)

    node_types = get_available_node_types(config)
    for node_type_name in node_types:
        node_type_data = node_types[node_type_name]

        node_type_data.setdefault("min_workers", 0)
        if "max_workers" not in node_type_data:
            if node_type_name == config["head_node_type"]:
                logger.debug(
                    "setting max workers for head node type to 0")
                node_type_data.setdefault("max_workers", 0)
            else:
                global_max_workers = config["max_workers"]
                logger.debug(
                    f"setting max workers for {node_type_name} to "
                    f"{global_max_workers}")
                node_type_data.setdefault("max_workers", global_max_workers)


def set_node_type_resources(config):
    node_types = get_available_node_types(config)
    for node_type_name in node_types:
        node_type = node_types[node_type_name]
        if "resources" not in node_type:
            node_type["resources"] = {}
        resources = node_type["resources"]
        # This is virtual resource which identified the node type for resource schedulers
        resources[node_type_name] = 1


def with_head_node_ip_environment_variables(
        head_ip, envs: Dict[str, Any] = None) -> Dict[str, Any]:
    if head_ip is None:
        head_ip = get_node_ip_address()
    if envs is None:
        envs = {}
    envs[CLOUDTIK_RUNTIME_ENV_HEAD_IP] = head_ip
    return envs


def with_node_ip_environment_variables(
        call_context, node_ip, provider, node_id):
    if node_ip is None:
        # Waiting for node internal ip for node
        if (provider is None) or (node_id is None):
            raise RuntimeError(
                "Missing provider or node id for retrieving node ip.")

        deadline = time.time() + CLOUDTIK_NODE_START_WAIT_S
        node_ip = wait_for_cluster_ip(call_context, provider, node_id, deadline)
        if node_ip is None:
            raise RuntimeError(
                "Failed to get node ip for node {}.".format(node_id))

    ip_envs = {CLOUDTIK_RUNTIME_ENV_NODE_IP: node_ip}
    return ip_envs


def with_runtime_encryption_key(
        encryption_key, environment_variables: Dict[str, Any]):
    if encryption_key:
        encoded_secrets = encode_cluster_secrets(encryption_key)
        if environment_variables is None:
            environment_variables = {}
        environment_variables[CLOUDTIK_RUNTIME_ENV_SECRETS] = encoded_secrets
    return environment_variables


def hash_launch_conf(provider: NodeProvider, node_config, auth):
    node_config = provider.prepare_node_config_for_launch_hash(
        node_config)
    hasher = hashlib.sha1()
    # For hashing, we replace the path to the key with the
    # key itself. This is to make sure the hashes are the
    # same even if keys live at different locations on different
    # machines.
    full_auth = auth.copy()
    for key_type in ["ssh_private_key", "ssh_public_key"]:
        if key_type in auth:
            with open(os.path.expanduser(auth[key_type])) as key:
                full_auth[key_type] = key.read()
    hasher.update(
        json.dumps([node_config, full_auth], sort_keys=True).encode("utf-8"))
    return hasher.hexdigest()


def prepare_config_for_runtime_hash(
        provider: NodeProvider,
        config: Dict[str, Any]) -> Dict[str, Any]:
    return provider.prepare_config_for_runtime_hash(config)


# Cache the file hashes to avoid rescanning it each time. Also, this avoids
# inadvertently restarting workers if the file mount content is mutated on the
# head node.
# This global cache needs to be protected for thread concurrency for future cases
_hash_cache = ConcurrentObjectCache()

HASH_CONTEXT_HEAD_NODE_CONTENTS_HASH = "head_node_contents_hash"
HASH_CONTEXT_CONTENTS_HASHER = "contents_hasher"


def add_content_hashes(hasher, path, allow_non_existing_paths: bool = False):
    def add_hash_of_file(fpath):
        with open(fpath, "rb") as f:
            for chunk in iter(lambda: f.read(2 ** 20), b""):
                hasher.update(chunk)

    path = os.path.expanduser(path)
    if allow_non_existing_paths and not os.path.exists(path):
        return
    if os.path.isdir(path):
        dirs = []
        for dirpath, _, filenames in os.walk(path):
            dirs.append((dirpath, sorted(filenames)))
        for dirpath, filenames in sorted(dirs):
            hasher.update(dirpath.encode("utf-8"))
            for name in filenames:
                hasher.update(name.encode("utf-8"))
                fpath = os.path.join(dirpath, name)
                add_hash_of_file(fpath)
    else:
        add_hash_of_file(path)


def load_runtime_hash(hash_context: Dict[str, Any], file_mounts, hash_str: str):
    contents_hash = hash_context.get(HASH_CONTEXT_HEAD_NODE_CONTENTS_HASH)
    if contents_hash is None:
        contents_hasher = hash_context.get(HASH_CONTEXT_CONTENTS_HASHER)
        if contents_hasher is None:
            contents_hasher = hashlib.sha1()
        contents_hash = hash_contents(contents_hasher, file_mounts)
        hash_context[HASH_CONTEXT_HEAD_NODE_CONTENTS_HASH] = contents_hash

    runtime_hasher = hashlib.sha1()
    runtime_hasher.update(hash_str)
    runtime_hasher.update(contents_hash.encode("utf-8"))
    return runtime_hasher.hexdigest()


def hash_runtime_conf(
        file_mounts,
        cluster_synced_files,
        extra_objs,
        generate_runtime_hash=True,
        generate_file_mounts_contents_hash=False,
        generate_node_types_runtime_hash=False,
        config: Dict[str, Any] = None):
    """Returns two hashes, a runtime hash and file_mounts_content hash.

    The runtime hash is used to determine if the configuration or file_mounts
    contents have changed. It is used at launch time (cloudtik up) to determine if
    a restart is needed.

    The file_mounts_content hash is used to determine if the file_mounts or
    cluster_synced_files contents have changed. It is used at monitor time to
    determine if additional file syncing is needed.
    """
    contents_hasher = hashlib.sha1()
    hash_context = {HASH_CONTEXT_CONTENTS_HASHER: contents_hasher}
    file_mounts_str = json.dumps(
        file_mounts, sort_keys=True).encode("utf-8")

    if generate_runtime_hash:
        extra_objs_str = json.dumps(
            extra_objs, sort_keys=True).encode("utf-8")
        conf_str = (file_mounts_str + extra_objs_str)
        runtime_hash = _hash_cache.get(
            conf_str, load_runtime_hash,
            hash_context=hash_context,
            file_mounts=file_mounts,
            hash_str=conf_str)
    else:
        runtime_hash = None

    head_node_contents_hash = hash_context.get(HASH_CONTEXT_HEAD_NODE_CONTENTS_HASH)
    if generate_file_mounts_contents_hash or head_node_contents_hash is not None:
        # Only generate a contents hash if generate_file_mounts_contents_hash is true or
        # if we need to generate the runtime_hash (head_node_contents_hash is not None)
        if head_node_contents_hash is None:
            head_node_contents_hash = hash_contents(
                contents_hasher, file_mounts)

        # Add cluster_synced_files to the file_mounts_content hash
        if cluster_synced_files is not None:
            for local_path in sorted(cluster_synced_files):
                # For cluster_synced_files, we let the path be non-existant
                # because its possible that the source directory gets set up
                # anytime over the life of the head node.
                add_content_hashes(
                    contents_hasher, local_path, allow_non_existing_paths=True)

        file_mounts_contents_hash = contents_hasher.hexdigest()
    else:
        file_mounts_contents_hash = None

    if generate_node_types_runtime_hash:
        runtime_hash_for_node_types = hash_runtime_conf_for_node_types(
            config, file_mounts, file_mounts_str, head_node_contents_hash)
    else:
        runtime_hash_for_node_types = None

    return runtime_hash, file_mounts_contents_hash, runtime_hash_for_node_types


def hash_contents(hasher, file_mounts):
    for local_path in sorted(file_mounts.values()):
        add_content_hashes(hasher, local_path)
    return hasher.hexdigest()


def hash_runtime_conf_for_node_types(
        config, file_mounts, file_mounts_str, head_node_contents_hash):
    runtime_hash_for_node_types = {}
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)

    hash_context = {}
    if head_node_contents_hash is not None:
        hash_context[HASH_CONTEXT_HEAD_NODE_CONTENTS_HASH] = head_node_contents_hash

    for node_type in available_node_types:
        if node_type == head_node_type:
            continue

        # Check whether we have node type specific commands
        # And runtime config, otherwise, we don't need
        if (not _has_node_type_specific_commands(config, node_type)) and (
                not _has_node_type_specific_runtime_config(config, node_type)):
            continue

        node_type_runtime_conf = {
            "worker_setup_commands": _get_node_type_specific_commands(
                config, node_type, "worker_setup_commands"),
            "worker_start_commands": _get_node_type_specific_commands(
                config, node_type, "worker_start_commands"),
            "runtime": _get_node_type_specific_runtime_config(
                config, node_type)
        }

        extra_objs_str = json.dumps(
            node_type_runtime_conf, sort_keys=True).encode("utf-8")
        node_type_conf_str = (file_mounts_str + extra_objs_str)

        runtime_hash_for_node_types[node_type] = _hash_cache.get(
            node_type_conf_str, load_runtime_hash,
            hash_context=hash_context,
            file_mounts=file_mounts,
            hash_str=node_type_conf_str)

    return runtime_hash_for_node_types


def add_prefix(info_string, prefix):
    """Prefixes each line of info_string, except the first, by prefix."""
    lines = info_string.split("\n")
    prefixed_lines = [lines[0]]
    for line in lines[1:]:
        prefixed_line = ":".join([prefix, line])
        prefixed_lines.append(prefixed_line)
    prefixed_info_string = "\n".join(prefixed_lines)
    return prefixed_info_string


def format_pg(pg):
    strategy = pg["strategy"]
    bundles = pg["bundles"]
    shape_strs = []
    for bundle, count in bundles:
        shape_strs.append(f"{bundle} * {count}")
    bundles_str = ", ".join(shape_strs)
    return f"{bundles_str} ({strategy})"


def parse_placement_group_resource_str(
        placement_group_resource_str: str) -> Tuple[str, Optional[str], bool]:
    """Parse placement group resource in the form of following 3 cases:
    {resource_name}_group_{bundle_id}_{group_name};
    -> This case is ignored as it is duplicated to the case below.
    {resource_name}_group_{group_name};
    {resource_name}

    Returns:
        Tuple of (resource_name, placement_group_name, is_countable_resource).
        placement_group_name could be None if its not a placement group
        resource. is_countable_resource is True if the resource
        doesn't contain bundle index. We shouldn't count resources
        with bundle index because it will
        have duplicated resource information as
        wildcard resources (resource name without bundle index).
    """
    result = PLACEMENT_GROUP_RESOURCE_BUNDLED_PATTERN.match(
        placement_group_resource_str)
    if result:
        return result.group(1), result.group(3), False
    result = PLACEMENT_GROUP_RESOURCE_PATTERN.match(
        placement_group_resource_str)
    if result:
        return result.group(1), result.group(2), True
    return placement_group_resource_str, None, True


def get_usage_report(cluster_metrics_summary: ClusterMetricsSummary) -> str:
    # first collect resources used in placement groups
    placement_group_resource_usage = {}
    placement_group_resource_total = collections.defaultdict(float)
    for resource, (used, total) in cluster_metrics_summary.usage.items():
        (pg_resource_name, pg_name,
         is_countable) = parse_placement_group_resource_str(resource)
        if pg_name:
            if pg_resource_name not in placement_group_resource_usage:
                placement_group_resource_usage[pg_resource_name] = 0
            if is_countable:
                placement_group_resource_usage[pg_resource_name] += used
                placement_group_resource_total[pg_resource_name] += total
            continue

    usage_lines = []
    for resource, (used, total) in sorted(cluster_metrics_summary.usage.items()):
        if "node:" in resource:
            continue  # Skip the auto-added per-node "node:<ip>" resource.

        (_, pg_name, _) = parse_placement_group_resource_str(resource)
        if pg_name:
            continue  # Skip resource used by placement groups

        pg_used = 0
        pg_total = 0
        used_in_pg = resource in placement_group_resource_usage
        if used_in_pg:
            pg_used = placement_group_resource_usage[resource]
            pg_total = placement_group_resource_total[resource]
            # Used includes pg_total because when pgs are created
            # it allocates resources.
            # To get the real resource usage, we should subtract the pg
            # reserved resources from the usage and add pg used instead.
            used = used - pg_total + pg_used

        if resource in ["memory"]:
            to_gib = 1 / 2**30
            line = (f" {(used * to_gib):.2f}/"
                    f"{(total * to_gib):.3f} GiB {resource}")
            if used_in_pg:
                line = line + (f" ({(pg_used * to_gib):.2f} used of "
                               f"{(pg_total * to_gib):.2f} GiB " +
                               "reserved in placement groups)")
            usage_lines.append(line)
        else:
            line = f" {used}/{total} {resource}"
            if used_in_pg:
                line += (f" ({pg_used} used of "
                         f"{pg_total} reserved in placement groups)")
            usage_lines.append(line)

    if len(usage_lines) > 0:
        usage_report = "\n".join(usage_lines)
    else:
        usage_report = " (no usage report)"

    return usage_report


def format_resource_demand_summary(
        resource_demand: List[Tuple[ResourceBundle, int]]) -> List[str]:
    def filter_placement_group_from_bundle(bundle: ResourceBundle):
        """filter placement group from bundle resource name. returns
        filtered bundle and a bool indicate if the bundle is using
        placement group.

        Example: {"CPU_group_groupid": 1} returns {"CPU": 1}, True
                 {"memory": 1} return {"memory": 1}, False
        """
        using_placement_group = False
        result_bundle = dict()
        for pg_resource_str, resource_count in bundle.items():
            (resource_name, pg_name,
             _) = parse_placement_group_resource_str(pg_resource_str)
            result_bundle[resource_name] = resource_count
            if pg_name:
                using_placement_group = True
        return result_bundle, using_placement_group

    bundle_demand = collections.defaultdict(int)
    pg_bundle_demand = collections.defaultdict(int)

    for bundle, count in resource_demand:
        (pg_filtered_bundle,
         using_placement_group) = filter_placement_group_from_bundle(bundle)

        # bundle is a special keyword for placement group ready tasks
        # do not report the demand for this.
        if "bundle" in pg_filtered_bundle.keys():
            continue

        bundle_demand[tuple(sorted(pg_filtered_bundle.items()))] += count
        if using_placement_group:
            pg_bundle_demand[tuple(sorted(
                pg_filtered_bundle.items()))] += count

    demand_lines = []
    for bundle, count in bundle_demand.items():
        line = f" {dict(bundle)}: {count}+ pending tasks/actors"
        if bundle in pg_bundle_demand:
            line += f" ({pg_bundle_demand[bundle]}+ using placement groups)"
        demand_lines.append(line)
    return demand_lines


def get_demand_report(cluster_metrics_summary: ClusterMetricsSummary):
    demand_lines = []
    if cluster_metrics_summary.resource_demand:
        demand_lines.extend(
            format_resource_demand_summary(cluster_metrics_summary.resource_demand))
    for bundle, count in cluster_metrics_summary.request_demand:
        line = f" {bundle}: {count}+ from request_resources()"
        demand_lines.append(line)
    if len(demand_lines) > 0:
        demand_report = "\n".join(demand_lines)
    else:
        demand_report = " (no resource demands)"
    return demand_report


def decode_cluster_scaling_time(status):
    status = status.decode("utf-8")
    as_dict = json.loads(status)
    report_time = float(as_dict["time"])
    return report_time


def format_info_string(
        cluster_metrics_summary,
        cluster_scaler_summary,
        report_time=None,
        verbose: bool = False):
    if report_time is None:
        report_time = datetime.now()
    header = "=" * 8 + f" Cluster Scaler status: {report_time} " + "=" * 8
    separator = "-" * len(header)
    available_node_report_lines = []
    for node_type, count in cluster_scaler_summary.active_nodes.items():
        line = f" {count} {node_type}"
        available_node_report_lines.append(line)
    available_node_report = "\n".join(available_node_report_lines)

    pending_lines = []
    for node_type, count in cluster_scaler_summary.pending_launches.items():
        line = f" {node_type}, {count} launching"
        pending_lines.append(line)
    for ip, node_type, status in cluster_scaler_summary.pending_nodes:
        line = f" {ip}: {node_type}, {status.lower()}"
        pending_lines.append(line)
    if pending_lines:
        pending_report = "\n".join(pending_lines)
    else:
        pending_report = " (no pending nodes)"

    failure_lines = []
    for ip, node_type in cluster_scaler_summary.failed_nodes:
        line = f" {ip}: {node_type}"
        failure_lines.append(line)
    if cluster_scaler_summary.node_availability_summary:
        records = sorted(
            cluster_scaler_summary.node_availability_summary.node_availabilities.values(),
            key=lambda record: record.last_checked_timestamp,
        )
        for record in records:
            if record.is_available:
                continue
            assert record.unavailable_node_information is not None
            node_type = record.node_type
            category = record.unavailable_node_information.category
            description = record.unavailable_node_information.description
            attempted_time = datetime.fromtimestamp(record.last_checked_timestamp)
            formatted_time = (
                # This `:02d` is python syntax for printing a 2
                # digits number with a leading zero as padding if needed.
                f"{attempted_time.hour:02d}:"
                f"{attempted_time.minute:02d}:"
                f"{attempted_time.second:02d}"
            )
            line = f" {node_type}: {category} (latest_attempt: {formatted_time})"
            if verbose:
                line += f" - {description}"
            failure_lines.append(line)

    failure_lines = failure_lines[:
                                  -constants.CLOUDTIK_MAX_FAILURES_DISPLAYED:
                                  -1]
    failure_report = "Recent failures:\n"
    if failure_lines:
        failure_report += "\n".join(failure_lines)
    else:
        failure_report += " (no failures)"

    usage_report = get_usage_report(cluster_metrics_summary)
    demand_report = get_demand_report(cluster_metrics_summary)

    formatted_output = f"""{header}
Node status
{separator}
Healthy:
{available_node_report}
Pending:
{pending_report}
{failure_report}

Resources
{separator}
Usage:
{usage_report}
Demands:
{demand_report}"""
    return formatted_output


def format_readonly_node_type(node_id: str):
    """The anonymous node type for readonly node provider nodes."""
    return "node_{}".format(node_id)


def format_no_node_type_string(node_type: dict):
    placement_group_resource_usage = {}
    regular_resource_usage = collections.defaultdict(float)
    for resource, total in node_type.items():
        (pg_resource_name, pg_name,
         is_countable) = parse_placement_group_resource_str(resource)
        if pg_name:
            if not is_countable:
                continue
            if pg_resource_name not in placement_group_resource_usage:
                placement_group_resource_usage[pg_resource_name] = 0
            placement_group_resource_usage[pg_resource_name] += total
        else:
            regular_resource_usage[resource] += total

    output_lines = [""]
    for resource, total in regular_resource_usage.items():
        output_line = f"{resource}: {total}"
        if resource in placement_group_resource_usage:
            pg_resource = placement_group_resource_usage[resource]
            output_line += f" ({pg_resource} reserved in placement groups)"
        output_lines.append(output_line)

    return "\n  ".join(output_lines)


def check_cidr_conflict(cidr_block, cidr_blocks):
    existed_nets = [ipaddr.IPNetwork(cidr_block) for cidr_block in cidr_blocks]
    net = ipaddr.IPNetwork(cidr_block)

    for existed_net in existed_nets:
        if net.overlaps(existed_net):
            return False

    return True


def get_proxy_process_file(cluster_name: str):
    proxy_process_file = os.path.join(
        get_cloudtik_temp_dir(), "cloudtik-proxy-{}".format(cluster_name))
    return proxy_process_file


def _get_proxy_process(proxy_process_file: str):
    server_process = get_server_process(proxy_process_file)
    if server_process is None:
        return None, None, None
    pid = server_process.get("pid")
    if not pid:
        return None, None, None
    return (server_process["pid"],
            server_process.get("bind_address"),
            server_process.get("port"))


def get_safe_proxy_process(proxy_process_file: str):
    pid, bind_address, port = _get_proxy_process(proxy_process_file)
    if pid is None:
        return None, None, None
    if not check_process_exists(pid):
        return None, None, None
    return pid, bind_address, port


def get_proxy_bind_address_to_show(bind_address: str):
    if bind_address is None or bind_address == "":
        bind_address_to_show = "127.0.0.1"
    elif bind_address == "*" or bind_address == "0.0.0.0":
        bind_address_to_show = "this-node-ip"
    else:
        bind_address_to_show = bind_address
    return bind_address_to_show


def is_use_internal_ip(config: Dict[str, Any]) -> bool:
    return _is_use_internal_ip(config.get("provider", {}))


def _is_use_internal_ip(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("use_internal_ips", False)


def is_use_working_vpc(config: Dict[str, Any]) -> bool:
    return _is_use_working_vpc(config.get("provider", {}))


def _is_use_working_vpc(provider_config: Dict[str, Any]) -> bool:
    if not _is_use_internal_ip(provider_config):
        return False

    return provider_config.get("use_working_vpc", True)


def is_use_peering_vpc(config: Dict[str, Any]) -> bool:
    return _is_use_peering_vpc(config.get("provider", {}))


def _is_use_peering_vpc(provider_config: Dict[str, Any]) -> bool:
    if not _is_use_internal_ip(provider_config):
        return False

    return not _is_use_working_vpc(provider_config)


def is_peering_firewall_allow_working_subnet(config: Dict[str, Any]) -> bool:
    return config.get("provider", {}).get("peering_firewall_allow_working_subnet", True)


def is_peering_firewall_allow_ssh_only(config: Dict[str, Any]) -> bool:
    return config.get("provider", {}).get(
        "peering_firewall_allow_ssh_only", True)


def get_node_cluster_ip(provider: NodeProvider, node: str) -> str:
    return provider.internal_ip(node)


def get_node_cluster_ip_of(config: Dict[str, Any], node: str) -> str:
    provider = get_node_provider_of(config)
    return get_node_cluster_ip(provider, node)


def wait_for_cluster_ip(call_context, provider, node_id, deadline):
    # if we have IP do not print waiting info
    ip = get_node_cluster_ip(provider, node_id)
    if ip is not None:
        return ip

    interval = CLOUDTIK_NODE_SSH_INTERVAL_S
    with call_context.cli_logger.group("Waiting for IP"):
        while time.time() < deadline and \
                not provider.is_terminated(node_id):
            ip = get_node_cluster_ip(provider, node_id)
            if ip is not None:
                call_context.cli_logger.labeled_value("Received", ip)
                return ip
            call_context.cli_logger.print(
                "Not yet available, retrying in {} seconds", str(interval))
            time.sleep(interval)

    return None


def get_node_working_ip(
        config: Dict[str, Any],
        provider: NodeProvider, node: str) -> str:
    if is_use_internal_ip(config):
        node_ip = provider.internal_ip(node)
    else:
        node_ip = provider.external_ip(node)
    return node_ip


def get_head_working_ip(
        config: Dict[str, Any],
        provider: NodeProvider, node: str) -> str:
    return get_node_working_ip(config, provider, node)


def get_cluster_head_ip(config: Dict[str, Any], public: bool = False) -> str:
    provider = get_node_provider_of(config)
    head_node = get_running_head_node(config)
    if public:
        return get_head_working_ip(config, provider, head_node)
    else:
        return get_node_cluster_ip(provider, head_node)


def _get_only_named_dict_child(v):
    if not isinstance(v, list) or len(v) != 1:
        return None

    child = v[0]
    if child is None or not isinstance(
            child, collections.abc.Mapping) or "name" not in child:
        return None

    return child


def _match_named_item(item_1, item_2):
    name_1 = item_1["name"]
    name_2 = item_2["name"]
    return True if name_1 == name_2 else False


def _match_list_item_with_name(target_dict, k, v):
    new_item = _get_only_named_dict_child(v)
    if new_item is None:
        return None

    if k not in target_dict:
        return None

    target_v = target_dict[k]
    target_item = _get_only_named_dict_child(target_v)
    if target_item is None:
        return None

    if not _match_named_item(new_item, target_item):
        return None

    return target_item, new_item


def _is_list_appending(advanced_list_appending: bool, k, v):
    if advanced_list_appending and isinstance(v, list) and (
            k.endswith("++") or k.startswith("++")):
        return True
    return False


def _append_list(target_dict, list_appending):
    if not list_appending:
        return

    for k, v in list_appending.items():
        if k.startswith("++"):
            # Append before
            to_key = k[2:]
            if to_key in target_dict and target_dict[to_key] is not None:
                to_value = target_dict[to_key]
                new_value = v + to_value
                target_dict[to_key] = new_value
            else:
                target_dict[to_key] = v
        else:
            # Append after
            to_key = k[:-2]
            if to_key in target_dict and target_dict[to_key] is not None:
                to_value = target_dict[to_key]
                new_value = to_value + v
                target_dict[to_key] = new_value
            else:
                target_dict[to_key] = v


def update_nested_dict(
        target_dict, new_dict,
        match_list_item_with_name: bool = True,
        advanced_list_appending: bool = True):
    list_appending = {}
    for k, v in new_dict.items():
        if isinstance(v, collections.abc.Mapping):
            target_dict[k] = update_nested_dict(
                target_dict.get(k, {}), v, match_list_item_with_name)
        elif match_list_item_with_name:
            matched_items = _match_list_item_with_name(target_dict, k, v)
            if matched_items is not None:
                target_item, new_item = matched_items
                target_dict[k][0] = update_nested_dict(
                    target_item, new_item, match_list_item_with_name)
            else:
                if _is_list_appending(advanced_list_appending, k, v):
                    list_appending[k] = v
                else:
                    target_dict[k] = v
        else:
            if _is_list_appending(advanced_list_appending, k, v):
                list_appending[k] = v
            else:
                target_dict[k] = v

    # handling list appending
    if advanced_list_appending:
        _append_list(target_dict, list_appending)

    return target_dict


def find_name_in_command(cmdline, name_to_find) -> bool:
    for arglist in cmdline:
        if name_to_find in arglist:
            return True
    return False


def is_alive_time(report_time):
    # TODO: We probably shouldn't rely on time here, but cloud providers
    # have very well synchronized NTP servers, so this should be fine in
    # practice.
    current_time = time.time()
    return is_alive_time_at(report_time, current_time)


def is_alive_time_at(report_time, current_time):
    # If the status is too old, the service has probably already died.
    delta = current_time - report_time
    return delta < constants.HEALTHCHECK_EXPIRATION_S


def get_head_bootstrap_config():
    bootstrap_config_file = os.path.expanduser(
        CLOUDTIK_BOOTSTRAP_CONFIG_FILE)
    if os.path.exists(bootstrap_config_file):
        return bootstrap_config_file
    raise RuntimeError(
        "Cluster bootstrap config not found. Incorrect head environment!")


def load_head_cluster_config() -> Dict[str, Any]:
    config_file = get_head_bootstrap_config()
    with open(config_file) as f:
        config = yaml.safe_load(f.read())
    config = decrypt_config(config)
    return config


def get_attach_command(
        use_screen: bool,
        use_tmux: bool,
        new: bool = False):
    if use_tmux:
        if new:
            cmd = "tmux new"
        else:
            cmd = "tmux attach || tmux new"
    elif use_screen:
        if new:
            cmd = "screen -L"
        else:
            cmd = "screen -L -xRR"
    else:
        if new:
            raise ValueError(
                "--new only makes sense if passing --screen or --tmux")
        cmd = "$SHELL"
    cmd += " || true"
    return cmd


def is_docker_enabled(config: Dict[str, Any]) -> bool:
    return config.get(DOCKER_CONFIG_KEY, {}).get("enabled", False)


def get_nodes_info(
        provider, nodes, extras: bool = False,
        available_node_types: Dict[str, Any] = None):
    return [get_node_info(
        provider, node, extras, available_node_types) for node in nodes]


def get_node_info(
        provider, node, extras: bool = False,
        available_node_types: Dict[str, Any] = None):
    node_info = provider.get_node_info(node)
    node_info["node"] = node

    if extras:
        node_type = node_info.get(CLOUDTIK_TAG_USER_NODE_TYPE)
        resource_info = get_resource_info_of_node_type(
            node_type, available_node_types)
        node_info.update(resource_info)

    return node_info


def _get_sorted_nodes_info(
        config: Dict[str, Any], provider, nodes,
        runtime: str = None, node_status: str = None):
    nodes_info = _get_nodes_info_for(
        config, provider, nodes,
        runtime=runtime, node_status=node_status)

    # sort nodes info based on node type and then node ip for workers
    def node_info_sort(node_info):
        node_ip = node_info[NODE_INFO_NODE_IP]
        node_ip_addr = int(
            ipaddr.IPAddress(node_ip)) if node_ip else 0
        return [node_info[CLOUDTIK_TAG_NODE_KIND], node_ip_addr]

    nodes_info.sort(key=node_info_sort)
    return nodes_info


def _get_number_of_node_in_status(node_info_list, status):
    num_nodes = 0
    for node_info in node_info_list:
        if status == node_info.get(CLOUDTIK_TAG_NODE_STATUS):
            num_nodes += 1
    return num_nodes


def _get_nodes_info_in_status(node_info_list, status):
    return [node_info for node_info in node_info_list if status == node_info.get(
        CLOUDTIK_TAG_NODE_STATUS)]


def _get_nodes_in_status(
        provider, nodes: List[str], node_status: str) -> List[str]:
    return [node for node in nodes if is_node_in_status(provider, node, node_status)]


def is_node_in_status(provider, node: str, node_status: str):
    node_info = provider.get_node_info(node)
    return True if node_status == node_info.get(CLOUDTIK_TAG_NODE_STATUS) else False


def get_provider_config(
        config: Dict[str, Any]) -> Dict[str, Any]:
    # provider key must exist
    return config["provider"]


def get_available_node_types(
        config: Dict[str, Any]) -> Dict[str, Any]:
    # provider key must exist
    return config["available_node_types"]


def get_head_node_type(config: Dict[str, Any]) -> str:
    return config["head_node_type"]


def get_node_provider_of(config: Dict[str, Any]):
    return _get_node_provider(
        config["provider"], config["cluster_name"])


def get_workspace_provider_of(config: Dict[str, Any]):
    return _get_workspace_provider(
        config["provider"], config["workspace_name"])


def get_storage_provider_of(config: Dict[str, Any]):
    return _get_storage_provider(
        config["provider"], config["workspace_name"],
        config["storage_name"])


def get_database_provider_of(config: Dict[str, Any]):
    return _get_database_provider(
        config["provider"], config["workspace_name"],
        config["database_name"])


def _get_worker_nodes(config: Dict[str, Any]) -> List[str]:
    """Returns worker node ids for given configuration."""
    # Technically could be reused in get_worker_node_ips
    provider = get_node_provider_of(config)
    return provider.non_terminated_nodes(
        {CLOUDTIK_TAG_NODE_KIND: NODE_KIND_WORKER})


def is_node_info_for_runtime(
        config: Dict[str, Any], node_info, runtime: str) -> bool:
    node_type = node_info.get(CLOUDTIK_TAG_USER_NODE_TYPE)
    runtime_types = _get_node_type_specific_runtime_types(config, node_type)
    if (runtime_types is not None) and (runtime in runtime_types):
        return True
    return False


def get_nodes_info_for_runtime(
        config: Dict[str, Any], nodes_info, runtime: str):
    return [node_info for node_info in nodes_info if is_node_info_for_runtime(
        config, node_info, runtime)]


def _get_worker_nodes_info(
        config: Dict[str, Any],
        runtime: str = None,
        node_status: str = None) -> List[str]:
    provider = get_node_provider_of(config)
    workers = _get_worker_nodes(config)
    return _get_nodes_info_for(
        config, provider, workers,
        runtime=runtime, node_status=node_status)


def _get_nodes_info_for(
        config: Dict[str, Any], provider, nodes,
        runtime: str = None,
        node_status: str = None):
    nodes_info = get_nodes_info(provider, nodes)

    if runtime is not None:
        # Filter the nodes for the specific runtime only
        nodes_info = get_nodes_info_for_runtime(
            config, nodes_info, runtime)

    if node_status:
        nodes_info = _get_nodes_info_in_status(
            nodes_info, node_status)

    return nodes_info


def _get_worker_node_ips(
        config: Dict[str, Any], runtime: str = None,
        node_status: str = None) -> List[str]:
    provider = get_node_provider_of(config)
    nodes = provider.non_terminated_nodes({
        CLOUDTIK_TAG_NODE_KIND: NODE_KIND_WORKER
    })

    if runtime is not None:
        # Filter the nodes for the specific runtime only
        nodes = get_nodes_for_runtime(config, nodes, runtime)

    if node_status:
        nodes = _get_nodes_in_status(provider, nodes, node_status)

    return [get_node_cluster_ip(provider, node) for node in nodes]


def _get_workers_ready(config: Dict[str, Any], provider):
    workers = _get_worker_nodes(config)
    workers_info = get_nodes_info(provider, workers)

    # get working nodes which are ready
    workers_ready = _get_number_of_node_in_status(
        workers_info, STATUS_UP_TO_DATE)
    return workers_ready


def get_resource_info_of_node_type(node_type, available_node_types):
    resource_info = {}
    if node_type is not None and node_type in available_node_types:
        resources = available_node_types[node_type].get("resources", {})
        resource_info.update(resources)
    return resource_info


def is_node_for_runtime(
        config: Dict[str, Any], node_id: str, runtime: str) -> bool:
    runtime_types = _get_node_specific_runtime_types(config, node_id)
    if (runtime_types is not None) and (runtime in runtime_types):
        return True
    return False


def get_nodes_for_runtime(
        config: Dict[str, Any], nodes: List[str], runtime: str) -> List[str]:
    return [node for node in nodes if is_node_for_runtime(config, node, runtime)]


def sum_worker_cpus(workers_info):
    return sum_nodes_resource(workers_info, constants.CLOUDTIK_RESOURCE_CPU)


def sum_worker_gpus(workers_info):
    return sum_nodes_resource(workers_info, constants.CLOUDTIK_RESOURCE_GPU)


def sum_worker_memory(workers_info):
    return sum_nodes_resource(workers_info, constants.CLOUDTIK_RESOURCE_MEMORY)


def sum_nodes_resource(nodes_info, resource_name):
    total_resource = 0
    for node_info in nodes_info:
        amount = node_info.get(resource_name, 0)
        total_resource += amount
    return total_resource


def get_cpus_of_node_info(node_info):
    return get_resource_of_node_info(
        node_info, constants.CLOUDTIK_RESOURCE_CPU)


def get_gpus_of_node_info(node_info):
    return get_resource_of_node_info(
        node_info, constants.CLOUDTIK_RESOURCE_GPU)


def get_memory_of_node_info(node_info):
    return get_resource_of_node_info(
        node_info, constants.CLOUDTIK_RESOURCE_MEMORY)


def get_resource_of_node_info(node_info, resource_name):
    if node_info:
        return node_info.get(resource_name)
    return None


def unescape_private_key(private_key: str):
    if private_key is None:
        return private_key

    if not private_key.startswith("-----BEGIN PRIVATE KEY-----\\n"):
        return private_key

    # Unescape "/n" to the real newline characters
    # use json load to do the work
    unescaped_private_key = json.loads("\"" + private_key + "\"")
    return unescaped_private_key


def escape_private_key(private_key: str):
    if private_key is None:
        return private_key

    if not private_key.startswith("-----BEGIN PRIVATE KEY-----\n"):
        return private_key

    # Escape the real newline characters
    # Use json dumps to do the work
    escaped_private_key = json.dumps(private_key)
    escaped_private_key = escaped_private_key.strip("\"\'")
    return escaped_private_key


def _get_node_type_specific_object(config, node_type, object_name):
    config_object = config.get(object_name)
    node_type_config = _get_node_type_config(config, node_type)
    if node_type_config is not None:
        node_config_object = node_type_config.get(object_name)
        if node_config_object is not None:
            # Merge with global config object
            if config_object is not None:
                config_object = copy.deepcopy(config_object)
                return merge_config(config_object, node_config_object)
            else:
                return node_config_object
    return config_object


def with_environment_variables_from_config(config, node_type: str):
    config_envs = {}
    runtime_config = _get_node_type_specific_runtime_config(config, node_type)
    if runtime_config is not None:
        envs = runtime_config.get("envs")
        if envs is not None:
            config_envs.update(envs)
    return config_envs


def with_runtime_environment_variables(
        runtime_config, config, provider, node_id: str):
    all_runtime_envs = {}
    if runtime_config is None:
        return all_runtime_envs

    node_type = get_node_type(provider, node_id)
    static_runtime_envs = with_environment_variables_from_config(
        config=config, node_type=node_type)
    all_runtime_envs.update(static_runtime_envs)

    # We always export the cloud storage
    node_type_config = get_node_type_config(config, provider, node_id)
    provider_envs = provider.with_environment_variables(node_type_config, node_id)
    if provider_envs:
        all_runtime_envs.update(provider_envs)

    runtime_types = get_runtime_types(runtime_config)

    if len(runtime_types) > 0:
        all_runtime_envs[constants.CLOUDTIK_RUNTIME_ENV_RUNTIMES] = ",".join(
            runtime_types)

    # Iterate through all the runtimes
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime_envs = runtime.with_environment_variables(
            config, provider=provider, node_id=node_id)
        all_runtime_envs.update(runtime_envs)

    return all_runtime_envs


def get_runtime_shared_memory_ratio(runtime_config, config, node_type: str):
    total_shared_memory_ratio = 0.0

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime_shared_memory_ratio = runtime.get_runtime_shared_memory_ratio(
            config, node_type=node_type)
        if runtime_shared_memory_ratio > 0:
            total_shared_memory_ratio += runtime_shared_memory_ratio

    return total_shared_memory_ratio


def is_gpu_runtime(config):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if runtime_config is None:
        return False
    return _is_gpu_runtime(runtime_config)


def _is_gpu_runtime(runtime_config):
    runtime_types = get_runtime_types(runtime_config)
    if "ai" not in runtime_types:
        return False
    return runtime_config.get("ai", {}).get("with_gpu", False)


def runtime_validate_config(runtime_config, config):
    if runtime_config is None:
        return

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime.validate_config(config)


def runtime_prepare_config(
        runtime_config: Dict[str, Any],
        config: Dict[str, Any]) -> Dict[str, Any]:
    if runtime_config is None:
        return config

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        config = runtime.prepare_config(config)

    return config


def runtime_bootstrap_config(
        runtime_config: Dict[str, Any],
        config: Dict[str, Any]) -> Dict[str, Any]:
    if runtime_config is None:
        return config

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        config = runtime.bootstrap_config(config)

    return config


def runtime_verify_config(runtime_config, config):
    if runtime_config is None:
        return

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime.verify_config(config)


def get_runnable_command(
        runtime_config, target,
        runtime_type: str = None, runtime_options: Optional[List[str]] = None):
    if runtime_config is None:
        return None

    if runtime_type:
        runtime = _get_runtime(runtime_type, runtime_config)
        commands = runtime.get_runnable_command(target, runtime_options)
        if commands:
            return commands
    else:
        # Iterate through all the runtimes
        runtime_types = get_runtime_types(runtime_config)
        for runtime_type in runtime_types:
            runtime = _get_runtime(runtime_type, runtime_config)
            commands = runtime.get_runnable_command(target, runtime_options)
            if commands:
                return commands

    return None


def cluster_booting_completed(config, head_node_id):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if runtime_config is not None:
        # Iterate through all the runtimes
        runtime_types = get_runtime_types(runtime_config)
        for runtime_type in runtime_types:
            runtime = _get_runtime(runtime_type, runtime_config)
            runtime.cluster_booting_completed(config, head_node_id)


def _get_runtime_config_object(config_home: str, provider_config, object_name: str):
    if not object_name.endswith(".yaml"):
        object_name += ".yaml"

    provider_type = provider_config["type"]

    path_to_config_file = os.path.join(config_home, provider_type, object_name)
    if not os.path.exists(path_to_config_file):
        path_to_config_file = os.path.join(config_home, object_name)

    if not os.path.exists(path_to_config_file):
        return {}

    with open(path_to_config_file) as f:
        config_object = yaml.safe_load(f) or {}

    return config_object


def get_runtime_endpoints(config, head_cluster_ip):
    runtime_config = get_runtime_config(config)
    runtime_endpoints = {}
    if runtime_config is None:
        return runtime_endpoints

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        endpoints = runtime.get_runtime_endpoints(
            config, head_cluster_ip)
        if endpoints:
            runtime_endpoints.update(endpoints)

    return runtime_endpoints


def get_enabled_runtimes(config):
    runtime_config = get_runtime_config(config)
    return runtime_config.get(RUNTIME_TYPES_CONFIG_KEY, DEFAULT_RUNTIMES)


def is_runtime_enabled(runtime_config, runtime_type: str):
    if runtime_config is None:
        return False

    runtime_types = get_runtime_types(runtime_config)
    if runtime_type in runtime_types:
        return True

    return False


def get_runtime_types(runtime_config):
    return runtime_config.get(RUNTIME_TYPES_CONFIG_KEY, [])


def get_runtime_logs(runtimes: List[str]):
    runtime_logs = {}
    if runtimes is None:
        return runtime_logs

    # Iterate through all the runtimes
    for runtime_type in runtimes:
        runtime_cls = _get_runtime_cls(runtime_type)
        logs = runtime_cls.get_logs()
        if logs:
            runtime_logs.update(logs)

    return runtime_logs


def get_runtime_processes(runtimes: List[str]):
    runtime_processes = []
    if runtimes is None:
        return runtime_processes

    # Iterate through all the runtimes
    for runtime_type in runtimes:
        runtime_cls = _get_runtime_cls(runtime_type)
        processes = runtime_cls.get_processes()
        if processes:
            runtime_processes += processes

    return runtime_processes


def get_cluster_uri(config: Dict[str, Any]) -> str:
    return _get_cluster_uri(config["provider"]["type"], config["cluster_name"])


def _get_cluster_uri(provider_type: str, cluster_name: str) -> str:
    return CLOUDTIK_CLUSTER_URI_TEMPLATE.format(
        provider_type, cluster_name)


def _parse_runtime_list(runtimes: str):
    if not runtimes:
        return []

    runtime_list = []
    items = split_list(runtimes)
    for item in items:
        if item:
            runtime_list.append(item)
    return runtime_list


def verify_runtime_list(config: Dict[str, Any], runtimes: List[str]):
    runtime_config = get_runtime_config(config)
    runtime_types = get_runtime_types(runtime_config)
    for runtime in runtimes:
        if runtime in runtime_types or runtime == CLOUDTIK_RUNTIME_NAME:
            continue
        raise ValueError(
            f"Runtime {runtime} is not enabled in config.")


def get_verified_runtime_list(config: Dict[str, Any], runtimes: str):
    runtime_list = _parse_runtime_list(runtimes)
    verify_runtime_list(config, runtime_list)
    return runtime_list


def is_node_in_completed_status(provider, node_id) -> bool:
    node_tags = provider.node_tags(node_id)
    status = node_tags.get(CLOUDTIK_TAG_NODE_STATUS)
    if status is None:
        return False

    completed_states = [STATUS_UP_TO_DATE, STATUS_UPDATE_FAILED]
    if status in completed_states:
        return True
    return False


def check_for_single_worker_type(config: Dict[str, Any]):
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    num_worker_type = 0
    worker_type = None
    for node_type in available_node_types:
        if node_type != head_node_type:
            num_worker_type += 1
            worker_type = node_type

    if num_worker_type == 0:
        raise ValueError(
            "No worker type defined for cluster.")
    elif num_worker_type > 1:
        raise ValueError(
            "There are more than one worker types defined.")

    return worker_type


def get_worker_node_type(config: Dict[str, Any]):
    # Get any worker node type.
    # Usually we consider there is only one worker node type
    available_node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    for node_type in available_node_types:
        if node_type != head_node_type:
            return node_type

    raise ValueError(
        "No worker node type defined.")


def _gcd_of_numbers(numbers):
    num1 = numbers[0]
    num2 = numbers[1]
    gcd = math.gcd(num1, num2)
    for i in range(2, len(numbers)):
        gcd = math.gcd(gcd, numbers[i])
    return gcd


def get_preferred_cpu_bundle_size(
        config: Dict[str, Any]) -> Optional[int]:
    return get_preferred_bundle_size(
        config, constants.CLOUDTIK_RESOURCE_CPU)


def get_preferred_gpu_bundle_size(
        config: Dict[str, Any]) -> Optional[int]:
    return get_preferred_bundle_size(
        config, constants.CLOUDTIK_RESOURCE_GPU)


def get_preferred_memory_bundle_size(
        config: Dict[str, Any]) -> Optional[int]:
    return get_preferred_bundle_size(
        config, constants.CLOUDTIK_RESOURCE_MEMORY)


def get_preferred_bundle_size(
        config: Dict[str, Any], resource_id: str) -> Optional[int]:
    available_node_types = config.get("available_node_types")
    if available_node_types is None:
        return None

    resource_sizes = []
    head_node_type = get_head_node_type(config)
    for node_type in available_node_types:
        if node_type == head_node_type:
            continue

        resources = available_node_types[node_type].get("resources", {})
        resource_total = resources.get(resource_id, 0)
        if resource_total > 0:
            resource_sizes += [resource_total]

    num_types = len(resource_sizes)
    if num_types == 0:
        return None
    elif num_types == 1:
        return resource_sizes[0]
    else:
        return _gcd_of_numbers(resource_sizes)


def get_resource_requests_for_cpu(config, num_cpus):
    return get_resource_requests_for(
        config, constants.CLOUDTIK_RESOURCE_CPU, num_cpus)


def get_resource_requests_for(
        config, resource_id, amount, default_bundle_size=1):
    resource_requests = []
    if amount is None:
        return resource_requests
    # For resource requests, it is the statically total resources of the cluster
    # While amount here is the number of worker amount resource, we need to accounted into
    # the head node as the first resource request if the head has such resource
    head_resource_requests = _get_head_resource_requests(
        config, resource_id)
    if head_resource_requests:
        resource_requests += head_resource_requests

    if not resource_requests and amount == 0:
        resource_requests = [{resource_id: 0}]
    else:
        resource_demands_for_workers = get_resource_demands(
            amount, resource_id, config, default_bundle_size)
        if resource_demands_for_workers:
            resource_requests += resource_demands_for_workers
    return resource_requests


def _get_head_resource_requests(config, resource_id):
    head_node_type = get_head_node_type(config)
    return _get_node_type_resource_requests(config, head_node_type, resource_id)


def _get_node_type_resource_requests(config, node_type, resource_id):
    resource_requests = []
    available_node_types = config.get("available_node_types")
    if available_node_types is None:
        return resource_requests

    if node_type not in available_node_types:
        raise RuntimeError(
            "Invalid configuration. Node type {} is not defined.".format(
                node_type))

    node_type_config = available_node_types[node_type]
    resources = node_type_config.get("resources", {})
    resource_total = resources.get(resource_id, 0)
    if resource_total > 0:
        resource_requests += [{resource_id: resource_total}]

    return resource_requests


def get_resource_demands_for_cpu(num_cpus, config):
    return get_resource_demands(
        num_cpus, constants.CLOUDTIK_RESOURCE_CPU, config, 1)


def get_resource_demands_for_gpu(num_gpus, config):
    return get_resource_demands(
        num_gpus, constants.CLOUDTIK_RESOURCE_GPU, config, 1)


def get_resource_demands_for_memory(memory_in_bytes, config):
    return get_resource_demands(
        memory_in_bytes, constants.CLOUDTIK_RESOURCE_MEMORY, config, pow(1024, 3))


def get_resource_demands_for(amount, resource_id, config):
    if resource_id == constants.CLOUDTIK_RESOURCE_CPU:
        return get_resource_demands_for_cpu(amount, config)
    elif resource_id == constants.CLOUDTIK_RESOURCE_GPU:
        return get_resource_demands_for_gpu(amount, config)
    elif resource_id == constants.CLOUDTIK_RESOURCE_MEMORY:
        return get_resource_demands_for_memory(amount, config)
    else:
        return get_resource_demands(amount, resource_id, config, 1)


def get_resource_demands(amount, resource_id, config, default_bundle_size):
    if amount is None:
        return None

    bundle_size = default_bundle_size
    if config:
        # convert the num cpus based on the largest common factor of the node types
        preferred_bundle_size = get_preferred_bundle_size(config, resource_id)
        if preferred_bundle_size and preferred_bundle_size > default_bundle_size:
            bundle_size = preferred_bundle_size

    count = int(amount / bundle_size)
    remaining = amount % bundle_size
    to_request = []
    if count > 0:
        to_request += [{resource_id: bundle_size}] * count
    if remaining > 0:
        to_request += [{resource_id: remaining}]

    return to_request


def get_node_type(provider, node_id: str):
    node_tags = provider.node_tags(node_id)
    node_type = node_tags.get(CLOUDTIK_TAG_USER_NODE_TYPE)
    return node_type


def get_node_type_config(config, provider, node_id: str):
    node_type = get_node_type(provider, node_id)
    if node_type is None:
        raise RuntimeError(
            "Node type of node {} is unknown.".format(node_id))

    return get_node_type_config_of_node_type(config, node_type)


def get_resource_of_node_type(config, node_type: str):
    available_node_types = config.get("available_node_types")
    if (available_node_types is None) or (node_type not in available_node_types):
        return None

    return available_node_types[node_type].get("resources", {})


def get_node_type_config_of_node_type(config, node_type: str):
    available_node_types = config.get("available_node_types")
    if (available_node_types is None) or (node_type not in available_node_types):
        return None

    return available_node_types[node_type]


def encode_cluster_secrets(secrets):
    return to_hex_string(secrets)


def decode_cluster_secrets(encoded_secrets):
    return from_hex_string(encoded_secrets)


def get_runtime_config_key(node_type: str):
    if node_type and len(node_type) > 0:
        runtime_config_key = CLOUDTIK_CLUSTER_RUNTIME_CONFIG_NODE_TYPE.format(
            node_type)
    else:
        runtime_config_key = CLOUDTIK_CLUSTER_RUNTIME_CONFIG
    return runtime_config_key


def _get_node_constraints_for_node_type(config: Dict[str, Any], node_type: str):
    # Check the runtimes of the node type whether it needs to wait minimal before update
    runtime_config = _get_node_type_specific_runtime_config(config, node_type)
    if not runtime_config:
        return None

    # For each
    runtimes_with_node_constraints = []
    runtime_types = get_runtime_types(runtime_config)
    quorum = False
    scalable = False
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime_minimal, runtime_quorum, runtime_scalable = \
            runtime.get_node_constraints(config, node_type)
        if runtime_minimal:
            runtimes_with_node_constraints += [runtime_type]
            if runtime_quorum:
                quorum = runtime_quorum
                if runtime_scalable:
                    scalable = runtime_scalable

    if len(runtimes_with_node_constraints) > 0:
        node_type_config = config["available_node_types"][node_type]
        min_workers = node_type_config.get("min_workers", 0)
        if min_workers > 0:
            return min_workers, quorum, scalable, runtimes_with_node_constraints
    return None


def _notify_node_constraints_reached(
        config: Dict[str, Any], node_type: str,
        head_info, nodes_info, runtimes_to_notify,
        quorum_id: Optional[str] = None):
    runtime_config = _get_node_type_specific_runtime_config(config, node_type)
    if not runtime_config:
        return

    for runtime_type in runtimes_to_notify:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime.node_constraints_reached(
            config, node_type, head_info, nodes_info,
            quorum_id=quorum_id)


def get_running_head_node(
        config: Dict[str, Any],
        _provider: Optional[NodeProvider] = None,
        _allow_uninitialized_state: bool = True,
) -> str:
    """Get a valid, running head node."""
    provider = _provider or get_node_provider_of(config)
    head_node_tags = {
        CLOUDTIK_TAG_NODE_KIND: NODE_KIND_HEAD,
    }
    nodes = provider.non_terminated_nodes(head_node_tags)
    head_node = None
    _backup_head_node = None
    for node in nodes:
        node_state = provider.node_tags(node).get(CLOUDTIK_TAG_NODE_STATUS)
        if node_state == STATUS_UP_TO_DATE:
            head_node = node
        else:
            _backup_head_node = node
            cli_logger.warning(
                f"Head node ({node}) is in state {node_state}.")

    if head_node is not None:
        return head_node
    else:
        if _allow_uninitialized_state and _backup_head_node is not None:
            cli_logger.warning(
                f"The head node being returned: {_backup_head_node} is not "
                "`up-to-date`.\nIf you are not debugging a startup issue, "
                "it is recommended to restart this cluster.")

            return _backup_head_node
        if _backup_head_node is not None:
            raise HeadNotHealthyError(
                _backup_head_node,
                "Head node of cluster {} is not healthy.".format(
                    config["cluster_name"]))
        else:
            raise HeadNotRunningError(
                "Head node of cluster {} not found.".format(
                    config["cluster_name"]))


def load_properties_file(
        properties_file, separator='=') -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    properties = {}
    # the comments will attach to key before it
    comments = {}
    comments_for_key = []
    last_key = None
    with open(properties_file, "r") as f:
        for line in f.readlines():
            # Strip all the spaces and tabs
            striped_line = line.strip()
            if (striped_line == ""
                    or striped_line.startswith("#")
                    or striped_line.startswith("!")):
                # Consider a comment for current key
                # The comment is kept as it was instead of striped
                comments_for_key += [line.rstrip('\n')]
            else:
                if len(comments_for_key) > 0:
                    if last_key:
                        comments[last_key] = comments_for_key
                    else:
                        # header comments
                        comments[""] = comments_for_key
                    comments_for_key = []

                # Use split() instead of split(" ") to split value with multiple spaces
                key_value = striped_line.split(separator)
                key = key_value[0].strip()
                value = separator.join(key_value[1:]).strip()
                properties[key] = value
                last_key = key

    # if there are head comments without key
    if len(comments_for_key) > 0:
        if last_key:
            comments[last_key] = comments_for_key
        else:
            # header comments
            comments[""] = comments_for_key

    return properties, comments


def save_properties_file(
        properties_file,  properties: Dict[str, str], separator='=',
        comments: Dict[str, List[str]] = None):
    with open(properties_file, "w+") as f:
        # write head comments if there are any
        comments_for_key = comments.get("") if comments else None
        if comments_for_key:
            for comment in comments_for_key:
                # The comment line is kept as it was
                f.write(comment)
                f.write("\n")

        for key, value in properties.items():
            f.write("{}{}{}\n".format(key, separator, value))

            # write comments after key
            if comments and key in comments:
                comments_for_key = comments[key]
                for comment in comments_for_key:
                    # The comment line is kept as it was
                    f.write(comment)
                    f.write("\n")


def is_managed_cloud_storage(workspace_config: Dict[str, Any]) -> bool:
    return _is_managed_cloud_storage(workspace_config["provider"])


def _is_managed_cloud_storage(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("managed_cloud_storage", False)


def is_use_managed_cloud_storage(config: Dict[str, Any]) -> bool:
    return _is_use_managed_cloud_storage(config["provider"])


def _is_use_managed_cloud_storage(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("use_managed_cloud_storage", False)


def get_managed_cloud_storage_name(config: Dict[str, Any]):
    return _get_managed_cloud_storage_name(config["provider"])


def _get_managed_cloud_storage_name(provider_config: Dict[str, Any]):
    return provider_config.get("managed_cloud_storage_name")


def is_managed_cloud_database(workspace_config: Dict[str, Any]) -> bool:
    return _is_managed_cloud_database(workspace_config["provider"])


def _is_managed_cloud_database(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("managed_cloud_database", False)


def is_use_managed_cloud_database(config: Dict[str, Any]) -> bool:
    return _is_use_managed_cloud_database(config["provider"])


def _is_use_managed_cloud_database(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("use_managed_cloud_database", False)


def get_managed_cloud_database_name(config: Dict[str, Any]):
    return _get_managed_cloud_database_name(config["provider"])


def _get_managed_cloud_database_name(provider_config: Dict[str, Any]):
    return provider_config.get("managed_cloud_database_name")


def is_worker_role_for_cloud_storage(config: Dict[str, Any]) -> bool:
    return config["provider"].get("worker_role_for_cloud_storage", True)


def is_permanent_data_volumes(config: Dict[str, Any]) -> bool:
    return _is_permanent_data_volumes(config["provider"])


def _is_permanent_data_volumes(provider_config: Dict[str, Any]) -> bool:
    return provider_config.get("permanent_data_volumes", False)


def get_config_options(config: Dict[str, Any]):
    return config.get(OPTIONS_CONFIG_KEY, {})


def get_config_options_for_update(config: Dict[str, Any]):
    return get_config_for_update(config, OPTIONS_CONFIG_KEY)


def get_config_option(
        config: Dict[str, Any], option_name, default=None):
    options = config.get(OPTIONS_CONFIG_KEY)
    if options is None:
        return default
    return options.get(option_name, default)


def is_stable_node_seq_id_enabled(config):
    return get_config_option(
        config, "stable_node_seq_id", False)


def enable_stable_node_seq_id(config):
    config_options = get_config_options_for_update(config)
    config_options["stable_node_seq_id"] = True


def is_config_use_hostname(config):
    return get_config_option(config, "use_hostname", True)


def is_config_use_fqdn(config):
    return get_config_option(config, "use_fqdn", False)


def set_terminate_idle(config, value: bool):
    if get_config_option(config, "terminate_idle", True) != value:
        config_options = get_config_options_for_update(config)
        config_options["terminate_idle"] = value


def check_workspace_name_format(workspace_name):
    return bool(re.match("^[a-z0-9-]*$", workspace_name))


def print_dict_info(info: Dict[str, Any]):
    for k, v in info.items():
        if isinstance(v, collections.abc.Mapping):
            with cli_logger.group("{}:".format(k)):
                print_dict_info(v)
        else:
            cli_logger.labeled_value(k, v)


def get_head_service_ports(runtime_config):
    if runtime_config is None:
        return {}

    # Iterate through all the runtimes
    runtime_types = get_runtime_types(runtime_config)
    return _get_head_service_ports(runtime_types, runtime_config)


def _get_head_service_ports(runtime_types, runtime_config):
    service_ports = {}
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        runtime_service_ports = runtime.get_head_service_ports()
        if runtime_service_ports:
            service_ports.update(runtime_service_ports)

    return service_ports


def get_head_node_config(config: Dict[str, Any]) -> Dict[str, Any]:
    node_types = get_available_node_types(config)
    head_node_type = get_head_node_type(config)
    return node_types[head_node_type]["node_config"]


def is_config_key_with_privacy(key):
    if not isinstance(key, str):
        return False
    key = key.lower()
    for keyword in PRIVACY_CONFIG_KEYS:
        if keyword in key:
            return True
    return False


def process_key_with_privacy(v, param):
    if v is None:
        return v

    if isinstance(v, str):
        val_len = len(v)
        replacement_len = len(PRIVACY_REPLACEMENT)
        if val_len > replacement_len:
            v = PRIVACY_REPLACEMENT_TEMPLATE.format("-" * (val_len - replacement_len))
        else:
            v = PRIVACY_REPLACEMENT
    else:
        v = PRIVACY_REPLACEMENT

    return v


def process_config_with_privacy(
        config, func=process_key_with_privacy, param=None):
    if config is None:
        return

    if isinstance(config, collections.abc.Mapping):
        for k, v in config.items():
            if isinstance(v, collections.abc.Mapping) or isinstance(v, list):
                process_config_with_privacy(v, func, param)
            elif is_config_key_with_privacy(k):
                config[k] = func(v, param)
    elif isinstance(config, list):
        for item in config:
            if isinstance(item, collections.abc.Mapping) or isinstance(item, list):
                process_config_with_privacy(item, func, param)


def get_config_cipher():
    secrets = decode_cluster_secrets(CLOUDTIK_CONFIG_SECRET)
    cipher = AESCipher(secrets)
    return cipher


def encrypt_config_value(v, cipher):
    if v is None or not isinstance(v, str):
        return v

    return CLOUDTIK_ENCRYPTION_PREFIX + cipher.encrypt(v).decode("utf-8")


def decrypt_config_value(v, cipher):
    if v is None or (
            not isinstance(v, str)) or (
            not v.startswith(CLOUDTIK_ENCRYPTION_PREFIX)):
        return v

    target_bytes = v[len(CLOUDTIK_ENCRYPTION_PREFIX):].encode("utf-8")
    return cipher.decrypt(target_bytes)


def encrypt_string(v: str):
    if v is None:
        return v
    cipher = get_config_cipher()
    return cipher.encrypt(v).decode("utf-8")


def decrypt_string(v: str):
    if v is None:
        return v
    cipher = get_config_cipher()
    return cipher.decrypt(v.encode("utf-8"))


def get_runtime_encryption_key(config):
    # The encryption key is the key to encrypt the runtime configuration
    # shared among head and worker nodes.
    encryption_key = config.get(ENCRYPTION_KEY_CONFIG_KEY)
    if not encryption_key:
        return None
    secrets = decode_cluster_secrets(encryption_key)
    # the key must be 32 bytes
    n = len(secrets)
    if n < 32:
        # pad to 32 bytes if not
        x = bytearray(secrets)
        x.extend([i for i in range(32 - n)])
        return bytes(x)
    elif n > 32:
        return secrets[:32]
    return secrets


def _get_runtime_scaling_policy(config, head_host):
    runtime_config = get_runtime_config(config)
    if not runtime_config:
        return None

    runtime_types = get_runtime_types(runtime_config)
    if len(runtime_types) > 0:
        for runtime_type in runtime_types:
            runtime = _get_runtime(runtime_type, runtime_config)
            scaling_policy = runtime.get_scaling_policy(config, head_host)
            if scaling_policy is not None:
                return scaling_policy
    return None


def _get_scaling_config(config):
    runtime_config = get_runtime_config(config)
    if not runtime_config:
        return None
    return runtime_config.get("scaling")


def _get_scaling_policy_cls(class_path):
    """Get the ScalingPolicy class from user specified module and class name.
    Returns:
        ScalingPolicy class
    """
    scaling_policy_class = load_class(path=class_path)
    if scaling_policy_class is None:
        raise NotImplementedError(
            "Cannot load external scaling policy class: {}".format(class_path))

    return scaling_policy_class


def merge_optional_dict(config, updates):
    if config is None:
        return updates
    if updates is None:
        return config
    return update_nested_dict(config, updates)


def merge_scaling_state(scaling_state: ScalingState, new_scaling_state: ScalingState):
    autoscaling_instructions = merge_optional_dict(
        scaling_state.autoscaling_instructions, new_scaling_state.autoscaling_instructions)
    node_resource_states = merge_optional_dict(
        scaling_state.node_resource_states, new_scaling_state.node_resource_states)
    lost_nodes = merge_optional_dict(
        scaling_state.lost_nodes, new_scaling_state.lost_nodes)
    return ScalingState(autoscaling_instructions, node_resource_states, lost_nodes)


def convert_nodes_to_cpus(
        config: Dict[str, Any], nodes: int,
        node_type: Optional[str] = None) -> int:
    return convert_nodes_to_resource(
        config, nodes, constants.CLOUDTIK_RESOURCE_CPU, node_type)


def convert_nodes_to_memory(
        config: Dict[str, Any], nodes: int,
        node_type: Optional[str] = None) -> int:
    return convert_nodes_to_resource(
        config, nodes, constants.CLOUDTIK_RESOURCE_MEMORY, node_type)


def convert_nodes_to_gpus(
        config: Dict[str, Any], nodes: int,
        node_type: Optional[str] = None) -> int:
    return convert_nodes_to_resource(
        config, nodes, constants.CLOUDTIK_RESOURCE_GPU, node_type)


def convert_nodes_to_resource(
        config: Dict[str, Any], nodes: int, resource_id,
        node_type_name: Optional[str] = None) -> int:
    available_node_types = get_available_node_types(config)
    if node_type_name:
        if node_type_name not in available_node_types:
            raise RuntimeError(
                "Node type {} is not defined in available_node_types.".format(node_type_name))
        resources = available_node_types[node_type_name].get("resources", {})
        resource_total = resources.get(resource_id, 0)
        if resource_total <= 0:
            raise ValueError(
                "The amount of {} resource for {} is invalid {}".format(
                    resource_id, node_type_name, resource_total))
        return nodes * resource_total

    head_node_type = get_head_node_type(config)
    for node_type in available_node_types:
        if node_type != head_node_type:
            resources = available_node_types[node_type].get("resources", {})
            resource_total = resources.get(resource_id, 0)
            if resource_total > 0:
                return nodes * resource_total
    return 0


def get_storage_config_for_update(provider_config):
    return get_config_for_update(
        provider_config, PROVIDER_STORAGE_CONFIG_KEY)


def get_database_config_for_update(provider_config):
    return get_config_for_update(
        provider_config, PROVIDER_DATABASE_CONFIG_KEY)


def print_json_formatted(json_bytes):
    json_object = json.loads(json_bytes)
    formatted_response = json.dumps(json_object, indent=4)
    click.echo(formatted_response)


def get_command_session_name(cmd: str, timestamp: int):
    timestamp_str = "{}".format(timestamp)
    hasher = hashlib.sha1()
    hasher.update(cmd.encode("utf-8"))
    hasher.update(timestamp_str.encode("utf-8"))
    return "cloudtik-" + hasher.hexdigest()


def get_workspace_nat_public_ip_bandwidth_conf(
        workspace_config: Dict[str, Any]) -> int:
    return workspace_config.get('provider', {}).get('public_ip_bandwidth', 20)


def get_cluster_node_public_ip_bandwidth_conf(
        cluster_provider_config: Dict[str, Any]) -> int:
    return cluster_provider_config.get('public_ip_bandwidth', 20)


def export_runtime_flags(runtime_config, prefix, runtime_envs):
    # export each flags started with 'with_'
    for key in runtime_config:
        if key.startswith("with_"):
            with_flag = runtime_config.get(key)
            if with_flag:
                with_flag_var = "{}_{}".format(prefix.upper(), key.upper())
                runtime_envs[with_flag_var] = with_flag


def is_head_node_by_tags(tags):
    if not tags or CLOUDTIK_TAG_NODE_KIND not in tags:
        return False
    return True if tags[CLOUDTIK_TAG_NODE_KIND] == NODE_KIND_HEAD else False


def get_server_process(server_process_file: str):
    if os.path.exists(server_process_file):
        with open(server_process_file) as file:
            server_process = json.loads(file.read())
            return server_process
    return None


def save_server_process(
        server_process_file, server_process):
    server_process_dir = os.path.dirname(server_process_file)
    if not os.path.exists(server_process_dir):
        os.makedirs(server_process_dir, exist_ok=True)
    with open_with_mode(server_process_file, "w", os_mode=0o600) as f:
        f.write(json.dumps(server_process))


def validate_resources(resources, name="Resources"):
    if resources is not None:
        if isinstance(resources, Dict):
            for key in resources.keys():
                if not (isinstance(key, str) and isinstance(resources[key], int)):
                    raise TypeError(
                        f"{name} key should be str and value as int.")
        else:
            raise TypeError(
                f"{name} should be a Dict.")


def parse_resources_json(
    resources: str, command_arg="--resources"
) -> Dict[str, int]:
    try:
        resources = json.loads(resources)
        if not isinstance(resources, dict):
            raise ValueError
    except Exception:
        cli_logger.error(
            "`{}` is not a valid JSON string.", cf.bold(command_arg))
        cli_logger.abort(
            "Valid values look like this: `{}`",
            cf.bold(
                f'{command_arg}='
                '\'{"CustomResource1": 1, "CustomResource2": 2}\''
            ),
        )
    return resources


def validate_bundles(bundles):
    if bundles is not None:
        if isinstance(bundles, List):
            for bundle in bundles:
                validate_resources(bundle, "Bundle")
        else:
            raise TypeError(
                "Bundles should be of type List")


def parse_bundles_json(
    bundles: str, command_arg="--bundles"
) -> Dict[str, int]:
    try:
        bundles = json.loads(bundles)
        validate_bundles(bundles)
    except Exception:
        cli_logger.error(
            "`{}` is not a valid JSON string.", cf.bold(command_arg))
        cli_logger.abort(
            "Valid values look like this: `{}`",
            cf.bold(
                f'{command_arg}='
                '\'[{"CPU": 4, "GPU": 1}, {"CPU": 4, "GPU": 1}]\''
            ),
        )
    return bundles


def parse_resource_list(resource_list_str: str, ) -> Dict[str, int]:
    resource_dict = {}
    resources = split_list(resource_list_str)
    for resource in resources:
        resource_parts = [x.strip() for x in resource.split(":")]
        if len(resource_parts) != 2:
            raise ValueError(
                "Invalid resource specification. Format: resource_type:amount")
        resource_name = resource_parts[0]
        resource_amount = int(resource_parts[1])
        resource_dict[resource_name] = resource_amount
    return resource_dict


def get_resource_list_str(resources: Dict[str, int]) -> str:
    # Format the resources in a form like 'CPU:4,GPU:1,Custom:3'.
    return ",".join(["{}:{}".format(*kv) for kv in resources.items()])


def parse_resources(resources_str: str, ) -> Dict[str, int]:
    # try two ways, json or list
    try:
        resources = json.loads(resources_str)
        if not isinstance(resources, dict):
            raise ValueError
    except Exception:
        return parse_resource_list(resources_str)


def with_verbose_option(cmds, call_context):
    _cli_logger = call_context.cli_logger
    if _cli_logger.verbosity_overriden:
        verbosity = _cli_logger.verbosity
        if verbosity > 0:
            # maximum 10 verbose
            verbosity = verbosity if verbosity < 10 else 10
            cmds += ["-v" for _ in range(verbosity)]


def get_run_script_command(script, script_args):
    import cloudtik as cloudtik_home
    # import script registry here because it will search for all the packages for
    # registering scripts
    from cloudtik.core._private.script_registry import get_registered_script

    root_path = os.path.abspath(os.path.dirname(cloudtik_home.__file__))
    script_target = get_registered_script(script)
    if script_target:
        # run a registered command pointing to a script
        script = script_target

    if script.endswith(".sh"):
        target = os.path.join(root_path, script)
        cmds = ["bash", quote(target)]
    elif script.endswith(".py"):
        target = os.path.join(root_path, script)
        cmds = [sys.executable, "-u", quote(target)]
    else:
        target = script
        # it should be a python module
        cmds = [sys.executable, "-u", "-m", quote(target)]

    with_script_args(cmds, script_args)

    final_cmd = " ".join(cmds)
    return final_cmd


def run_script(script, script_args, with_output=False):
    final_cmd = get_run_script_command(script, script_args)
    try:
        if with_output:
            return subprocess.check_output(final_cmd, shell=True)
        else:
            subprocess.check_call(final_cmd, shell=True)
    except subprocess.CalledProcessError as err:
        print(f"Called process error {err}")
        raise err


def get_runtime_config(
        config: Dict[str, Any]) -> Dict[str, Any]:
    # There key cannot be empty for current implementation
    return config.get(RUNTIME_CONFIG_KEY, {})


def get_runtime_config_for_update(config):
    return get_config_for_update(config, RUNTIME_CONFIG_KEY)


def prepare_runtime_config_on_head(config):
    runtime_config = config.get(RUNTIME_CONFIG_KEY)
    if runtime_config is None:
        return

    old_config_digest = get_json_object_md5(config)

    # Iterate through all the runtimes
    # First pass for prepare config on head
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        config = runtime.prepare_config_on_head(
            config)

    # Second pass for bootstrap config on head
    runtime_types = get_runtime_types(runtime_config)
    for runtime_type in runtime_types:
        runtime = _get_runtime(runtime_type, runtime_config)
        config = runtime.bootstrap_config_on_head(
            config)

    new_config_digest = get_json_object_md5(config)
    if new_config_digest != old_config_digest:
        # save the new config
        encrypted_config = encrypt_config(config)
        cluster_config_file = get_head_bootstrap_config()
        with open(cluster_config_file, "w") as f:
            f.write(json.dumps(encrypted_config))


def get_cloud_credentials(provider_config, credentials_key, default=None):
    credentials_config = provider_config.get(
        PROVIDER_CREDENTIALS_CONFIG_KEY)
    if not credentials_config:
        return default
    return credentials_config.get(credentials_key, default)


def clear_cloud_credentials(provider_config, credentials_key):
    credentials_config = provider_config.get(
        PROVIDER_CREDENTIALS_CONFIG_KEY)
    if not credentials_config:
        return
    if credentials_key in credentials_config:
        credentials_config.pop(credentials_key, None)


def get_node_type_resources(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """Fills out resources for available_node_types."""
    cluster_resource = {}
    if "available_node_types" not in cluster_config:
        return cluster_resource

    # Since we have filled the resources for node types
    # We simply don't retrieve it from cloud provider again
    available_node_types = cluster_config["available_node_types"]
    head_node_type = cluster_config["head_node_type"]
    for node_type in available_node_types:
        resources = available_node_types[node_type].get("resources", {})
        memory_total_in_mb = int(resources.get("memory", 0) / (1024 * 1024))
        cpu_total = resources.get("CPU", 0)
        if node_type != head_node_type:
            if memory_total_in_mb > 0:
                cluster_resource["worker_memory"] = memory_total_in_mb
            if cpu_total > 0:
                cluster_resource["worker_cpu"] = cpu_total
        else:
            if memory_total_in_mb > 0:
                cluster_resource["head_memory"] = memory_total_in_mb
            if cpu_total > 0:
                cluster_resource["head_cpu"] = cpu_total

    # If there is only one node type, worker type uses the head type
    if ("worker_memory" not in cluster_resource) and ("head_memory" in cluster_resource):
        cluster_resource["worker_memory"] = cluster_resource["head_memory"]
    if ("worker_cpu" not in cluster_resource) and ("head_cpu" in cluster_resource):
        cluster_resource["worker_cpu"] = cluster_resource["head_cpu"]

    return cluster_resource


def get_cluster_name(config: Dict[str, Any]) -> str:
    return config["cluster_name"]


def get_workspace_name(config: Dict[str, Any]) -> str:
    return config["workspace_name"]


def _random_string():
    id_hash = hashlib.shake_128()
    id_hash.update(uuid.uuid4().bytes)
    id_bytes = id_hash.digest(constants.ID_SIZE)
    assert len(id_bytes) == constants.ID_SIZE
    return id_bytes

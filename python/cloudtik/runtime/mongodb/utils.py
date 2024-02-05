import os
import uuid
from typing import Any, Dict

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_MONGODB
from cloudtik.core._private.service_discovery.naming import get_cluster_head_host
from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime
from cloudtik.core._private.service_discovery.utils import \
    get_canonical_service_name, define_runtime_service, \
    get_service_discovery_config, define_runtime_service_on_head
from cloudtik.core._private.util.core_utils import base64_encode_string, get_config_for_update, \
    export_environment_variables, address_string
from cloudtik.core._private.utils import get_runtime_config, get_node_cluster_ip_of, get_workspace_name, \
    get_cluster_name
from cloudtik.runtime.common.service_discovery.discovery import DiscoveryType
from cloudtik.runtime.common.service_discovery.runtime_discovery import \
    discover_runtime_service
from cloudtik.runtime.common.service_discovery.utils import get_service_addresses_string, \
    get_service_addresses_from_string
from cloudtik.runtime.common.service_discovery.workspace import register_service_to_workspace

RUNTIME_PROCESSES = [
        # The first element is the substring to filter.
        # The second element, if True, is to filter ps results by command name.
        # The third element is the process name.
        # The forth element, if node, the process should on all nodes,if head, the process should on head node.
        ["mongod", True, "MongoDB", "node"],
        ["mongos", True, "Mongos", "node"],
    ]

MONGODB_SERVICE_PORT_CONFIG_KEY = "port"

MONGODB_REPLICATION_SET_NAME_CONFIG_KEY = "replication_set_name"
MONGODB_REPLICATION_SET_KEY_CONFIG_KEY = "replication_set_key"

MONGODB_CLUSTER_MODE_CONFIG_KEY = "cluster_mode"
MONGODB_CLUSTER_MODE_NONE = "none"
# replication
MONGODB_CLUSTER_MODE_REPLICATION = "replication"
# sharding
MONGODB_CLUSTER_MODE_SHARDING = "sharding"

MONGODB_ROOT_USER_CONFIG_KEY = "root_user"
MONGODB_ROOT_PASSWORD_CONFIG_KEY = "root_password"

MONGODB_DATABASE_CONFIG_KEY = "database"
MONGODB_DATABASE_NAME_CONFIG_KEY = "name"
MONGODB_DATABASE_USER_CONFIG_KEY = "user"
MONGODB_DATABASE_PASSWORD_CONFIG_KEY = "password"

MONGODB_SHARDING_CONFIG_KEY = "sharding"

MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_KEY = "cluster_role"
MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_SERVER = "configsvr"
MONGODB_SHARDING_CLUSTER_ROLE_MONGOS = "mongos"
MONGODB_SHARDING_CLUSTER_ROLE_SHARD = "shardsvr"

MONGODB_MONGOS_PORT_CONFIG_KEY = "mongos_port"

MONGODB_SERVICE_TYPE = BUILT_IN_RUNTIME_MONGODB
MONGODB_SECONDARY_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-secondary"

MONGODB_CONFIG_SERVER_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-configsvr"
MONGODB_MONGOS_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-mongos"
MONGODB_SHARD_SERVER_SERVICE_TYPE = MONGODB_SERVICE_TYPE + "-shardsvr"

MONGODB_SERVICE_PORT_DEFAULT = 27017
MONGODB_MONGOS_PORT_DEFAULT = 27018

MONGODB_ROOT_USER_DEFAULT = "root"
MONGODB_ROOT_PASSWORD_DEFAULT = "cloudtik"

MONGODB_CONFIG_SERVER_URI_KEY = "config_server_uri"
MONGODB_CONFIG_SERVER_SERVICE_DISCOVERY_KEY = "config_server_service_discovery"
MONGODB_CONFIG_SERVER_SERVICE_SELECTOR_KEY = "config_server_service_selector"


def _get_config(runtime_config: Dict[str, Any]):
    return runtime_config.get(BUILT_IN_RUNTIME_MONGODB, {})


def _get_service_port(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_SERVICE_PORT_CONFIG_KEY, MONGODB_SERVICE_PORT_DEFAULT)


def _get_root_password(mongodb_config):
    return mongodb_config.get(
        MONGODB_ROOT_PASSWORD_CONFIG_KEY, MONGODB_ROOT_PASSWORD_DEFAULT)


def _get_cluster_mode(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_CLUSTER_MODE_CONFIG_KEY, MONGODB_CLUSTER_MODE_REPLICATION)


def _get_replication_set_name(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_REPLICATION_SET_NAME_CONFIG_KEY)


def _get_replication_set_key(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_REPLICATION_SET_KEY_CONFIG_KEY)


def _get_sharding_config(mongodb_config: Dict[str, Any]):
    return mongodb_config.get(
        MONGODB_SHARDING_CONFIG_KEY, {})


def _get_sharding_cluster_role(sharding_config: Dict[str, Any]):
    return sharding_config.get(
        MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_KEY,
        MONGODB_SHARDING_CLUSTER_ROLE_SHARD)


def _get_mongos_port(sharding_config: Dict[str, Any]):
    return sharding_config.get(
        MONGODB_MONGOS_PORT_CONFIG_KEY)


def _generate_replication_set_name(workspace_name, cluster_name):
    if not cluster_name:
        raise RuntimeError(
            "Cluster name is needed for default replication set name.")
    return f"{workspace_name}-{cluster_name}"


def _generate_replication_set_key(config: Dict[str, Any]):
    workspace_name = get_workspace_name(config)
    key_material = str(uuid.uuid3(uuid.NAMESPACE_OID, workspace_name))
    return base64_encode_string(key_material)


def _get_replication_set_name_prefix(
        mongodb_config, workspace_name, cluster_name):
    replication_set_name = _get_replication_set_name(
        mongodb_config)
    if not replication_set_name:
        replication_set_name = _generate_replication_set_name(
            workspace_name, cluster_name)
    return replication_set_name


def _get_sharding_replication_set_name(
        mongodb_config, workspace_name, cluster_name, cluster_role):
    replication_set_name_prefix = _get_replication_set_name_prefix(
            mongodb_config, workspace_name, cluster_name)
    return f"{replication_set_name_prefix}-{cluster_role}"


def _get_home_dir():
    return os.path.join(
        os.getenv("HOME"), "runtime", BUILT_IN_RUNTIME_MONGODB)


def _get_runtime_processes():
    return RUNTIME_PROCESSES


def _get_runtime_logs():
    home_dir = _get_home_dir()
    logs_dir = os.path.join(home_dir, "logs")
    return {BUILT_IN_RUNTIME_MONGODB: logs_dir}


def _is_config_server_needed(mongodb_config):
    sharding_config = _get_sharding_config(mongodb_config)
    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        cluster_role = _get_sharding_cluster_role(sharding_config)
        if (cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_MONGOS or
                cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_SHARD):
            return True
    return False


def _prepare_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    mongodb_config = _get_config(runtime_config)
    if _is_config_server_needed(mongodb_config):
        cluster_config = discover_config_server_from_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB)

    return cluster_config


def _bootstrap_runtime_config(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    return cluster_config


def _prepare_config_on_head(
        runtime_config: Dict[str, Any], cluster_config: Dict[str, Any]):
    mongodb_config = _get_config(runtime_config)
    if _is_config_server_needed(mongodb_config):
        # discover config server for mongos
        cluster_config = discover_config_server_on_head(
            cluster_config, BUILT_IN_RUNTIME_MONGODB)

    # call validate config to fail earlier
    _validate_config(cluster_config, final=True)
    return cluster_config


def _validate_config(config: Dict[str, Any], final=False):
    runtime_config = get_runtime_config(config)
    mongodb_config = _get_config(runtime_config)

    database = mongodb_config.get(MONGODB_DATABASE_CONFIG_KEY, {})
    user = database.get(MONGODB_DATABASE_USER_CONFIG_KEY)
    password = database.get(MONGODB_DATABASE_PASSWORD_CONFIG_KEY)
    if (user and not password) or (not user and password):
        raise ValueError(
            "User and password must be both specified or not specified.")

    if _is_config_server_needed(mongodb_config):
        config_server_uri = mongodb_config.get(MONGODB_CONFIG_SERVER_URI_KEY)
        if not config_server_uri:
            # if there is service discovery mechanism, assume we can get from service discovery
            if (final or
                    not is_config_server_service_discovery(mongodb_config) or
                    not get_service_discovery_runtime(runtime_config)):
                raise ValueError(
                    "Config server must be configured for Mongos or Shard cluster.")


def _get_mongos_port_with_default(sharding_config):
    mongos_port = _get_mongos_port(sharding_config)
    if not mongos_port:
        cluster_role = _get_sharding_cluster_role(sharding_config)
        if cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_MONGOS:
            mongos_port = MONGODB_SERVICE_PORT_DEFAULT
        else:
            mongos_port = MONGODB_MONGOS_PORT_DEFAULT
    return mongos_port


def _with_common_environment_variables(
        mongodb_config, runtime_envs):
    service_port = _get_service_port(mongodb_config)
    runtime_envs["MONGODB_SERVICE_PORT"] = service_port

    root_user = mongodb_config.get(
        MONGODB_ROOT_USER_CONFIG_KEY, MONGODB_ROOT_USER_DEFAULT)
    runtime_envs["MONGODB_ROOT_USER"] = root_user

    root_password = _get_root_password(mongodb_config)
    runtime_envs["MONGODB_ROOT_PASSWORD"] = root_password

    database = mongodb_config.get(MONGODB_DATABASE_CONFIG_KEY, {})
    database_name = database.get(MONGODB_DATABASE_NAME_CONFIG_KEY)
    if database_name:
        runtime_envs["MONGODB_DATABASE"] = database_name
    user = database.get(MONGODB_DATABASE_USER_CONFIG_KEY)
    if user:
        runtime_envs["MONGODB_USER"] = user
    password = database.get(MONGODB_DATABASE_PASSWORD_CONFIG_KEY)
    if password:
        runtime_envs["MONGODB_PASSWORD"] = password


def _with_replication_set_key(
        mongodb_config, config, runtime_envs):
    root_password = _get_root_password(mongodb_config)
    if root_password:
        # use replication set key only when there is a root password set
        replication_set_key = _get_replication_set_key(
            mongodb_config)
        if not replication_set_key:
            replication_set_key = _generate_replication_set_key(config)
        runtime_envs["MONGODB_REPLICATION_SET_KEY"] = replication_set_key


def _with_replication_environment_variables(
        mongodb_config, config, runtime_envs):
    _with_replication_set_key(
        mongodb_config, config, runtime_envs)

    # default to workspace name + cluster name
    workspace_name = get_workspace_name(config)
    cluster_name = get_cluster_name(config)
    replication_set_name = _get_replication_set_name_prefix(
        mongodb_config, workspace_name, cluster_name)
    runtime_envs["MONGODB_REPLICATION_SET_NAME"] = replication_set_name


def _with_sharding_environment_variables(
        mongodb_config, config, runtime_envs):
    _with_replication_set_key(
        mongodb_config, config, runtime_envs)

    sharding_config = _get_sharding_config(mongodb_config)
    cluster_role = _get_sharding_cluster_role(sharding_config)
    runtime_envs["MONGODB_SHARDING_CLUSTER_ROLE"] = cluster_role

    # default to workspace name + cluster name + cluster role
    workspace_name = get_workspace_name(config)
    cluster_name = get_cluster_name(config)
    replication_set_name = _get_sharding_replication_set_name(
        mongodb_config, workspace_name, cluster_name, cluster_role)
    runtime_envs["MONGODB_REPLICATION_SET_NAME"] = replication_set_name

    mongos_port = _get_mongos_port_with_default(sharding_config)
    runtime_envs["MONGODB_MONGOS_SERVICE_PORT"] = mongos_port


def _with_runtime_environment_variables(
        runtime_config, config):
    runtime_envs = {}

    mongodb_config = _get_config(runtime_config)
    _with_common_environment_variables(
        mongodb_config, runtime_envs)

    cluster_mode = _get_cluster_mode(mongodb_config)
    runtime_envs["MONGODB_CLUSTER_MODE"] = cluster_mode
    if cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION:
        _with_replication_environment_variables(
            mongodb_config, config, runtime_envs)
    elif cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        _with_sharding_environment_variables(
            mongodb_config, config, runtime_envs)
    return runtime_envs


def _parse_config_server_uri(config_server_uri):
    # TODO: this will not be needed if we support parsing uri directly to mongos
    if not config_server_uri:
        raise ValueError(
            "Empy config server uri.")
    uri_components = config_server_uri.split('/')
    if len(uri_components) != 2:
        raise ValueError(
            "Invalid config server uri.")
    replication_set_name, addresses_string = config_server_uri.split('/')
    service_addresses = get_service_addresses_from_string(
        addresses_string)
    if len(service_addresses) == 0:
        raise ValueError(
            "Invalid config server uri: empty address.")
    primary_address = service_addresses[0]
    return replication_set_name, primary_address[0], primary_address[1]


def _with_config_server(mongodb_config, envs=None):
    # export the config server information
    config_server_uri = mongodb_config.get(MONGODB_CONFIG_SERVER_URI_KEY)

    (replication_set_name,
     config_server_host,
     config_server_port) = _parse_config_server_uri(config_server_uri)
    if (not replication_set_name
            or not config_server_host):
        raise RuntimeError(
            "No config server cluster configured or found.")
    if envs is None:
        envs = {}
    envs["MONGODB_CONFIG_SERVER_REPLICATION_SET_NAME"] = replication_set_name
    envs["MONGODB_CONFIG_SERVER_HOST"] = config_server_host
    if config_server_port:
        envs["MONGODB_CONFIG_SERVER_PORT"] = config_server_port
    return envs


def _node_configure(runtime_config, head: bool):
    mongodb_config = _get_config(runtime_config)
    if _is_config_server_needed(mongodb_config):
        envs = _with_config_server(mongodb_config)
        export_environment_variables(envs)


def register_sharding_service(mongodb_config, cluster_config, head_ip):
    service_port = _get_service_port(mongodb_config)
    sharding_config = _get_sharding_config(mongodb_config)
    cluster_role = _get_sharding_cluster_role(sharding_config)
    if cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_SERVER:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB,
            service_addresses=[(head_ip, service_port)],
            service_name=MONGODB_CONFIG_SERVER_SERVICE_TYPE)
    elif cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_SHARD:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB,
            service_addresses=[(head_ip, service_port)],
            service_name=MONGODB_SHARD_SERVER_SERVICE_TYPE)
    else:
        mongos_service_port = _get_mongos_port_with_default(sharding_config)
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB,
            service_addresses=[(head_ip, mongos_service_port)],
            service_name=MONGODB_MONGOS_SERVICE_TYPE)


def register_service(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any], head_node_id: str) -> None:
    mongodb_config = _get_config(runtime_config)
    service_port = _get_service_port(mongodb_config)

    head_ip = get_node_cluster_ip_of(cluster_config, head_node_id)
    head_host = get_cluster_head_host(cluster_config, head_ip)

    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION:
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB,
            service_addresses=[(head_host, service_port)])
    elif cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        register_sharding_service(
            mongodb_config, cluster_config, head_host)
    else:
        # single standalone on head
        register_service_to_workspace(
            cluster_config, BUILT_IN_RUNTIME_MONGODB,
            service_addresses=[(head_host, service_port)])


def _get_runtime_endpoints(
        runtime_config: Dict[str, Any], cluster_config, cluster_head_ip):
    head_host = get_cluster_head_host(cluster_config, cluster_head_ip)
    mongodb_config = _get_config(runtime_config)
    service_port = _get_service_port(mongodb_config)
    endpoints = {}

    def add_mongod_endpoint():
        endpoints["mongodb"] = {
            "name": "MongoDB",
            "url": address_string(head_host, service_port)
        }

    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION:
        add_mongod_endpoint()
    elif cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        sharding_config = _get_sharding_config(mongodb_config)
        mongos_service_port = _get_mongos_port_with_default(sharding_config)

        def add_mongos_endpoint():
            endpoints["mongos"] = {
                "name": "Mongos",
                "url": address_string(head_host, mongos_service_port)
            }

        cluster_role = _get_sharding_cluster_role(sharding_config)
        if cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_MONGOS:
            add_mongos_endpoint()
        else:
            add_mongod_endpoint()
            add_mongos_endpoint()
    else:
        add_mongod_endpoint()
    return endpoints


def _get_head_service_ports(runtime_config: Dict[str, Any]) -> Dict[str, Any]:
    mongodb_config = _get_config(runtime_config)
    service_port = _get_service_port(mongodb_config)
    service_ports = {
        "mongodb": {
            "protocol": "TCP",
            "port": service_port,
        },
    }
    return service_ports


def _get_sharding_runtime_services(
        mongodb_config, cluster_name,
        service_discovery_config, service_port):
    sharding_config = _get_sharding_config(mongodb_config)
    cluster_role = _get_sharding_cluster_role(sharding_config)
    mongos_service_name = get_canonical_service_name(
        service_discovery_config, cluster_name, MONGODB_MONGOS_SERVICE_TYPE)
    mongos_service_port = _get_mongos_port_with_default(sharding_config)

    if cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_SERVER:
        config_server_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_CONFIG_SERVER_SERVICE_TYPE)
        services = {
            config_server_service_name: define_runtime_service(
                MONGODB_CONFIG_SERVER_SERVICE_TYPE,
                service_discovery_config, service_port),
            mongos_service_name: define_runtime_service(
                MONGODB_MONGOS_SERVICE_TYPE,
                service_discovery_config, mongos_service_port),
        }
    elif cluster_role == MONGODB_SHARDING_CLUSTER_ROLE_SHARD:
        shard_server_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_SHARD_SERVER_SERVICE_TYPE)
        services = {
            shard_server_service_name: define_runtime_service(
                MONGODB_SHARD_SERVER_SERVICE_TYPE,
                service_discovery_config, service_port),
            mongos_service_name: define_runtime_service(
                MONGODB_MONGOS_SERVICE_TYPE,
                service_discovery_config, mongos_service_port),
        }
    else:
        services = {
            mongos_service_name: define_runtime_service(
                MONGODB_MONGOS_SERVICE_TYPE,
                service_discovery_config, mongos_service_port),
        }
    return services


def _get_runtime_services(
        runtime_config: Dict[str, Any],
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    cluster_name = get_cluster_name(cluster_config)
    mongodb_config = _get_config(runtime_config)
    service_discovery_config = get_service_discovery_config(mongodb_config)
    service_port = _get_service_port(mongodb_config)

    cluster_mode = _get_cluster_mode(mongodb_config)
    if cluster_mode == MONGODB_CLUSTER_MODE_REPLICATION:
        # all nodes are possible primary
        secondary_service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_SECONDARY_SERVICE_TYPE)
        services = {
            secondary_service_name: define_runtime_service(
                MONGODB_SECONDARY_SERVICE_TYPE,
                service_discovery_config, service_port),
        }
    elif cluster_mode == MONGODB_CLUSTER_MODE_SHARDING:
        services = _get_sharding_runtime_services(
            mongodb_config, cluster_name,
            service_discovery_config, service_port)
    else:
        # single standalone on head
        service_name = get_canonical_service_name(
            service_discovery_config, cluster_name, MONGODB_SERVICE_TYPE)
        services = {
            service_name: define_runtime_service_on_head(
                MONGODB_SERVICE_TYPE,
                service_discovery_config, service_port),
        }
    return services


def is_config_server_service_discovery(runtime_type_config):
    return runtime_type_config.get(
        MONGODB_CONFIG_SERVER_SERVICE_DISCOVERY_KEY, True)


def discover_config_server_from_workspace(
        cluster_config: Dict[str, Any], runtime_type):
    runtime_config = get_runtime_config(cluster_config)
    runtime_type_config = runtime_config.get(runtime_type, {})
    if (runtime_type_config.get(MONGODB_CONFIG_SERVER_URI_KEY) or
            not is_config_server_service_discovery(runtime_type_config)):
        return cluster_config

    config_server_uri = discover_config_server(
        runtime_type_config, MONGODB_CONFIG_SERVER_SERVICE_SELECTOR_KEY,
        cluster_config=cluster_config,
        discovery_type=DiscoveryType.WORKSPACE)
    if config_server_uri:
        runtime_type_config = get_config_for_update(
            runtime_config, runtime_type)
        runtime_type_config[MONGODB_CONFIG_SERVER_URI_KEY] = config_server_uri
    return cluster_config


def discover_config_server_on_head(
        cluster_config: Dict[str, Any], runtime_type):
    runtime_config = get_runtime_config(cluster_config)
    runtime_type_config = runtime_config.get(runtime_type, {})
    if not is_config_server_service_discovery(runtime_type_config):
        return cluster_config

    config_server_uri = runtime_type_config.get(MONGODB_CONFIG_SERVER_URI_KEY)
    if config_server_uri:
        # already configured
        return cluster_config

    # There is service discovery to come here
    config_server_uri = discover_config_server(
        runtime_type_config, MONGODB_CONFIG_SERVER_SERVICE_SELECTOR_KEY,
        cluster_config=cluster_config,
        discovery_type=DiscoveryType.CLUSTER)
    if config_server_uri:
        runtime_type_config = get_config_for_update(
            runtime_config, runtime_type)
        runtime_type_config[MONGODB_CONFIG_SERVER_URI_KEY] = config_server_uri
    return cluster_config


def discover_config_server(
        config: Dict[str, Any],
        service_selector_key: str,
        cluster_config: Dict[str, Any],
        discovery_type: DiscoveryType,):
    service_instance = discover_runtime_service(
        config, service_selector_key,
        cluster_config=cluster_config,
        discovery_type=discovery_type,
        runtime_type=BUILT_IN_RUNTIME_MONGODB,
        service_type=MONGODB_CONFIG_SERVER_SERVICE_TYPE,
    )
    if service_instance is None:
        return None

    cluster_name = service_instance.cluster_name
    workspace_name = cluster_config["workspace_name"]
    service_addresses = service_instance.service_addresses
    replication_set_name = _get_sharding_replication_set_name(
        config, workspace_name, cluster_name,
        MONGODB_SHARDING_CLUSTER_ROLE_CONFIG_SERVER)

    # TODO: support host name in service discovery?
    addresses_string = get_service_addresses_string(service_addresses)
    # in format of: <configReplSetName>/cfg1.example.net:27019,cfg2.example.net:27019
    config_server_uri = "{}/{}".format(
        replication_set_name, addresses_string)
    return config_server_uri

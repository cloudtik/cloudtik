LOAD_BALANCER_CONFIG_NAME = "name"
LOAD_BALANCER_CONFIG_TYPE = "type"
LOAD_BALANCER_CONFIG_SCHEME = "scheme"

LOAD_BALANCER_CONFIG_PROTOCOL = "protocol"
LOAD_BALANCER_CONFIG_PORT = "port"

LOAD_BALANCER_CONFIG_ROUTE_PATH = "route_path"
LOAD_BALANCER_CONFIG_SERVICE_PATH = "service_path"
LOAD_BALANCER_CONFIG_DEFAULT = "default"

LOAD_BALANCER_CONFIG_ADDRESS = "address"

LOAD_BALANCER_CONFIG_ID = "id"
LOAD_BALANCER_CONFIG_NODE_ID = "node_id"
LOAD_BALANCER_CONFIG_SEQ_ID = "seq_id"

LOAD_BALANCER_CONFIG_PUBLIC_IPS = "public_ips"
LOAD_BALANCER_CONFIG_SERVICE_GROUPS = "service_groups"
LOAD_BALANCER_CONFIG_SERVICES = "services"
LOAD_BALANCER_CONFIG_LISTENERS = "listeners"
LOAD_BALANCER_CONFIG_TARGETS = "targets"
LOAD_BALANCER_CONFIG_TAGS = "tags"


def get_load_balancer_config_name(load_balancer_config):
    return load_balancer_config[LOAD_BALANCER_CONFIG_NAME]


def get_load_balancer_config_type(load_balancer_config):
    return load_balancer_config[LOAD_BALANCER_CONFIG_TYPE]


def get_load_balancer_config_scheme(load_balancer_config):
    return load_balancer_config[LOAD_BALANCER_CONFIG_SCHEME]


def get_load_balancer_public_ips(load_balancer_config):
    return load_balancer_config.get(
        LOAD_BALANCER_CONFIG_PUBLIC_IPS, [])


def get_load_balancer_service_groups(load_balancer_config):
    return load_balancer_config.get(
        LOAD_BALANCER_CONFIG_SERVICE_GROUPS, [])


def get_service_group_services(service_group):
    return service_group.get(LOAD_BALANCER_CONFIG_SERVICES, [])


def get_service_group_listeners(service_group):
    return service_group.get(LOAD_BALANCER_CONFIG_LISTENERS, [])


def get_service_targets(service):
    return service.get(LOAD_BALANCER_CONFIG_TARGETS, [])

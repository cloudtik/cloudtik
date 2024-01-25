from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_XINETD
from cloudtik.core._private.service_discovery.utils import SERVICE_DISCOVERY_NODE_KIND, \
    SERVICE_DISCOVERY_NODE_KIND_HEAD, SERVICE_DISCOVERY_NODE_KIND_NODE, SERVICE_DISCOVERY_NODE_KIND_WORKER, \
    match_node_kind

HEALTH_CHECK_PORT = "port"
HEALTH_CHECK_SCRIPT = "script"
HEALTH_CHECK_NODE_KIND = SERVICE_DISCOVERY_NODE_KIND

HEALTH_CHECK_NODE_KIND_HEAD = SERVICE_DISCOVERY_NODE_KIND_HEAD
HEALTH_CHECK_NODE_KIND_WORKER = SERVICE_DISCOVERY_NODE_KIND_WORKER
HEALTH_CHECK_NODE_KIND_NODE = SERVICE_DISCOVERY_NODE_KIND_NODE

HEALTH_CHECK_RUNTIME = BUILT_IN_RUNTIME_XINETD
# service type = HEALTH_CHECK_RUNTIME-TARGET_RUNTIME
HEALTH_CHECK_SERVICE_TYPE_TEMPLATE = HEALTH_CHECK_RUNTIME + "-{}"


def match_health_check_node(health_check, head):
    node_kind = health_check.get(HEALTH_CHECK_NODE_KIND)
    return match_node_kind(node_kind, head)


def get_health_check_port_of_service(health_check_service):
    # get the port of health check service from consul
    service_addresses = health_check_service.service_addresses
    if not service_addresses:
        return None
    service_address = service_addresses[0]
    # return the port
    return service_address[1]


def get_health_check_service_type_of(runtime_type):
    return HEALTH_CHECK_SERVICE_TYPE_TEMPLATE.format(runtime_type)

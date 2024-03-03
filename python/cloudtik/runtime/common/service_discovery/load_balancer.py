# This label marks a service to act as default
# You should choose the default service carefully.
# There should be only one default service within a service group
from cloudtik.core._private.util.core_utils import JSONSerializableObject
from cloudtik.runtime.common.service_discovery.consul import get_common_label_of_service_nodes

LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_ROUTE_PATH = "route-path"
LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_SERVICE_PATH = "service-path"
LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_DEFAULT_SERVICE = "default-service"

LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_PROTOCOL = "load-balancer-protocol"
LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_PORT = "load-balancer-port"

LOAD_BALANCER_SERVICE_DISCOVERY_NAME_LABEL = "load-balancer-name"
LOAD_BALANCER_SERVICE_DISCOVERY_SCHEME_LABEL = "load-balancer-scheme"


class ApplicationBackendService(JSONSerializableObject):
    def __init__(
            self, service_name,
            route_path=None, service_path=None, default_service=False):
        self.service_name = service_name
        self.route_path = route_path
        self.service_path = service_path
        self.default_service = default_service

    def get_route_path(self):
        # Note: route path should be in the form of /abc, /abc/ or /
        # /abc will match /abc or /abc/*
        # /abc/ will match only /abc/*
        # / will match every path but because it is the shortest route,
        # it should be the last priority to be matched for load balancers.
        route_path = self.route_path
        if not route_path:
            route_path = self.get_default_route_path()
        return route_path

    def get_service_path(self):
        # Note: The final service path should be in the form of /abc
        # if it is in the form of / or /abc/, it will be stripped to empty or /abc
        service_path = self.service_path
        return service_path.rstrip('/') if service_path else None

    def get_default_route_path(self):
        if self.default_service:
            route_path = "/"
        else:
            route_path = "/" + self.service_name
        return route_path


def get_checked_port(port):
    if not port:
        raise ValueError(
            "Invalid port: port is not specified.")
    if isinstance(port, str):
        port = int(port)
    return port


def get_application_route_from_service_nodes(service_nodes):
    route_path = get_common_label_of_service_nodes(
        service_nodes, LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_ROUTE_PATH,
        error_if_not_same=True)
    service_path = get_common_label_of_service_nodes(
        service_nodes, LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_SERVICE_PATH,
        error_if_not_same=True)

    # Warning: for a group of services, there should be only one default service
    default_service_label = get_common_label_of_service_nodes(
        service_nodes, LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_DEFAULT_SERVICE,
        error_if_not_same=True)
    default_service = True if default_service_label is not None else False
    return route_path, service_path, default_service

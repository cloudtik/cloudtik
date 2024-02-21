# This label marks a service to act as default
# You should choose the default service carefully.
# There should be only one default service within a service group
LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_DEFAULT_SERVICE = "load-balancer-default-service"

LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_PROTOCOL = "load-balancer-protocol"
LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_PORT = "load-balancer-port"

LOAD_BALANCER_SERVICE_DISCOVERY_NAME_LABEL = "load-balancer-name"


def get_checked_port(port):
    if not port:
        raise ValueError(
            "Invalid port: port is not specified.")
    if isinstance(port, str):
        port = int(port)
    return port


def is_default_service_by_label(labels):
    if not labels:
        return False
    default_service_label = labels.get(
        LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_DEFAULT_SERVICE)
    return True if default_service_label is not None else False

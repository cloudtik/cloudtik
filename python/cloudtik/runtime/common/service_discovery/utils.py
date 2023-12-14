from cloudtik.core._private.core_utils import get_address_string


API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH = "route-path"
API_GATEWAY_SERVICE_DISCOVERY_LABEL_SERVICE_PATH = "service-path"


class ServiceInstance:
    """A service instance returned by discovering processes"""
    def __init__(
            self, service_name, service_addresses,
            runtime_type, cluster_name=None, tags=None):
        self.service_name = service_name
        self.service_addresses = service_addresses
        self.runtime_type = runtime_type
        self.cluster_name = cluster_name
        self.tags = tags


def get_service_addresses_string(service_addresses):
    # allow two format: host,host,host or host:port,host:port
    return ",".join([get_address_string(
        service_address[0], service_address[1])
                     if service_address[1] else service_address[0]
                     for service_address in service_addresses])


def get_service_addresses_from_string(addresses_string):
    addresses_list = [x.strip() for x in addresses_string.split(',')]
    service_addresses = []
    for address_string in addresses_list:
        address_parts = [x.strip() for x in address_string.split(':')]
        n = len(address_parts)
        if n == 1:
            host = address_parts[0]
            port = 0
        elif n == 2:
            host = address_parts[0]
            port = int(address_parts[1])
        else:
            raise ValueError(
                "Invalid service address find in: {}".format(addresses_string))
        service_addresses.append((host, port))
    return service_addresses

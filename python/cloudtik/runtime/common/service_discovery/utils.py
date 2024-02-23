from cloudtik.core._private.util.core_utils import get_address_string, JSONSerializableObject


class ServiceInstance(JSONSerializableObject):
    """A service instance returned by discovering processes"""
    def __init__(
            self, service_name, service_addresses,
            service_type, runtime_type,
            cluster_name=None, tags=None,
            labels=None):
        self.service_name = service_name
        self.service_addresses = service_addresses
        self.service_type = service_type
        self.runtime_type = runtime_type
        self.cluster_name = cluster_name
        self.tags = tags
        self.labels = labels


def get_service_addresses_string(service_addresses, separator=None):
    # allow two format: host,host,host or host:port,host:port
    if not separator:
        separator = ","
    return separator.join([get_address_string(
        service_address[0], service_address[1])
                     if service_address[1] else service_address[0]
                     for service_address in service_addresses])


def get_service_addresses_from_string(addresses_string, separator=None):
    # allow two format: host,host,host or host:port,host:port
    if not separator:
        separator = ","
    addresses_list = [x.strip() for x in addresses_string.split(separator)]
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

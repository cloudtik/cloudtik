from enum import Enum, auto
from typing import Dict, Any, Optional, Union, List

import ipaddr

from cloudtik.core._private.service_discovery.runtime_services import get_service_discovery_runtime, \
    get_consul_server_addresses
from cloudtik.core._private.service_discovery.utils import ServiceAddressType, include_cluster_for_selector, \
    include_service_name_for_selector, include_runtime_service_for_selector
from cloudtik.core._private.util.runtime_utils import subscribe_cluster_runtime_config, get_runtime_node_address_type, \
    get_runtime_cluster_name
from cloudtik.core._private.utils import RUNTIME_CONFIG_KEY
from cloudtik.runtime.common.service_discovery.consul import query_services_from_consul, \
    query_services_with_addresses as query_services_with_addresses_from_consul, \
    query_services_with_nodes as query_services_with_nodes_from_consul
from cloudtik.runtime.common.service_discovery.workspace import query_services_from_workspace


class DiscoveryType(Enum):
    WORKSPACE = auto()
    CLUSTER = auto()
    LOCAL = auto()
    ANY = auto()


def query_services(
        cluster_config: Dict[str, Any], service_selector,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = ServiceAddressType.NODE_IP,
        first: bool = False):
    runtime_config = cluster_config.get(RUNTIME_CONFIG_KEY)
    if (discovery_type == DiscoveryType.ANY or
            discovery_type == DiscoveryType.LOCAL or
            discovery_type == DiscoveryType.CLUSTER):
        services = query_services_from_discovery(
            runtime_config, service_selector,
            discovery_type=discovery_type,
            address_type=address_type,
            first=first
        )
        if services:
            return services
        if (discovery_type == DiscoveryType.LOCAL or
                discovery_type == DiscoveryType.CLUSTER):
            return None

    if (discovery_type == DiscoveryType.ANY or
            discovery_type == DiscoveryType.WORKSPACE):
        # try workspace discovery
        services = query_services_from_workspace(
            cluster_config, service_selector,
            first=first)
        if services:
            return services
        if discovery_type == DiscoveryType.WORKSPACE:
            return None

    return None


def query_service(
        cluster_config: Dict[str, Any], service_selector,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = ServiceAddressType.NODE_IP):
    return query_services(
        cluster_config, service_selector,
        discovery_type=discovery_type,
        address_type=address_type,
        first=True
    )


def query_services_from_discovery(
        runtime_config: Dict[str, Any], service_selector,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = ServiceAddressType.NODE_IP,
        first: bool = False):
    # Note: the runtime config should be global runtime config.
    # If a node type override the list of runtimes, we may not get the service discovery runtime
    if not get_service_discovery_runtime(runtime_config):
        return None

    if (discovery_type == DiscoveryType.ANY or
            discovery_type == DiscoveryType.LOCAL):
        # try first use service discovery if available
        services = query_services_from_consul(
            service_selector,
            address_type=address_type,
            first=first)
        if services:
            return services
        if discovery_type == DiscoveryType.LOCAL:
            return None

    if (discovery_type == DiscoveryType.ANY or
            discovery_type == DiscoveryType.CLUSTER):
        # For case that the local consul is not yet available
        # If the current cluster is consul server or consul server is not available
        # the address will be None
        addresses = get_consul_server_addresses(runtime_config)
        if addresses is not None:
            services = query_services_from_consul(
                service_selector,
                address_type=address_type,
                address=addresses,
                first=first)
            if services:
                return services
        if discovery_type == DiscoveryType.CLUSTER:
            return None
    return None


def query_services_from_local_discovery(
        service_selector,
        address_type: ServiceAddressType = ServiceAddressType.NODE_IP,
        first: bool = False):
    return query_services_from_consul(
            service_selector,
            address_type=address_type,
            first=first)


def discover_services_from_node(
        cluster_name: Optional[Union[str, List[str]]] = None,
        runtime_type: Optional[Union[str, List[str]]] = None,
        service_type: Optional[Union[str, List[str]]] = None,
        service_name: Optional[Union[str, List[str]]] = None,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = None,
        first: bool = False):
    service_selector = {}
    if cluster_name:
        service_selector = include_cluster_for_selector(
            service_selector, cluster_name)
    service_selector = include_runtime_service_for_selector(
        service_selector, runtime_type, service_type)
    if service_name:
        service_selector = include_service_name_for_selector(
            service_selector, service_name)
    return _discover_services_from_node(
        service_selector,
        discovery_type=discovery_type,
        address_type=address_type,
        first=first)


def _discover_services_from_node(
        service_selector,
        runtime_config: Dict[str, Any] = None,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = None,
        first: bool = False):
    if runtime_config is None:
        # the global runtime config
        runtime_config = subscribe_cluster_runtime_config()

    # raise error instead of return None if service discovery is not available
    if not get_service_discovery_runtime(runtime_config):
        raise RuntimeError("Service discovery runtime is not configured.")

    if address_type is None:
        # auto address type
        address_type = get_runtime_node_address_type()
    return query_services_from_discovery(
        runtime_config, service_selector,
        discovery_type=discovery_type,
        address_type=address_type,
        first=first)


def discover_services_in_cluster(
        runtime_type: Optional[Union[str, List[str]]] = None,
        service_type: Optional[Union[str, List[str]]] = None,
        service_name: Optional[Union[str, List[str]]] = None,
        discovery_type: DiscoveryType = DiscoveryType.ANY,
        address_type: ServiceAddressType = None,
        first: bool = False):
    cluster_name = get_runtime_cluster_name()
    return discover_services_from_node(
        cluster_name, runtime_type,
        service_type=service_type,
        service_name=service_name,
        discovery_type=discovery_type,
        address_type=address_type,
        first=first)


def _get_sorted_service_nodes(services, address_type):
    service_nodes = []
    for service_name, service_instance in services.items():
        service_addresses = service_instance.service_addresses
        for service_address in service_addresses:
            service_node = (service_name, service_address[0], service_address[1])
            service_nodes.append(service_node)

    def sort_node(node):
        name, service_host, service_port = node
        if address_type == ServiceAddressType.NODE_IP:
            node_ip_addr = int(ipaddr.IPAddress(service_host))
            return name, node_ip_addr, service_port
        return name, service_host, service_port

    service_nodes.sort(key=sort_node)
    return service_nodes


def get_service_nodes(
        runtime_type: str = None,
        service_name: str = None,
        service_type: str = None,
        host: bool = False):
    address_type = ServiceAddressType.NODE_IP
    if host:
        # Which address type is available/used at runtime
        address_type = get_runtime_node_address_type()
    services = discover_services_in_cluster(
        runtime_type=runtime_type,
        service_name=service_name,
        service_type=service_type,
        address_type=address_type)
    if not services:
        return None

    return _get_sorted_service_nodes(
        services, address_type)


def get_service_node_addresses(
        runtime_type: str = None,
        service_name: str = None,
        service_type: str = None,
        host: bool = False,
        no_port: bool = False):
    service_nodes = get_service_nodes(
        runtime_type=runtime_type,
        service_name=service_name,
        service_type=service_type,
        host=host)
    if not service_nodes:
        return None

    def get_service_node_address(node):
        name, service_host, service_port = node
        s = service_host
        if not no_port:
            s += ":"
            s += str(service_port)
        return s

    # dict keeps insert ordered
    nodes_addresses = {}
    for service_node in service_nodes:
        node_address = get_service_node_address(service_node)
        if node_address not in nodes_addresses:
            nodes_addresses[node_address] = None
    return list(nodes_addresses.keys())


def query_services_with_nodes(
        service_selector,
        first: bool = False):
    return query_services_with_nodes_from_consul(
        service_selector, first=first)


def query_services_with_addresses(
        service_selector,
        address_type: ServiceAddressType = ServiceAddressType.NODE_IP,
        first: bool = False):
    return query_services_with_addresses_from_consul(
        service_selector, address_type=address_type, first=first)

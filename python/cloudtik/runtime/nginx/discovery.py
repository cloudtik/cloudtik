import logging

from cloudtik.core._private.util.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.service_discovery.utils import deserialize_service_selector
from cloudtik.core._private.util.pull.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.consul \
    import query_services, query_service_nodes, get_service_address_of_node, get_tags_of_service_nodes, \
    get_service_fqdn_address, get_common_label_of_service_nodes
from cloudtik.runtime.common.service_discovery.utils import API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH
from cloudtik.runtime.nginx.scripting import update_load_balancer_configuration, \
    update_api_gateway_dynamic_backends, APIGatewayBackendService, update_api_gateway_dns_backends, \
    APIGatewayDNSBackendService

logger = logging.getLogger(__name__)


class DiscoverJob(PullJob):
    def __init__(
            self,
            service_selector=None,
            balance_method=None):
        self.service_selector = deserialize_service_selector(
            service_selector)
        self.balance_method = balance_method
        self.last_config_hash = None

    def _query_services(self):
        return query_services(self.service_selector)

    def _query_service_nodes(self, service_name):
        return query_service_nodes(service_name, self.service_selector)


class DiscoverBackendServers(DiscoverJob):
    """Pulling job for discovering backend targets and
    update the config if there are new or deleted servers with reload.
    """

    def __init__(
            self,
            service_selector=None,
            balance_method=None):
        super().__init__(service_selector, balance_method)

    def pull(self):
        selected_services = self._query_services()
        backend_servers = {}

        for service_name in selected_services:
            service_nodes = self._query_service_nodes(service_name)
            for service_node in service_nodes:
                server_address = get_service_address_of_node(service_node)
                server_key = get_address_string(server_address[0], server_address[1])
                backend_servers[server_key] = server_address
        if not backend_servers:
            logger.warning("No live servers return from the service selector.")

        # Finally, rebuild the configuration for reloads
        servers_hash = get_json_object_hash(backend_servers)
        if servers_hash != self.last_config_hash:
            # save config file and reload only when data changed
            update_load_balancer_configuration(
                backend_servers, self.balance_method)
            self.last_config_hash = servers_hash


class DiscoverAPIGatewayBackendServers(DiscoverJob):
    """Pulling job for discovering backend targets for API gateway backends
    and update the config if there are new or deleted backends.
    The selectors are used to select the list of services (include a service tag or service cluster)
    The servers are discovered through DNS by service name
    and optionally service tag and service cluster
    """

    def __init__(
            self,
            service_selector=None,
            balance_method=None):
        super().__init__(service_selector, balance_method)
        # TODO: logging the job parameters

    def pull(self):
        selected_services = self._query_services()

        api_gateway_backends = {}
        for service_name in selected_services:
            service_nodes = self._query_service_nodes(service_name)
            if not service_nodes:
                logger.warning("No live servers return from the service selector.")
            else:
                backend_name = service_name
                backend_service = self.get_backend_service(
                    service_name, service_nodes)
                api_gateway_backends[backend_name] = backend_service

        backends_hash = get_json_object_hash(api_gateway_backends)
        if backends_hash != self.last_config_hash:
            # save config file and reload only when data changed
            update_api_gateway_dynamic_backends(
                api_gateway_backends, self.balance_method)
            self.last_config_hash = backends_hash

    @staticmethod
    def get_backend_service(service_name, service_nodes):
        backend_servers = {}
        for service_node in service_nodes:
            server_address = get_service_address_of_node(service_node)
            server_key = get_address_string(server_address[0], server_address[1])
            backend_servers[server_key] = server_address

        route_path = get_common_label_of_service_nodes(
            service_nodes, API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH,
            error_if_not_same=True)

        return APIGatewayBackendService(
            service_name, backend_servers,
            route_path=route_path)


class DiscoverAPIGatewayBackends(DiscoverJob):
    def __init__(
            self,
            service_selector=None,
            balance_method=None):
        super().__init__(service_selector, balance_method)
        # TODO: logging the job parameters

    def pull(self):
        selected_services = self._query_services()

        api_gateway_backends = {}
        for service_name in selected_services:
            service_nodes = self._query_service_nodes(service_name)
            if not service_nodes:
                logger.warning("No live servers return from the service selector.")
            else:
                backend_name = service_name
                backend_service = self.get_dns_backend_service(
                    service_name, service_nodes)
                api_gateway_backends[backend_name] = backend_service

        backends_hash = get_json_object_hash(api_gateway_backends)
        if backends_hash != self.last_config_hash:
            # save config file and reload only when data changed
            update_api_gateway_dns_backends(
                api_gateway_backends)
            self.last_config_hash = backends_hash

    @staticmethod
    def get_dns_backend_service(service_name, service_nodes):
        # get service port in one of the servers
        service_node = service_nodes[0]
        server_address = get_service_address_of_node(service_node)
        service_port = server_address[1]

        # get a common set of tags
        tags = get_tags_of_service_nodes(
            service_nodes)
        service_dns_name = get_service_fqdn_address(
            service_name, tags)

        route_path = get_common_label_of_service_nodes(
            service_nodes, API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH,
            error_if_not_same=True)

        return APIGatewayDNSBackendService(
            service_name, service_port, service_dns_name,
            route_path=route_path)

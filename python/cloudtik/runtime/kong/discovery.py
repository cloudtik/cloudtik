import logging

from cloudtik.core._private.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.service_discovery.utils import deserialize_service_selector
from cloudtik.core._private.util.pull.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.consul \
    import query_services, query_service_nodes, get_service_address_of_node, get_common_label_of_service_nodes, \
    get_service_fqdn_address, get_tags_of_service_nodes
from cloudtik.runtime.common.service_discovery.utils import API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH, \
    API_GATEWAY_SERVICE_DISCOVERY_LABEL_SERVICE_PATH
from cloudtik.runtime.kong.admin_api import add_or_update_api_upstream, \
    delete_api_upstream, UpstreamService, list_services
from cloudtik.runtime.kong.utils import KONG_CONFIG_MODE_DNS, KONG_CONFIG_MODE_RING_DNS

logger = logging.getLogger(__name__)


def _get_new_and_update_upstreams(upstreams, existing_upstreams):
    # for all in upstreams, if it is not in the existing data sources,
    # we need to add new data sources
    new_upstreams = {}
    update_upstreams = {}
    for upstream_name, upstream in upstreams.items():
        if upstream_name not in existing_upstreams:
            new_upstreams[upstream_name] = upstream
        else:
            update_upstreams[upstream_name] = upstream
    return new_upstreams, update_upstreams


def _get_delete_upstreams(upstreams, existing_upstreams):
    # for all the existing data sources that not in upstreams
    # we need to delete
    delete_upstreams = set()
    for upstream_name, upstream in existing_upstreams.items():
        if upstream_name not in upstreams:
            delete_upstreams.add(upstream_name)
    return delete_upstreams


class DiscoverJob(PullJob):
    def __init__(self,
                 service_selector=None,
                 ):
        self.service_selector = deserialize_service_selector(
            service_selector)
        self.last_config_hash = None

    def _query_services(self):
        return query_services(self.service_selector)

    def _query_service_nodes(self, service_name):
        return query_service_nodes(service_name, self.service_selector)


class DiscoverUpstreamServers(DiscoverJob):
    """Pulling job for discovering upstream targets for API gateway upstreams
    """

    def __init__(self,
                 service_selector=None,
                 config_mode=None,
                 balance_method=None,
                 admin_endpoint=None,
                 ):
        super().__init__(service_selector)
        self.config_mode = config_mode
        self.balance_method = balance_method
        self.admin_endpoint = admin_endpoint

    def pull(self):
        selected_services = self._query_services()

        upstreams = {}
        for service_name in selected_services:
            service_nodes = self._query_service_nodes(service_name)
            if not service_nodes:
                logger.warning("No live servers return from the service selector.")
            else:
                upstream_name = service_name
                try:
                    upstream_service = self.get_upstream_service(
                        service_name, service_nodes)
                    upstreams[upstream_name] = upstream_service
                except Exception as e:
                    logger.error(
                        "Failed to get upstream service: {}.".format(
                            str(e)))

        upstreams_hash = get_json_object_hash(upstreams)
        if upstreams_hash != self.last_config_hash:
            self._configure_upstreams(upstreams)
            self.last_config_hash = upstreams_hash

    def get_upstream_service(
            self, service_name, service_nodes):
        route_path = get_common_label_of_service_nodes(
            service_nodes, API_GATEWAY_SERVICE_DISCOVERY_LABEL_ROUTE_PATH,
            error_if_not_same=True)
        service_path = get_common_label_of_service_nodes(
            service_nodes, API_GATEWAY_SERVICE_DISCOVERY_LABEL_SERVICE_PATH,
            error_if_not_same=True)

        service_node = service_nodes[0]
        server_address = get_service_address_of_node(service_node)
        service_port = server_address[1]

        if (self.config_mode == KONG_CONFIG_MODE_DNS or
                self.config_mode == KONG_CONFIG_MODE_RING_DNS):
            # get a common set of tags
            tags = get_tags_of_service_nodes(
                service_nodes)
            service_dns_name = get_service_fqdn_address(
                service_name, tags)

            if self.config_mode == KONG_CONFIG_MODE_DNS:
                # service DNS name with port in service
                return UpstreamService(
                    service_name, service_dns_name=service_dns_name,
                    service_port=service_port,
                    route_path=route_path, service_path=service_path)
            else:
                # a single server target with the service dns name in upstream
                target_address = (service_dns_name, service_port)
                server_key = get_address_string(target_address[0], target_address[1])
                upstream_servers = {server_key: target_address}
                return UpstreamService(
                    service_name, servers=upstream_servers,
                    service_port=service_port,
                    route_path=route_path, service_path=service_path)
        else:
            upstream_servers = {}
            for service_node in service_nodes:
                server_address = get_service_address_of_node(service_node)
                server_key = get_address_string(server_address[0], server_address[1])
                upstream_servers[server_key] = server_address
            return UpstreamService(
                service_name, servers=upstream_servers,
                service_port=service_port,
                route_path=route_path, service_path=service_path)

    def _configure_upstreams(self, upstreams):
        # 1. delete data sources was added but now exists
        # 2. add new data sources
        existing_upstreams = self._query_upstreams()
        new_upstreams, update_upstreams = _get_new_and_update_upstreams(
            upstreams, existing_upstreams)
        delete_upstreams = _get_delete_upstreams(
            upstreams, existing_upstreams)

        self._add_or_update_upstreams(new_upstreams)
        self._add_or_update_upstreams(update_upstreams)
        self._delete_upstreams(delete_upstreams)

    def _query_upstreams(self):
        # This upstream concept are services
        upstreams = list_services(self.admin_endpoint)
        if not upstreams:
            return {}
        return {
            upstream["name"]: upstream
            for upstream in upstreams}

    def _add_or_update_upstreams(self, upstreams):
        for upstream_name, upstream_service in upstreams.items():
            self._add_or_update_upstream(upstream_name, upstream_service)

    def _delete_upstreams(self, delete_upstreams):
        for upstream_name in delete_upstreams:
            self._delete_upstream(upstream_name)

    def _add_or_update_upstream(self, upstream_name, upstream_service):
        try:
            add_or_update_api_upstream(
                self.admin_endpoint, upstream_name,
                self.balance_method, upstream_service)
            logger.info("Upstream {} created or updated.".format(
                upstream_name))
        except Exception as e:
            logger.error("Upstream {} created or updated failed: {}".format(
                upstream_name, str(e)))

    def _delete_upstream(self, upstream_name):
        try:
            delete_api_upstream(
                self.admin_endpoint, upstream_name)
            logger.info("Upstream {} deleted successfully.".format(
                upstream_name))
        except Exception as e:
            logger.error("Upstream {} deletion failed: {}.".format(
                upstream_name, str(e)))

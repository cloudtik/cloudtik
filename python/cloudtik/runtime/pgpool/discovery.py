import logging

from cloudtik.core._private.service_discovery.utils import deserialize_service_selector, ServiceAddressType
from cloudtik.core._private.util.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.util.pull.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.consul import query_services, query_service_nodes, \
    get_service_address_of_node
from cloudtik.runtime.pgpool.scripting import update_configuration, do_health_check

logger = logging.getLogger(__name__)


class DiscoverBackendServers(PullJob):
    """Pulling job for discovering backend targets and update Pgpool conf and reload"""

    def __init__(
            self,
            service_selector=None,
            address_type=None):
        self.service_selector = deserialize_service_selector(
            service_selector)
        if address_type:
            address_type = ServiceAddressType.from_str(address_type)
        else:
            address_type = ServiceAddressType.NODE_IP
        self.address_type = address_type
        self.last_config_hash = None

    def pull(self):
        try:
            self.update_backend()
        except Exception as e:
            logger.exception(
                "Error happened when discovering and updating backend: " + str(e))

        try:
            self.check_health()
        except Exception as e:
            logger.exception(
                "Check health failed: " + str(e))

    def update_backend(self):
        selected_services = self._query_services()
        backend_servers = {}

        for service_name in selected_services:
            service_nodes = self._query_service_nodes(service_name)
            for service_node in service_nodes:
                server_address = get_service_address_of_node(
                    service_node, self.address_type)
                server_key = get_address_string(server_address[0], server_address[1])
                backend_servers[server_key] = server_address
        if not backend_servers:
            logger.warning(
                "No live servers return from the service selector.")

        # Finally, rebuild the configuration for reloads
        servers_hash = get_json_object_hash(backend_servers)
        if servers_hash != self.last_config_hash:
            # save config file and reload only when data changed
            update_configuration(backend_servers)
            self.last_config_hash = servers_hash

    def check_health(self):
        do_health_check()

    def _query_services(self):
        return query_services(self.service_selector)

    def _query_service_nodes(self, service_name):
        return query_service_nodes(service_name, self.service_selector)

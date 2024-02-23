import logging

from cloudtik.core._private.service_discovery.utils import deserialize_service_selector, ServiceAddressType
from cloudtik.core._private.util.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.util.service.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.discovery import query_services_with_addresses
from cloudtik.runtime.pgpool.scripting import update_configuration, do_node_check

logger = logging.getLogger(__name__)


class DiscoverBackendService(PullJob):
    """Pulling job for discovering backend targets and update Pgpool conf and reload"""

    def __init__(
            self,
            interval=None,
            service_selector=None,
            address_type=None):
        super().__init__(interval)
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
            self.check_node()
        except Exception as e:
            logger.exception(
                "Node check failed: " + str(e))

    def update_backend(self):
        selected_services = self._query_services()
        backend_servers = {}
        for service_name, server_addresses in selected_services.items():
            for server_address in server_addresses:
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

    def check_node(self):
        do_node_check()

    def _query_services(self):
        return query_services_with_addresses(
            self.service_selector, self.address_type)

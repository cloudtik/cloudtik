import logging

from cloudtik.core._private.service_discovery.utils import deserialize_service_selector, ServiceAddressType
from cloudtik.core._private.util.core_utils import get_json_object_hash
from cloudtik.core._private.util.service.pull_job import PullJob
from cloudtik.runtime.common.service_discovery.discovery import query_services_from_local_discovery
from cloudtik.runtime.pgbouncer.scripting import update_configuration

logger = logging.getLogger(__name__)


class DiscoverBackendService(PullJob):
    """Pulling job for discovering backend targets and update PgBouncer conf and reload"""

    def __init__(
            self,
            interval=None,
            service_selector=None,
            address_type=None,
            db_user=None,
            db_name=None,
            auth_user=None,
            bind_user=None):
        super().__init__(interval)
        self.service_selector = deserialize_service_selector(
            service_selector)
        if address_type:
            address_type = ServiceAddressType.from_str(address_type)
        else:
            address_type = ServiceAddressType.NODE_IP
        self.address_type = address_type
        self.db_user = db_user
        self.db_name = db_name
        self.auth_user = auth_user
        self.bind_user = True if bind_user == "true" else False
        self.last_config_hash = None

    def pull(self):
        self.update_backend()

    def update_backend(self):
        services = self._query_services()
        # Finally, rebuild the configuration for reloads
        config_hash = get_json_object_hash(services)
        if config_hash != self.last_config_hash:
            # save config file and reload only when data changed
            update_configuration(
                services, self.db_user, self.db_name,
                self.auth_user, self.bind_user)
            self.last_config_hash = config_hash

    def _query_services(self):
        services = query_services_from_local_discovery(
            self.service_selector, self.address_type)
        if services is None:
            services = {}
        return services

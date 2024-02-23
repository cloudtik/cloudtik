import logging

from cloudtik.core._private.service_discovery.utils import deserialize_service_selector
from cloudtik.core._private.util.core_utils import get_json_object_hash, get_address_string
from cloudtik.core._private.util.rest_api import REST_API_AUTH_TYPE, REST_API_AUTH_API_KEY
from cloudtik.core._private.util.service.pull_job import PullJob
from cloudtik.core._private.utils import decrypt_string
from cloudtik.runtime.apisix.admin_api import add_or_update_backend, \
    delete_backend, BackendService, list_services
from cloudtik.runtime.apisix.utils import APISIX_CONFIG_MODE_DNS, APISIX_CONFIG_MODE_CONSUL
from cloudtik.runtime.common.service_discovery.consul \
    import get_service_address_of_node, get_service_fqdn_address, get_tags_of_service_nodes
from cloudtik.runtime.common.service_discovery.discovery import query_services_with_nodes
from cloudtik.runtime.common.service_discovery.load_balancer import \
    get_application_route_from_service_nodes

logger = logging.getLogger(__name__)


def _get_new_and_update_backends(backends, existing_backends):
    # for all in backends, if it is not in the existing data sources,
    # we need to add new data sources
    new_backends = {}
    update_backends = {}
    for backend_name, backend in backends.items():
        if backend_name not in existing_backends:
            new_backends[backend_name] = backend
        else:
            update_backends[backend_name] = backend
    return new_backends, update_backends


def _get_delete_backends(backends, existing_backends):
    # for all the existing backends that not in the latest backends
    # we need to delete
    delete_backends = set()
    for backend_name, backend in existing_backends.items():
        if backend_name not in backends:
            delete_backends.add(backend_name)
    return delete_backends


class DiscoverJob(PullJob):
    def __init__(
            self,
            interval=None,
            service_selector=None,):
        super().__init__(interval)
        self.service_selector = deserialize_service_selector(
            service_selector)
        self.last_config_hash = None

    def _query_services(self):
        return query_services_with_nodes(self.service_selector)


class DiscoverBackendService(DiscoverJob):
    """Pulling job for discovering backend targets for API gateway
    """

    def __init__(
            self,
            interval=None,
            service_selector=None,
            config_mode=None,
            balance_method=None,
            admin_endpoint=None,
            admin_key=None,):
        super().__init__(interval, service_selector)
        self.config_mode = config_mode
        self.balance_method = balance_method
        self.admin_endpoint = admin_endpoint
        admin_key = decrypt_string(admin_key)
        self.auth = {
            REST_API_AUTH_TYPE: REST_API_AUTH_API_KEY,
            REST_API_AUTH_API_KEY: admin_key,
        }

    def pull(self):
        selected_services = self._query_services()
        backends = {}
        for service_name, service_nodes in selected_services.items():
            backend_name = service_name
            try:
                backend_service = self.get_backend_service(
                    service_name, service_nodes)
                backends[backend_name] = backend_service
            except Exception as e:
                logger.error(
                    "Failed to get backend service: {}.".format(
                        str(e)))

        backends_hash = get_json_object_hash(backends)
        if backends_hash != self.last_config_hash:
            self._configure_backends(backends)
            self.last_config_hash = backends_hash

    def get_backend_service(
            self, service_name, service_nodes):
        (route_path,
         service_path,
         default_service) = get_application_route_from_service_nodes(
            service_nodes)

        service_node = service_nodes[0]
        server_address = get_service_address_of_node(service_node)
        service_port = server_address[1]

        if self.config_mode == APISIX_CONFIG_MODE_DNS:
            # get a common set of tags
            tags = get_tags_of_service_nodes(
                service_nodes)
            service_dns_name = get_service_fqdn_address(
                service_name, tags)

            # service DNS name with port in service
            return BackendService(
                service_name, service_dns_name=service_dns_name,
                service_port=service_port,
                route_path=route_path, service_path=service_path,
                default_service=default_service)
        elif self.config_mode == APISIX_CONFIG_MODE_CONSUL:
            # service name with port in service
            return BackendService(
                service_name,
                service_port=service_port,
                route_path=route_path, service_path=service_path,
                default_service=default_service)
        else:
            backend_servers = {}
            for service_node in service_nodes:
                server_address = get_service_address_of_node(service_node)
                server_key = get_address_string(server_address[0], server_address[1])
                backend_servers[server_key] = server_address
            return BackendService(
                service_name, servers=backend_servers,
                service_port=service_port,
                route_path=route_path, service_path=service_path,
                default_service=default_service)

    def _configure_backends(self, backends):
        # 1. delete data sources was added but now exists
        # 2. add new data sources
        existing_backends = self._query_backends()
        new_backends, update_backends = _get_new_and_update_backends(
            backends, existing_backends)
        delete_backends = _get_delete_backends(
            backends, existing_backends)

        self._add_or_update_backends(new_backends)
        self._add_or_update_backends(update_backends)
        self._delete_backends(delete_backends)

    def _query_backends(self):
        # This backend concept are services
        services = list_services(self.admin_endpoint, self.auth)
        if not services:
            return {}
        return {
            service["value"]["id"]: service
            for service in services}

    def _add_or_update_backends(self, backends):
        for backend_name, backend_service in backends.items():
            self._add_or_update_backend(backend_name, backend_service)

    def _delete_backends(self, delete_backends):
        for backend_name in delete_backends:
            self._delete_backend(backend_name)

    def _add_or_update_backend(self, backend_name, backend_service):
        try:
            add_or_update_backend(
                self.admin_endpoint, self.auth, backend_name,
                self.balance_method, backend_service)
            logger.info(
                "Backend {} created or updated.".format(
                    backend_name))
        except Exception as e:
            logger.error(
                "Backend {} created or updated failed: {}".format(
                    backend_name, str(e)))

    def _delete_backend(self, backend_name):
        try:
            delete_backend(
                self.admin_endpoint, self.auth, backend_name)
            logger.info(
                "Backend {} deleted successfully.".format(
                    backend_name))
        except Exception as e:
            logger.error(
                "Backend {} deletion failed: {}.".format(
                    backend_name, str(e)))

import logging
import time

from cloudtik.core._private.service_discovery.utils import deserialize_service_selector
from cloudtik.core._private.util.core_utils import split_list
from cloudtik.runtime.common.active_standby_service import ActiveStandbyService
from cloudtik.runtime.common.service_discovery.consul import \
    get_service_address_of_node, get_common_label_of_service_nodes
from cloudtik.runtime.common.service_discovery.discovery import query_services_with_nodes
from cloudtik.runtime.common.service_discovery.load_balancer import LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_FRONTEND_PORT
from cloudtik.runtime.loadbalancer.scripting import update_backend_configuration, \
    LoadBalancerBackendService

logger = logging.getLogger(__name__)

# print every 30 minutes for repeating errors
LOG_ERROR_REPEAT_SECONDS = 30 * 60
DEFAULT_LOAD_BALANCER_PULL_INTERVAL = 10
DEFAULT_LOAD_BALANCER_LEADER_TTL = 10
DEFAULT_LOAD_BALANCER_LEADER_ELECT_DELAY = 5

LOAD_BALANCER_CONTROLLER_SERVICE_NAME = "load-balancer-controller"


class LoadBalancerController(ActiveStandbyService):
    """Pulling job for discovering backend services for LoadBalancer
    and update LoadBalancer using provider specific API"""

    def __init__(
            self,
            interval=None,
            service_selector=None,
            runtime_types: str = None):
        runtime_types = split_list(runtime_types) if runtime_types else None
        super().__init__(
            runtime_types,
            LOAD_BALANCER_CONTROLLER_SERVICE_NAME,
            leader_ttl=DEFAULT_LOAD_BALANCER_LEADER_TTL,
            leader_elect_delay=DEFAULT_LOAD_BALANCER_LEADER_ELECT_DELAY)
        if not interval:
            interval = DEFAULT_LOAD_BALANCER_PULL_INTERVAL
        self.interval = interval
        self.service_selector = deserialize_service_selector(
            service_selector)
        self.log_repeat_errors = LOG_ERROR_REPEAT_SECONDS // interval
        self.last_error_str = None
        self.last_error_num = 0

    def _run(self):
        self.update()
        time.sleep(self.interval)

    def update(self):
        try:
            self._update()
            if self.last_error_str is not None:
                # if this is a recover from many errors, we print a recovering message
                if self.last_error_num >= self.log_repeat_errors:
                    logger.info(
                        "Recovering from {} repeated errors.".format(self.last_error_num))
                self.last_error_str = None
        except Exception as e:
            error_str = str(e)
            if self.last_error_str != error_str:
                logger.exception(
                    "Error happened when pulling: " + error_str)
                self.last_error_str = error_str
                self.last_error_num = 1
            else:
                self.last_error_num += 1
                if self.last_error_num % self.log_repeat_errors == 0:
                    logger.error(
                        "Error happened {} times for pulling: {}".format(
                            self.last_error_num, error_str))

    def _update(self):
        selected_services = self._query_services()
        backends = {}
        for service_name, service_nodes in selected_services.items():
            backend_service = self.get_backend_service(
                service_name, service_nodes)
            backend_name = service_name
            backends[backend_name] = backend_service

        # Finally, rebuild the LoadBalancer configuration
        update_backend_configuration(backends)

    def _query_services(self):
        return query_services_with_nodes(self.service_selector)

    @staticmethod
    def get_backend_service(service_name, service_nodes):
        backend_servers = []
        for service_node in service_nodes:
            server_address = get_service_address_of_node(service_node)
            backend_servers.append(server_address)

        frontend_port = get_common_label_of_service_nodes(
            service_nodes, LOAD_BALANCER_SERVICE_DISCOVERY_LABEL_FRONTEND_PORT,
            error_if_not_same=True)

        return LoadBalancerBackendService(
            service_name, backend_servers, frontend_port)

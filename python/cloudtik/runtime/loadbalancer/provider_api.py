import logging

from cloudtik.core._private.load_balancer_provider_factory import _get_load_balancer_provider, \
    _get_load_balancer_provider_cls
from cloudtik.core._private.util.core_utils import get_list_for_update, get_json_object_hash
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_PROTOCOL_TCP, LOAD_BALANCER_TYPE_NETWORK, \
    LOAD_BALANCER_SCHEME_INTERNET_FACING, LOAD_BALANCER_PROTOCOL_HTTP, LOAD_BALANCER_PROTOCOL_HTTPS, \
    LOAD_BALANCER_TYPE_APPLICATION
from cloudtik.runtime.common.service_discovery.load_balancer import get_checked_port, ApplicationBackendService

logger = logging.getLogger(__name__)


LOAD_BALANCER_AUTO_CREATED_TAG = "cloudtik-auto-create"
LOAD_BALANCER_NETWORK_DEFAULT = "{}-n"
LOAD_BALANCER_APPLICATION_DEFAULT = "{}-a"


class LoadBalancerBackendService(ApplicationBackendService):
    def __init__(
            self,
            service_name, backend_servers,
            protocol, port,
            load_balancer_name=None, load_balancer_scheme=None,
            load_balancer_protocol=None, load_balancer_port=None,
            route_path=None, service_path=None, default_service=False):
        super().__init__(
            service_name, route_path, service_path, default_service)

        if not protocol:
            protocol = LOAD_BALANCER_PROTOCOL_TCP
        if not port:
            # default to the same port of the backend servers
            service_address = next(iter(backend_servers.values()))
            port = service_address[1]
        if not load_balancer_protocol:
            load_balancer_protocol = protocol
        if not load_balancer_port:
            load_balancer_port = port

        self.backend_servers = backend_servers
        self.protocol = protocol
        self.port = get_checked_port(port)

        self.load_balancer_name = load_balancer_name
        self.load_balancer_protocol = load_balancer_protocol
        self.load_balancer_port = get_checked_port(load_balancer_port)
        self.load_balancer_scheme = load_balancer_scheme


def get_load_balancer_manager(provider_config, workspace_name):
    return LoadBalancerManager(provider_config, workspace_name)


def bootstrap_provider_config(cluster_config, provider_config):
    cluster_provider_config = get_provider_config(cluster_config)
    provider_cls = _get_load_balancer_provider_cls(cluster_provider_config)
    return provider_cls.bootstrap_config(
        cluster_config, provider_config)


def _get_sorted_backend_services(backend_services):
    def sort_by_service_name(backend_service):
        return backend_service.service_name

    return sorted(backend_services, key=sort_by_service_name)


def _get_sorted_service_groups(service_groups):
    def sort_by_service_group_key(service_group):
        service_group_key, _ = service_group
        return service_group_key

    return sorted(service_groups.items(), key=sort_by_service_group_key)


def _sorted_backend_targets(backend_targets):
    def sort_by_address_port(backend_target):
        return backend_target["address"], backend_target["port"]
    backend_targets.sort(key=sort_by_address_port)


class LoadBalancerManager:
    """
    LoadBalancerManager takes control of managing each backend services cases
    and call into load balancer provider API to create a load balancer,
    to create service groups, or update the targets of services.
    """
    def __init__(self, provider_config, workspace_name):
        self.provider_config = provider_config
        self.workspace_name = workspace_name
        self.load_balancer_provider = _get_load_balancer_provider(
            self.provider_config, self.workspace_name)
        self.load_balancer_hash = {}
        self.default_network_load_balancer = self._get_default_network_load_balancer_name()
        self.default_application_load_balancer = self._get_default_application_load_balancer_name()
        self.error_abort = provider_config.get("error_abort", False)
        self.default_load_balancer_scheme = provider_config.get(
            "load_balancer_scheme", LOAD_BALANCER_SCHEME_INTERNET_FACING)

    def update(self, backend_services):
        load_balancers = self._get_load_balancers(backend_services)
        existing_load_balancers = self.load_balancer_provider.list()
        (load_balancers_to_create,
         load_balancers_to_update,
         load_balancers_to_delete) = self._get_load_balancer_for_action(
            load_balancers, existing_load_balancers)
        for load_balancer_name, load_balancer in load_balancers_to_create.items():
            self._create_load_balancer(load_balancer_name, load_balancer)

        for load_balancer_name, load_balancer_to_update in load_balancers_to_update.items():
            load_balancer, existing_load_balancer = load_balancer_to_update
            self._update_load_balancer(
                load_balancer_name, load_balancer, existing_load_balancer)

        for load_balancer_name, existing_load_balancer in load_balancers_to_delete.items():
            if self._is_delete_auto_empty():
                # delete auto created load balancer if no targets
                self._delete_load_balancer(load_balancer_name, existing_load_balancer)

    def _create_load_balancer(
            self, load_balancer_name, load_balancer):
        try:
            self.load_balancer_provider.create(load_balancer)
            self._update_load_balancer_hash(
                load_balancer_name, load_balancer)
        except Exception as e:
            logger.error(
                "Error happened when creating load balancer {}: {}".format(
                    load_balancer_name, str(e)))
            if self.error_abort:
                raise e

    def _update_load_balancer(
            self, load_balancer_name, load_balancer, existing_load_balancer):
        if self._is_load_balancer_updated(
                load_balancer_name, load_balancer):
            try:
                self.load_balancer_provider.update(
                    existing_load_balancer, load_balancer)
                self._update_load_balancer_hash(
                    load_balancer_name, load_balancer)
            except Exception as e:
                logger.error(
                    "Error happened when updating load balancer {}: {}".format(
                        load_balancer_name, str(e)))
                if self.error_abort:
                    raise e

    def _delete_load_balancer(
            self, load_balancer_name, existing_load_balancer):
        try:
            self.load_balancer_provider.delete(
                existing_load_balancer)
            self._clear_load_balancer_hash(load_balancer_name)
        except Exception as e:
            logger.error(
                "Error happened when deleting load balancer {}: {}".format(
                    load_balancer_name, str(e)))
            if self.error_abort:
                raise e

    def _is_delete_auto_empty(self):
        return self.provider_config.get("delete_auto_empty", True)

    def _is_anonymous_prefer_default(self):
        return self.provider_config.get("anonymous_prefer_default", True)

    def _get_default_network_load_balancer_name(self):
        default_load_balancer_name = self.provider_config.get(
            "default_network_load_balancer_name")
        if default_load_balancer_name:
            return default_load_balancer_name
        return LOAD_BALANCER_NETWORK_DEFAULT.format(self.workspace_name)

    def _get_default_application_load_balancer_name(self):
        default_load_balancer_name = self.provider_config.get(
            "default_application_load_balancer_name")
        if default_load_balancer_name:
            return default_load_balancer_name
        return LOAD_BALANCER_APPLICATION_DEFAULT.format(self.workspace_name)

    def _get_load_balancers(self, backend_services):
        load_balancer_plan = self._plan_load_balancers(backend_services)
        load_balancers = {}
        for load_balancer_name, load_balancer_service_groups in load_balancer_plan.items():
            # currently the service group key is a (protocol, port) tuple
            # logically, a service group can have more than one listener
            service_group_key = next(iter(load_balancer_service_groups))
            load_balancer_protocol = service_group_key[0]
            load_balancer_type = self._get_load_balancer_type(load_balancer_protocol)
            # TODO: decide the scheme from backend service
            load_balancer_scheme = self.default_load_balancer_scheme

            load_balancer = {
                "name": load_balancer_name,
                "type": load_balancer_type,
                "scheme": load_balancer_scheme,
                "tags": {
                    LOAD_BALANCER_AUTO_CREATED_TAG: "true"
                }
            }
            self._add_load_balancer_service_groups(
                load_balancer, load_balancer_type, load_balancer_service_groups)
            load_balancers[load_balancer_name] = load_balancer

        return load_balancers

    def _plan_load_balancers(self, backend_services):
        # Note for the planning:
        # First pass: consider the load balancers that was specifically named by the user
        load_balancers_plan_naming = self._get_explicit_naming_plan(
            backend_services)

        # Second pass considering the multiple service group support of the provider
        load_balancers_plan_service_groups = self._get_load_balancer_service_group_plan(
            load_balancers_plan_naming)
        return load_balancers_plan_service_groups

    def _get_explicit_naming_plan(self, backend_services):
        load_balancers_plan_naming = {}
        for service_name, backend_service in backend_services.items():
            load_balancer_name = self._get_explicit_load_balancer_name(
                backend_service)
            if not load_balancer_name:
                # use empty name to mark the services which have no explicit name
                load_balancer_name = ""
            load_balancer_backend_services = get_list_for_update(
                load_balancers_plan_naming, load_balancer_name)
            load_balancer_backend_services.append(backend_service)
        return load_balancers_plan_naming

    def _get_load_balancer_service_group_plan(self, load_balancers_plan_naming):
        load_balancers_plan_service_groups = {}
        for load_balancer_name, load_balancer_backend_services in load_balancers_plan_naming.items():
            if load_balancer_name:
                # For services that explicitly named
                try:
                    self._plan_named_load_balancer(
                        load_balancers_plan_service_groups,
                        load_balancer_name, load_balancer_backend_services)
                except Exception as e:
                    logger.debug(
                        "Configuration conflicts for load balancer {}: {}.".format(
                            load_balancer_name, str(e)))
            else:
                # no name, we make plan for naming (which should be stable)
                self._plan_unnamed_load_balancer(
                    load_balancers_plan_service_groups, load_balancer_backend_services)
        return load_balancers_plan_service_groups

    def _get_load_balancer_type_of_services(self, load_balancer_backend_services):
        backend_service = load_balancer_backend_services[0]
        load_balancer_type = self._get_load_balancer_type(
            backend_service.load_balancer_protocol)
        for backend_service in load_balancer_backend_services[1:]:
            if load_balancer_type != self._get_load_balancer_type(
                    backend_service.load_balancer_protocol):
                raise ValueError(
                    "Load balancer protocol conflicts.")
        return load_balancer_type

    def _get_load_balancer_type(self, load_balancer_protocol):
        type_application = self._is_application_load_balancer(
            load_balancer_protocol)
        if type_application:
            return LOAD_BALANCER_TYPE_APPLICATION
        else:
            return LOAD_BALANCER_TYPE_NETWORK

    def _get_service_groups(self, load_balancer_backend_services):
        service_groups = {}
        for backend_service in load_balancer_backend_services:
            # Currently, we use (protocol, port) as service group key.
            # Logically, it can be more than one (protocol, port) tuples.
            service_group_key = (
                backend_service.load_balancer_protocol,
                backend_service.load_balancer_port)
            service_group_backend_services = get_list_for_update(service_groups, service_group_key)
            service_group_backend_services.append(backend_service)
        return service_groups

    def _plan_named_load_balancer(
            self, load_balancers_plan_service_groups,
            load_balancer_name, load_balancer_backend_services):
        # A named load balancer is either Network load balancer or a
        # Application load balancer.
        # Network load balancer uses TCP, TLS or UDP load balancer protocol.
        # Application load balancer uses HTTP or HTTPS load balancer protocol.

        # For load balancer,
        # Can only have a single service group if multiple service groups is not supported

        # For network load balancer,
        # Not allow different backend services to use the same load balancer service group.
        load_balancer_type = self._get_load_balancer_type_of_services(
            load_balancer_backend_services)
        support_multi_service_group = self.load_balancer_provider.support_multi_service_group()
        service_groups = self._get_service_groups(load_balancer_backend_services)
        if not support_multi_service_group and len(service_groups) > 1:
            raise ValueError(
                "Provider doesn't support load balancer with multiple service groups.")

        if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
            for _, service_group_backend_services in service_groups.items():
                if len(service_group_backend_services) > 1:
                    raise ValueError(
                        "Network load balancer doesn't allow multiple backend services.")
        load_balancers_plan_service_groups[load_balancer_name] = service_groups

    def _plan_unnamed_load_balancer(
            self, load_balancers_plan_service_groups, load_balancer_backend_services):
        # We decide its load balancer type based on load balancer protocol:
        # Network load balancer uses TCP, TLS or UDP load balancer protocol.
        # Application load balancer uses HTTP or HTTPS load balancer protocol.
        application_backend_services = []
        network_load_balancers = []
        for backend_service in load_balancer_backend_services:
            type_application = self._is_application_load_balancer(
                backend_service.load_balancer_protocol)
            if type_application:
                application_backend_services.append(backend_service)
            else:
                network_load_balancers.append(backend_service)

        if network_load_balancers:
            self._plan_unnamed_network_load_balancer(
                load_balancers_plan_service_groups, network_load_balancers)
        if application_backend_services:
            self._plan_unnamed_application_load_balancer(
                load_balancers_plan_service_groups, application_backend_services)

    def _plan_unnamed_network_load_balancer(
            self, load_balancers_plan_service_groups, network_load_balancers):
        support_multi_service_group = self.load_balancer_provider.support_multi_service_group()
        service_groups = self._get_service_groups(network_load_balancers)
        if not support_multi_service_group or not self._is_anonymous_prefer_default():
            for service_group_key, service_group_backend_services in service_groups.items():
                # expand further to a load balancer if one service group has multiple backend services
                for backend_service in service_group_backend_services:
                    load_balancer_name = backend_service.service_name
                    load_balancers_plan_service_groups[load_balancer_name] = {
                        service_group_key: [backend_service]
                    }
        else:
            # TODO: check any service group with multiple backend services
            load_balancers_plan_service_groups[self.default_network_load_balancer] = service_groups

    def _plan_unnamed_application_load_balancer(
            self, load_balancers_plan_service_groups, application_backend_services):
        support_multi_service_group = self.load_balancer_provider.support_multi_service_group()
        service_groups = self._get_service_groups(application_backend_services)
        if not support_multi_service_group or not self._is_anonymous_prefer_default():
            for service_group_key, service_group_backend_services in service_groups.items():
                # each service group will be a load balancer
                # named it by workspace_name-protocol-port
                load_balancer_name = "{}-{}-{}".format(
                    self.workspace_name, service_group_key[0], service_group_key[1])
                load_balancers_plan_service_groups[load_balancer_name] = {
                    service_group_key: service_group_backend_services
                }
        else:
            load_balancers_plan_service_groups[self.default_application_load_balancer] = service_groups

    def _get_explicit_load_balancer_name(
            self, backend_service):
        return backend_service.load_balancer_name

    def _is_application_load_balancer(self, load_balancer_protocol):
        if (load_balancer_protocol == LOAD_BALANCER_PROTOCOL_HTTP
                or load_balancer_protocol == LOAD_BALANCER_PROTOCOL_HTTPS):
            return True
        return False

    def _add_load_balancer_service_groups(
            self, load_balancer, load_balancer_type, load_balancer_service_groups):
        service_groups = get_list_for_update(load_balancer, "service_groups")
        # service groups should be sorted
        sorted_service_groups = _get_sorted_service_groups(
            load_balancer_service_groups)
        for service_group_key, service_group_backend_services in sorted_service_groups:
            # currently, each service group has one listener. logically, it can have more than one
            protocol = service_group_key[0]
            port = service_group_key[1]
            listener = {
                "protocol": protocol,
                "port": port,
            }
            service_group = {
                "listeners": [listener],
            }
            self._add_service_group_services(
                load_balancer_type, service_group, service_group_backend_services)
            service_groups.append(service_group)

    def _add_service_group_services(
            self, load_balancer_type, service_group, backend_services):
        services = get_list_for_update(service_group, "services")
        # we shall sort the services by name to generate stable hash
        sorted_backend_services = _get_sorted_backend_services(backend_services)
        for backend_service in sorted_backend_services:
            backend_servers = backend_service.backend_servers
            backend_targets = [backend_server for backend_server in backend_servers.values()]
            # The sort backend targets by ip and port
            _sorted_backend_targets(backend_targets)

            service = {
                "name": backend_service.service_name,
                "protocol": backend_service.protocol,
                "port": backend_service.port,
                "targets": backend_targets
            }
            if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
                route_path = backend_service.get_route_path()
                if route_path:
                    service["route_path"] = route_path
                service_path = backend_service.get_service_path()
                if service_path:
                    service["service_path"] = service_path
                default_service = backend_service.default_service
                if default_service:
                    service["default"] = default_service

            services.append(service)

    def _get_load_balancer_for_action(
            self, load_balancers, existing_load_balancers):
        load_balancer_to_create = {}
        load_balancer_to_update = {}
        load_balancer_to_delete = {}
        for load_balancer_name, load_balancer in load_balancers.items():
            if load_balancer_name not in existing_load_balancers:
                load_balancer_to_create[load_balancer_name] = load_balancer
            else:
                existing_load_balancer = existing_load_balancers[load_balancer_name]
                load_balancer_to_update[load_balancer_name] = (
                    load_balancer, existing_load_balancer)

        for load_balancer_name, load_balancer in existing_load_balancers.items():
            if not self._is_auto_created(load_balancer):
                continue
            if load_balancer_name not in load_balancers:
                load_balancer_to_delete[load_balancer_name] = load_balancer
        return load_balancer_to_create, load_balancer_to_update, load_balancer_to_delete

    def _is_auto_created(self, load_balancer):
        tags = load_balancer.get("tags",  {})
        return tags.get(LOAD_BALANCER_AUTO_CREATED_TAG, False)

    def _update_load_balancer_hash(
            self, load_balancer_name, load_balancer):
        load_balancer_hash = get_json_object_hash(load_balancer)
        self.load_balancer_hash[load_balancer_name] = load_balancer_hash

    def _is_load_balancer_updated(
            self, load_balancer_name, load_balancer):
        old_load_balancer_hash = self.load_balancer_hash.get(load_balancer_name)
        if not old_load_balancer_hash:
            return True
        load_balancer_hash = get_json_object_hash(load_balancer)
        if load_balancer_hash != old_load_balancer_hash:
            return True
        return False

    def _clear_load_balancer_hash(self, load_balancer_name):
        self.load_balancer_hash.pop(load_balancer_name, None)

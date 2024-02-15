from cloudtik.core._private.load_balancer_provider_factory import _get_load_balancer_provider, \
    _get_load_balancer_provider_cls
from cloudtik.core._private.util.core_utils import JSONSerializableObject, get_list_for_update, get_json_object_hash
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_PROTOCOL_TCP
from cloudtik.runtime.common.service_discovery.load_balancer import LOAD_BALANCER_SERVICE_DISCOVERY_NAME_LABEL

LOAD_BALANCER_AUTO_CREATED_TAG = "cloudtik-auto-create"
LOAD_BALANCER_DEFAULT = "{}"


class LoadBalancerBackendService(JSONSerializableObject):
    def __init__(
            self,
            service_name, backend_servers,
            protocol, port,
            labels=None):
        if not protocol:
            protocol = LOAD_BALANCER_PROTOCOL_TCP
        if not port:
            # default to the same port of the backend servers
            backend_server = backend_servers[0]
            port = backend_server[1]

        self.service_name = service_name
        self.backend_servers = backend_servers
        self.protocol = protocol
        self.port = port
        self.labels = labels


def get_load_balancer_manager(provider_config, workspace_name):
    return LoadBalancerManager(provider_config, workspace_name)


def bootstrap_provider_config(cluster_config, provider_config):
    cluster_provider_config = get_provider_config(cluster_config)
    provider_cls = _get_load_balancer_provider_cls(cluster_provider_config)
    return provider_cls.bootstrap_config(
        cluster_config, provider_config)


class LoadBalancerManager:
    """
    LoadBalancerManager takes control of managing each backends cases
    and call into load balancer provider API to create a load balancer,
    to create a listener, or update the targets of a listener.
    """
    def __init__(self, provider_config, workspace_name):
        self.provider_config = provider_config
        self.workspace_name = workspace_name
        self.load_balancer_provider = _get_load_balancer_provider(
            self.provider_config, self.workspace_name)
        self.load_balancer_last_hash = {}

    def update(self, backends):
        load_balancer_backends = self._get_load_balancer_backends(backends)
        existing_load_balancers = self.load_balancer_provider.list()
        (load_balancers_to_create,
         load_balancers_to_update,
         load_balancers_to_delete) = self._get_load_balancer_for_action(
            load_balancer_backends, existing_load_balancers)
        for load_balancer_name, load_balancer_backend in load_balancers_to_create.items():
            self._create_load_balancer(load_balancer_name, load_balancer_backend)

        for load_balancer_name, load_balancer_backend in load_balancers_to_update.items():
            self._update_load_balancer(load_balancer_name, load_balancer_backend)

        for load_balancer_name in load_balancers_to_delete:
            if self._is_delete_auto_empty():
                # delete auto created load balancer if no targets
                self.load_balancer_provider.delete(load_balancer_name)
            else:
                # the load balancer has no targets, clear it
                self._clear_load_balancer(load_balancer_name)

    def _create_load_balancer(
            self, load_balancer_name, load_balancer_backend):
        self.load_balancer_provider.create(load_balancer_backend)
        self._update_listener_last_hash(
            load_balancer_name, load_balancer_backend)

    def _update_load_balancer(
            self, load_balancer_name, load_balancer_backend):
        if self._is_load_balancer_updated(
                load_balancer_name, load_balancer_backend):
            self.load_balancer_provider.update(load_balancer_backend)
            self._update_listener_last_hash(
                load_balancer_name, load_balancer_backend)

    def _delete_load_balancer(
            self, load_balancer_name):
        self.load_balancer_provider.delete(load_balancer_name)
        self._clear_load_balancer_last_hash(load_balancer_name)

    def _clear_load_balancer(
            self, load_balancer_name):
        load_balancer_backend = {
            "name": load_balancer_name
        }
        self._update_load_balancer(load_balancer_name, load_balancer_backend)

    def _is_delete_auto_empty(self):
        return self.provider_config.get("delete_auto_empty", True)

    def _is_anonymous_prefer_default(self):
        return self.provider_config.get("anonymous_prefer_default", True)

    def _get_load_balancer_backends(self, backends):
        # based on whether the provider supports multi-listener
        # TODO: check the different services with the same frontend port
        load_balancer_backends = {}
        is_multi_listener = self.load_balancer_provider.is_multi_listener()
        # decide with load balancer name tag
        for service_name, load_balancer_backend_service in backends.items():
            if is_multi_listener:
                load_balancer_name = self._get_load_balancer_name(
                    service_name, load_balancer_backend_service)
            else:
                load_balancer_name = service_name
            load_balancer_backend = load_balancer_backends.get(load_balancer_name)
            if load_balancer_backend is None:
                load_balancer_backend = {
                    "name": load_balancer_name,
                    "tags": {
                        LOAD_BALANCER_AUTO_CREATED_TAG: "true"
                    }
                }
                self._add_load_balancer_listener(
                    load_balancer_backend, load_balancer_backend_service)
                load_balancer_backends[load_balancer_name] = load_balancer_backend
            else:
                self._add_load_balancer_listener(
                    load_balancer_backend, load_balancer_backend_service)
        return load_balancer_backends

    def _get_load_balancer_name_label(
            self, load_balancer_backend_service):
        labels = load_balancer_backend_service.labels
        if not labels:
            return None
        load_balancer_name = labels.get(LOAD_BALANCER_SERVICE_DISCOVERY_NAME_LABEL)
        return load_balancer_name

    def _get_load_balancer_name(
            self, service_name, load_balancer_backend_service):
        load_balancer_name = self._get_load_balancer_name_label(
            load_balancer_backend_service)
        if load_balancer_name:
            return load_balancer_name

        if self._is_anonymous_prefer_default():
            return LOAD_BALANCER_DEFAULT.format(self.workspace_name)
        else:
            return service_name

    def _add_load_balancer_listener(
            self, load_balancer_backend, load_balancer_backend_service):
        listeners = get_list_for_update(load_balancer_backend, "listeners")

        backend_targets = [
            {"ip": backend_server[0], "port": backend_server[1]}
            for backend_server in load_balancer_backend_service.backend_servers
        ]
        backend_service_listener = {
            "protocol": load_balancer_backend_service.protocol,
            "port": load_balancer_backend_service.port,
            "targets": backend_targets
        }

        # TODO: convert to the format acceptable for provider API
        listeners.append(backend_service_listener)

    def _get_load_balancer_for_action(
            self, load_balancer_backends, existing_load_balancers):
        load_balancer_to_create = {}
        load_balancer_to_update = {}
        load_balancer_to_delete = {}
        for load_balancer_name, load_balancer_backend in load_balancer_backends.items():
            if load_balancer_name not in existing_load_balancers:
                load_balancer_to_create[load_balancer_name] = load_balancer_backend
            else:
                load_balancer_to_update[load_balancer_name] = load_balancer_backend

        for load_balancer_name, load_balancer in existing_load_balancers.items():
            if not self._is_auto_created(load_balancer):
                continue
            if load_balancer_name not in load_balancer_backends:
                load_balancer_to_delete[load_balancer_name] = load_balancer
        return load_balancer_to_create, load_balancer_to_update, load_balancer_to_delete

    def _is_auto_created(self, load_balancer):
        tags = load_balancer.get("tags",  {})
        return tags.get(LOAD_BALANCER_AUTO_CREATED_TAG, False)

    def _update_listener_last_hash(
            self, load_balancer_name, load_balancer_backend):
        load_balancer_hash = get_json_object_hash(load_balancer_backend)
        self.load_balancer_last_hash[load_balancer_name] = load_balancer_hash

    def _is_load_balancer_updated(
            self, load_balancer_name, load_balancer_backend):
        old_load_balancer_hash = self.load_balancer_last_hash.get(load_balancer_name)
        if not old_load_balancer_hash:
            return True
        load_balancer_hash = get_json_object_hash(load_balancer_backend)
        if load_balancer_hash != old_load_balancer_hash:
            return True
        return False

    def _clear_load_balancer_last_hash(self, load_balancer_name):
        self.load_balancer_last_hash.pop(load_balancer_name, None)

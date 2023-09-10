from cloudtik.core._private.core_utils import get_address_string
from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_post_json, rest_api_delete, \
    rest_api_method_json

REST_API_ENDPOINT_UPSTREAMS = "/upstreams"
REST_API_ENDPOINT_TARGETS = REST_API_ENDPOINT_UPSTREAMS + "/{}/targets"
REST_API_ENDPOINT_SERVICES = "/services"
REST_API_ENDPOINT_ROUTES = "/routes"


class UpstreamService:
    def __init__(self, service_name, servers,
                 route_path=None, service_path=None):
        self.service_name = service_name
        self.servers = servers
        self.route_path = route_path
        self.service_path = service_path

    def get_route_path(self):
        route_path = self.route_path or "/" + self.service_name
        return route_path


def list_entities(admin_endpoint, entities_url):
    endpoint_url = "{}{}".format(
        admin_endpoint, entities_url)

    entities = []
    paged_entities = rest_api_get_json(
                endpoint_url)
    entities.extend(paged_entities.get("data", []))
    next_page_url = paged_entities.get("next", None)

    while next_page_url:
        paged_entities = rest_api_get_json(
            next_page_url)
        entities.extend(paged_entities.get("data", []))
        next_page_url = paged_entities.get("next", None)

    return entities


def list_upstreams(admin_endpoint):
    return list_entities(
        admin_endpoint, REST_API_ENDPOINT_UPSTREAMS)


def get_upstream_endpoint_url(admin_endpoint, upstream_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_UPSTREAMS, upstream_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_upstream(
        admin_endpoint, upstream_name, algorithm):
    endpoint_url = "{}{}".format(
            admin_endpoint, REST_API_ENDPOINT_UPSTREAMS)
    body = {
        "name": upstream_name,
        "algorithm": algorithm,
    }
    upstream = rest_api_post_json(
        endpoint_url, body)
    return upstream


def get_upstream(
        admin_endpoint, upstream_name):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    return rest_api_get_json(endpoint_url)


def update_upstream(
        admin_endpoint, upstream_name, algorithm):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    body = {
        "name": upstream_name,
        "algorithm": algorithm,
    }
    upstream = rest_api_method_json(
        endpoint_url, body, method="PATCH")
    return upstream


def delete_upstream(
        admin_endpoint, upstream_name):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    rest_api_delete(endpoint_url)


def list_upstream_targets(admin_endpoint, upstream_name):
    return list_entities(
        admin_endpoint, REST_API_ENDPOINT_TARGETS.format(upstream_name))


def add_upstream_target(
        admin_endpoint, upstream_name, target_address):
    endpoint = REST_API_ENDPOINT_TARGETS.format(upstream_name)
    endpoint_url = "{}{}".format(
            admin_endpoint, endpoint)
    body = {
        "upstream": {"name": upstream_name},
        "target": target_address,
    }
    target = rest_api_post_json(
        endpoint_url, body)
    return target


def delete_upstream_target(
        admin_endpoint, upstream_name, target_address):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_TARGETS.format(upstream_name), target_address)
    endpoint_url = "{}{}".format(
        admin_endpoint, endpoint)
    rest_api_delete(endpoint_url)


def list_services(admin_endpoint):
    return list_entities(
        admin_endpoint, REST_API_ENDPOINT_SERVICES)


def get_service_endpoint_url(admin_endpoint, service_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_SERVICES, service_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_service(
        admin_endpoint, service_name, upstream_name, service_path=None):
    endpoint_url = "{}{}".format(
            admin_endpoint, REST_API_ENDPOINT_SERVICES)
    body = {
        "name": service_name,
        "host": upstream_name,
    }
    if service_path:
        body["path"] = service_path
    service = rest_api_post_json(
        endpoint_url, body)
    return service


def get_service(
        admin_endpoint, service_name):
    endpoint_url = get_service_endpoint_url(
        admin_endpoint, service_name)
    return rest_api_get_json(endpoint_url)


def delete_service(
        admin_endpoint, service_name):
    endpoint_url = get_service_endpoint_url(
        admin_endpoint, service_name)
    rest_api_delete(endpoint_url)


def list_routes(admin_endpoint):
    return list_entities(
        admin_endpoint, REST_API_ENDPOINT_ROUTES)


def get_route_endpoint_url(admin_endpoint, route_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_ROUTES, route_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_route(
        admin_endpoint, route_name, service_name, route_path):
    endpoint_url = "{}{}".format(
            admin_endpoint, REST_API_ENDPOINT_ROUTES)
    body = {
        "name": route_name,
        "protocols": ["http", "https"],
        "paths": [route_path],
        "strip_path": True,
        "service": {"name": service_name}
    }
    route = rest_api_post_json(
        endpoint_url, body)
    return route


def get_route(
        admin_endpoint, route_name):
    endpoint_url = get_route_endpoint_url(
        admin_endpoint, route_name)
    return rest_api_get_json(endpoint_url)


def delete_route(
        admin_endpoint, route_name):
    endpoint_url = get_route_endpoint_url(
        admin_endpoint, route_name)
    rest_api_delete(endpoint_url)


def _get_new_targets(upstream_servers, existing_targets):
    new_targets = {}
    for target_name, server_address in upstream_servers.items():
        if target_name not in existing_targets:
            new_targets[target_name] = server_address
    return new_targets


def _get_delete_targets(upstream_servers, existing_targets):
    delete_targets = set()
    for target_name, target in existing_targets.items():
        if target_name not in upstream_servers:
            delete_targets.add(target_name)
    return delete_targets


def update_upstream_targets(
        admin_endpoint, upstream_name, upstream_servers):
    targets_list = list_upstream_targets(admin_endpoint, upstream_name)
    existing_targets = {target["target"]: target for target in targets_list}
    new_targets = _get_new_targets(upstream_servers, existing_targets)
    delete_targets = _get_delete_targets(upstream_servers, existing_targets)

    for target_name, server_address in new_targets.items():
        add_upstream_target(
            admin_endpoint, upstream_name,
            get_address_string(server_address[0], server_address[1]))

    for target_name in delete_targets:
        delete_upstream_target(
            admin_endpoint, upstream_name, target_name)


def add_api_upstream(
        admin_endpoint, upstream_name, algorithm, upstream_service: UpstreamService):
    add_upstream(
        admin_endpoint, upstream_name, algorithm)
    update_upstream_targets(
        admin_endpoint, upstream_name, upstream_service.servers)
    if not get_service(admin_endpoint, service_name=upstream_name):
        add_service(
            admin_endpoint, service_name=upstream_name,
            upstream_name=upstream_name, service_path=upstream_service.service_path)
    if not get_route(admin_endpoint, route_name=upstream_name):
        route_path = upstream_service.get_route_path()
        add_route(
            admin_endpoint, route_name=upstream_name,
            service_name=upstream_name, route_path=route_path)


def update_api_upstream(
        admin_endpoint, upstream_name, algorithm, upstream_service: UpstreamService,
        existing_upstream):
    if existing_upstream.get("algorithm") != algorithm:
        update_upstream(
            admin_endpoint, upstream_name, algorithm)
    update_upstream_targets(
        admin_endpoint, upstream_name, upstream_service.servers)
    # TODO update other properties like service path or route path?


def delete_api_upstream(
        admin_endpoint, upstream_name):
    if get_route(admin_endpoint, route_name=upstream_name):
        delete_route(admin_endpoint, route_name=upstream_name)

    if get_service(admin_endpoint, service_name=upstream_name):
        delete_service(admin_endpoint, service_name=upstream_name)

    delete_upstream(
        admin_endpoint, upstream_name)

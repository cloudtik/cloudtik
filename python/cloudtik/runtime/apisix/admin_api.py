import urllib.error

from cloudtik.core._private.util.core_utils import get_address_string, get_config_for_update
from cloudtik.core._private.util.rest_api import rest_api_get_json, rest_api_delete, \
    rest_api_method_json, rest_api_put_json
from cloudtik.runtime.apisix.utils import APISIX_BALANCE_TYPE_ROUND_ROBIN
from cloudtik.runtime.common.service_discovery.load_balancer import ApplicationBackendService

REST_API_ENDPOINT_ADMIN = "/apisix/admin"
REST_API_ENDPOINT_UPSTREAMS = REST_API_ENDPOINT_ADMIN + "/upstreams"
REST_API_ENDPOINT_SERVICES = REST_API_ENDPOINT_ADMIN + "/services"
REST_API_ENDPOINT_ROUTES = REST_API_ENDPOINT_ADMIN + "/routes"

"""
ID's as a text string must be of a length between 1 and 64 characters and
they should only contain uppercase, lowercase, numbers and no special characters
apart from dashes ( - ), periods ( . ) and underscores ( _ )
"""


class BackendService(ApplicationBackendService):
    def __init__(
            self, service_name, servers=None,
            service_dns_name=None, service_port=None,
            route_path=None, service_path=None,
            default_service=False):
        super().__init__(
            service_name, route_path, service_path, default_service)
        self.servers = servers
        self.service_dns_name = service_dns_name
        self.service_port = service_port


def list_entities(admin_endpoint, auth, entities_url):
    endpoint_url = "{}{}".format(
        admin_endpoint, entities_url)

    response = rest_api_get_json(
                endpoint_url, auth=auth)
    entities = response.get("list", [])
    return entities


def get_entity(
        endpoint_url, auth):
    try:
        return rest_api_get_json(endpoint_url, auth=auth)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None


def list_upstreams(admin_endpoint, auth):
    return list_entities(
        admin_endpoint, auth, REST_API_ENDPOINT_UPSTREAMS)


def get_upstream_endpoint_url(admin_endpoint, upstream_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_UPSTREAMS, upstream_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_upstream(
        admin_endpoint, auth, upstream_name, balance_type,
        nodes=None, service_name=None, discovery_type=None):
    if balance_type is None:
        balance_type = APISIX_BALANCE_TYPE_ROUND_ROBIN
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_UPSTREAMS, upstream_name)
    endpoint_url = "{}{}".format(
            admin_endpoint, endpoint)
    body = {
        "type": balance_type,
    }
    if not nodes and not service_name:
        raise ValueError("Either nodes or service name need be specified.")
    if service_name:
        if not discovery_type:
            raise ValueError("Discovery type is need when service name is specified.")
        body["service_name"] = service_name
        body["discovery_type"] = discovery_type
    else:
        # nodes are key table with key=host:port and value= weight
        body["nodes"] = nodes

    # Use PUT for adding upstream with specified id
    upstream = rest_api_put_json(
        endpoint_url, body, auth=auth)
    return upstream


def get_upstream(
        admin_endpoint, auth, upstream_name):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    return get_entity(endpoint_url, auth=auth)


def update_upstream(
        admin_endpoint, auth, upstream_name, balance_type=None,
        nodes=None, service_name=None, discovery_type=None):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    body = {}
    if balance_type:
        body["type"] = balance_type
    if service_name:
        body["service_name"] = service_name
        body["nodes"] = None
    if discovery_type:
        body["discovery_type"] = discovery_type
    if nodes:
        body["nodes"] = nodes
        body["service_name"] = None
        body["discovery_type"] = None
    if not body:
        # no update is needed
        return None
    upstream = rest_api_method_json(
        endpoint_url, body, auth=auth, method="PATCH")
    return upstream


def delete_upstream(
        admin_endpoint, auth, upstream_name):
    endpoint_url = get_upstream_endpoint_url(
        admin_endpoint, upstream_name)
    rest_api_delete(endpoint_url, auth=auth)


def list_services(admin_endpoint, auth):
    return list_entities(
        admin_endpoint, auth, REST_API_ENDPOINT_SERVICES)


def get_service_endpoint_url(admin_endpoint, service_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_SERVICES, service_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_service(
        admin_endpoint, auth, service_name,
        upstream_name, service_path=None, strip_path=None):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_SERVICES, service_name)
    endpoint_url = "{}{}".format(
            admin_endpoint, endpoint)
    body = {
        "upstream_id": upstream_name
    }

    # Note that right / is by default handled
    # because strip /abc/ from /abc/xyz is equivalent with stripping /abc from it.
    # you cannot make a http request without / in the front of the path.
    if strip_path:
        # this makes /abc/ to /abc or / to empty string (which means no strip)
        strip_path = strip_path.rstrip('/')

    if service_path or strip_path:
        plugins = get_config_for_update(body, "plugins")
        proxy_rewrite = get_config_for_update(plugins, "proxy-rewrite")
        # IMPORTANT NOTE: match and strip for two cases
        # match /abc/xyz and strip /abc and replace with /xyz or /service_path/xyz
        # match /abc exactly and strip /abc and replace with / or /service_path
        if strip_path:
            path_regex_prefix = "^" + strip_path + "/(.*)"
        else:
            path_regex_prefix = "^/(.*)"
        if service_path:
            path_uri_prefix = service_path + "/$1"
        else:
            path_uri_prefix = "/$1"
        if strip_path:
            path_regex_exact = "^" + strip_path + "$"
        else:
            path_regex_exact = "^/$"
        if service_path:
            path_uri_exact = service_path
        else:
            path_uri_exact = "/"
        proxy_rewrite["regex_uri"] = [path_regex_prefix, path_uri_prefix, path_regex_exact, path_uri_exact]

    service = rest_api_put_json(
        endpoint_url, body, auth=auth)
    return service


def get_service(
        admin_endpoint, auth, service_name):
    endpoint_url = get_service_endpoint_url(
        admin_endpoint, service_name)
    return get_entity(endpoint_url, auth=auth)


def delete_service(
        admin_endpoint, auth, service_name):
    endpoint_url = get_service_endpoint_url(
        admin_endpoint, service_name)
    rest_api_delete(endpoint_url, auth=auth)


def list_routes(admin_endpoint, auth):
    return list_entities(
        admin_endpoint, auth, REST_API_ENDPOINT_ROUTES)


def get_route_endpoint_url(admin_endpoint, route_name):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_ROUTES, route_name)
    return "{}{}".format(
        admin_endpoint, endpoint)


def add_route(
        admin_endpoint, auth, route_name, service_name, route_path):
    endpoint = "{}/{}".format(
        REST_API_ENDPOINT_ROUTES, route_name)
    endpoint_url = "{}{}".format(
            admin_endpoint, endpoint)
    # IMPORTANT NOTE:
    # /abc will do an exact match
    # /abc/* will match the prefix
    # The route will not strip the path by the default, we need proxy-rewrite to do this.
    if route_path.endswith('/'):
        # if the route path ends with /, we don't need two uris to match
        # /abc/ will use /abc/* to match
        # / will use /* to match
        uris = [route_path + "*"]
    else:
        uris = [route_path, route_path + "/*"]

    body = {
        "uris": uris,
        "service_id": service_name
    }
    route = rest_api_put_json(
        endpoint_url, body, auth=auth)
    return route


def get_route(
        admin_endpoint, auth, route_name):
    endpoint_url = get_route_endpoint_url(
        admin_endpoint, route_name)
    return get_entity(endpoint_url, auth=auth)


def delete_route(
        admin_endpoint, auth, route_name):
    endpoint_url = get_route_endpoint_url(
        admin_endpoint, route_name)
    rest_api_delete(endpoint_url, auth=auth)


def get_upstream_nodes_for_update(upstream, nodes):
    existing_nodes = upstream.get("nodes", {})
    nodes_to_add = []
    for node_key in nodes:
        if node_key not in existing_nodes:
            nodes_to_add.append(node_key)
    nodes_to_delete = []
    for node_key in existing_nodes:
        if node_key not in nodes:
            nodes_to_delete.append(node_key)
    if not nodes_to_add and not nodes_to_delete:
        # no needs to update
        return None
    for node_key in nodes_to_delete:
        nodes[node_key] = None
    return nodes


def add_or_update_backend(
        admin_endpoint, auth, backend_name, algorithm,
        backend_service: BackendService):
    if backend_service.service_dns_name:
        nodes = None
        service_name = get_address_string(
            backend_service.service_dns_name, backend_service.service_port)
        discovery_type = "dns"
    elif backend_service.servers:
        nodes = {
            server_key: 1
            for server_key, server_address
            in backend_service.servers.items()}
        service_name = None
        discovery_type = None
    else:
        # consul
        nodes = None
        service_name = backend_service.service_name
        discovery_type = "consul"

    upstream = get_upstream(admin_endpoint, auth=auth, upstream_name=backend_name)
    if not upstream:
        add_upstream(
            admin_endpoint, auth=auth,
            upstream_name=backend_name, balance_type=algorithm,
            nodes=nodes, service_name=service_name, discovery_type=discovery_type)
    else:
        # check what's changed with the existing upstream
        upstream = upstream["value"]
        if upstream.get("type") == algorithm:
            algorithm = None

        if service_name:
            if upstream.get("service_name") == service_name:
                service_name = None
            if upstream.get("discovery_type") == discovery_type:
                discovery_type = None
        else:
            # for nodes that needs to be deleted, set its weight to None
            nodes = get_upstream_nodes_for_update(upstream, nodes)

        update_upstream(
            admin_endpoint, auth=auth,
            upstream_name=backend_name, balance_type=algorithm,
            nodes=nodes, service_name=service_name, discovery_type=discovery_type)

    # TODO: update other service properties if changed
    service = get_service(admin_endpoint, auth=auth, service_name=backend_name)
    if not service:
        route_path = backend_service.get_route_path()
        add_service(
            admin_endpoint, auth=auth,
            service_name=backend_name, upstream_name=backend_name,
            service_path=backend_service.get_service_path(), strip_path=route_path)

    # TODO: update other route properties if changed
    route = get_route(admin_endpoint, auth=auth, route_name=backend_name)
    if not route:
        route_path = backend_service.get_route_path()
        add_route(
            admin_endpoint, auth=auth,
            route_name=backend_name, service_name=backend_name,
            route_path=route_path)


def delete_backend(
        admin_endpoint, auth, backend_name):
    if get_route(admin_endpoint, auth, route_name=backend_name):
        delete_route(admin_endpoint, auth, route_name=backend_name)

    if get_service(admin_endpoint, auth, service_name=backend_name):
        delete_service(admin_endpoint, auth, service_name=backend_name)

    if get_upstream(admin_endpoint, auth, upstream_name=backend_name):
        delete_upstream(admin_endpoint, auth, upstream_name=backend_name)

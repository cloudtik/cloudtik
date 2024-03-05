from typing import Dict, Any

from cloudtik.core._private.util.core_utils import get_json_object_hash, get_config_for_update, copy_config_key
from cloudtik.core._private.util.load_balancer import get_load_balancer_service_groups, get_service_group_services, \
    get_service_group_listeners, get_load_balancer_config_type, get_load_balancer_config_name, get_service_targets, \
    get_load_balancer_config_scheme, get_load_balancer_public_ips
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_TYPE_NETWORK, LOAD_BALANCER_SCHEME_INTERNET_FACING, \
    LOAD_BALANCER_PROTOCOL_TCP, LOAD_BALANCER_PROTOCOL_TLS, LOAD_BALANCER_PROTOCOL_HTTP, \
    LOAD_BALANCER_PROTOCOL_HTTPS, LOAD_BALANCER_TYPE_APPLICATION, LOAD_BALANCER_SCHEME_INTERNAL
from cloudtik.core.tags import CLOUDTIK_TAG_WORKSPACE_NAME
from cloudtik.providers._private.gcp.config import get_workspace_subnet_name, \
    get_workspace_subnet_name_of_type, GCP_WORKSPACE_PUBLIC_SUBNET, GCP_WORKSPACE_PRIVATE_SUBNET
from cloudtik.providers._private.gcp.utils import wait_for_compute_region_operation, \
    wait_for_compute_global_operation, wait_for_compute_zone_operation, get_network_url, get_subnetwork_url

CLOUDTIK_TAG_LOAD_BALANCER_NAME = "load-balancer-name"
CLOUDTIK_TAG_LOAD_BALANCER_TYPE = "load-balancer-type"
CLOUDTIK_TAG_LOAD_BALANCER_SCHEME = "load-balancer-scheme"
CLOUDTIK_TAG_LOAD_BALANCER_PROTOCOL = "load-balancer-protocol"

LOAD_BALANCERS_HASH_CONTEXT = "load_balancers_hash"
LOAD_BALANCERS_CONTEXT = "load_balancers"
BACKEND_SERVICES_HASH_CONTEXT = "backend_services_hash"

"""

Key Concepts to note for GCP load balancers:

Proxy Network Load Balancers:

A global external proxy Network Load Balancer is implemented on
globally distributed GFEs and supports advanced traffic management
capabilities.

A regional external proxy Network Load Balancer, Regional internal
proxy Network Load Balancer, Cross-region internal proxy Network
Load Balancer are implemented on the open source Envoy proxy software
stack.

Application load balancers:
- Global external Application Load Balancer. This is a global
load balancer that is implemented as a managed service on Google
Front Ends (GFEs).
- Regional external Application Load Balancer, Regional internal
Application Load Balancer, Cross-region internal Application Load
Balancer, these are load balancer that is implemented as a managed
service on the open-source Envoy proxy.

Example components:
- For regional external Application Load Balancers only, a proxy-only subnet
is used to send connections from the load balancer to the backends.
- An external forwarding rule specifies an external IP address, port,
and target HTTP(S) proxy.
- A target HTTP(S) proxy receives a request from the client.
- The HTTP(S) proxy uses a URL map to make a routing determination.
- A backend service distributes requests to healthy backends.
- One or more backends must be connected to the backend service.
- A health check periodically monitors the readiness of your backends.
- Firewall rules for your backends to accept health check probes.
Regional external Application Load Balancers require an additional
firewall rule to allow traffic from the proxy-only subnet to reach
the backends.

Note for Proxy-only subnets:
Proxy-only subnets are only required for all regional and cross-regional
Envoy-based load balancers.
The proxy-only subnet provides a set of IP addresses that Google uses
to run Envoy proxies on your behalf. You must create one proxy-only subnet
in each region of a VPC network where you use regional external Application
Load Balancers. The --purpose flag for this proxy-only subnet is set to
REGIONAL_MANAGED_PROXY.

You must create one proxy-only subnet in each region of a VPC network
where you use load balancers. Each network running these load balancers
need a proxy-only subnet and it will be used automatically.

Notes for internal forwarding rule subnet for internal load balancers:
The internal IP address associated with the forwarding rule can come from a subnet
in the same network and region as the backends. Note the following conditions:
The IP address can (but does not need to) come from the same subnet as the backend.
The IP address must not come from a reserved proxy-only subnet.

For external forwarding rules, we reserve an external IP address (global or regional).
If not specified, it assigns an ephemeral IP address (confirm?)

The forwarding rule's target, and in most cases, also the loadBalancingScheme,
determine the type of IP address that you can use.

Notes for Multiple forwarding rules with a common IP address:

Two or more forwarding rules with the EXTERNAL or EXTERNAL_MANAGED load balancing
scheme can share the same IP address if the following are true:
- The ports used by each forwarding rule don't overlap.
- The Network Service Tiers of each forwarding rule matches the Network Service Tiers
of the external IP address.

For multiple internal forwarding rules to share the same internal IP address,
you must reserve the IP address and set its --purpose flag to SHARED_LOADBALANCER_VIP
    gcloud compute addresses create SHARED_IP_ADDRESS_NAME \
        --region=REGION \
        --subnet=SUBNET_NAME \
        --purpose=SHARED_LOADBALANCER_VIP

# The IP address of the forwarding rule can be static specified or dynamic allocated
from the subnet.


Firewall rules:

For all the load balancers:
- Configure the firewall to allow traffic from the load balancer and
health checker to the instances. For example:
    gcloud compute firewall-rules create fw-allow-health-check \
        --network=default \
        --action=allow \
        --direction=ingress \
        --source-ranges=130.211.0.0/22,35.191.0.0/16 \
        --target-tags=allow-health-check \
        --rules=tcp:80
The target tags define the backend instances. Without the target tags,
the firewall rules apply to all of your backend instances in the VPC network.

For load balancers that needs proxy-subnet (Envoy based proxy):
- An ingress rule that allows connections from the proxy-only subnet to
reach the backends. Replace source-ranges to the actual proxy-only subnet CIDR.
    gcloud compute firewall-rules create fw-allow-proxy-only-subnet \
        --network=lb-network \
        --action=allow \
        --direction=ingress \
        --source-ranges=10.129.0.0/23 \
        --target-tags=allow-proxy-only-subnet \
        --rules=tcp:80

Finally, the network endpoint group to use is zonal.
Zonal NEGs are zonal resources that represent collections of either IP addresses
or IP address and port combinations for Google Cloud resources within a single subnet.

Global network endpoint group = Internet NEG
Regional network endpoint group = Serverless NEG

"""


def _bootstrap_load_balancer_config(
        config: Dict[str, Any], provider_config: Dict[str, Any]):
    cluster_provider_config = get_provider_config(config)
    # copy the related information from cluster provider config to provider config
    copy_config_key(
        cluster_provider_config, provider_config, "type")
    copy_config_key(
        cluster_provider_config, provider_config, "project_id")
    copy_config_key(
        cluster_provider_config, provider_config, "region")
    # TODO: how about to support multiple availability zone
    copy_config_key(
        cluster_provider_config, provider_config, "availability_zone")
    copy_config_key(
        cluster_provider_config, provider_config, "use_working_vpc")

    return provider_config


def _list_load_balancers(
        compute, provider_config, workspace_name):
    # only forwarding rules has capability of labels
    # we first list all region or global forwarding rules so that to get a list of
    # load balancer names (region or global)
    project_id = provider_config["project_id"]

    # list global forwarding rules
    region = None
    forwarding_rules = _list_workspace_forwarding_rules(
        compute, project_id, region, workspace_name)
    # list regional forwarding rules
    region = provider_config["region"]
    regional_forwarding_rules = _list_workspace_forwarding_rules(
        compute, project_id, region, workspace_name)
    forwarding_rules += regional_forwarding_rules

    load_balancer_map = {}
    for forwarding_rule in forwarding_rules:
        load_balancer_info = _get_load_balancer_info_of(forwarding_rule)
        if load_balancer_info:
            load_balancer_name = load_balancer_info["name"]
            load_balancer_map[load_balancer_name] = load_balancer_info

    return load_balancer_map


def _get_load_balancer(
        compute, provider_config, workspace_name,
        load_balancer_name):
    # use list to get the load balancer since there are many variations
    load_balancers = _list_load_balancers(
        compute, provider_config, workspace_name)
    return load_balancers.get(load_balancer_name)


def _create_load_balancer(
        compute, provider_config, workspace_name, vpc_name,
        load_balancer_config, context):
    project_id = provider_config["project_id"]
    region = _get_load_balancer_scope_type(provider_config)
    load_balancer_name = get_load_balancer_config_name(load_balancer_config)

    load_balancer = _get_load_balancer_object(load_balancer_config)

    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)
    load_balancer_context = _get_load_balancer_context(
        load_balancers_context, load_balancer_name)

    _create_services(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, load_balancer_context)

    load_balancer_proxy_rules = _get_load_balancer_proxy_rules(
        load_balancer)
    _create_proxy_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer)
    _update_load_balancer_hash(
        load_balancers_hash_context, load_balancer_name,
        load_balancer_proxy_rules)


def _update_load_balancer(
        compute, provider_config, workspace_name, vpc_name,
        load_balancer, load_balancer_config, context):
    project_id = provider_config["project_id"]
    # If region is None, it is a global load balancer
    region = load_balancer.get("region")
    load_balancer_name = get_load_balancer_config_name(load_balancer_config)

    load_balancer = _get_load_balancer_object(
        load_balancer_config)

    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)
    load_balancer_context = _get_load_balancer_context(
        load_balancers_context, load_balancer_name)

    _update_services(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, load_balancer_context)

    load_balancer_proxy_rules = _get_load_balancer_proxy_rules(
        load_balancer)
    if _is_load_balancer_updated(
            load_balancers_hash_context, load_balancer_name,
            load_balancer_proxy_rules):
        _update_proxy_rules(
            compute, provider_config, project_id, region, vpc_name,
            workspace_name, load_balancer)
        _update_load_balancer_hash(
            load_balancers_hash_context, load_balancer_name,
            load_balancer_proxy_rules)

        # delete the backend services that is not needed
        _delete_services_unused(
            compute, provider_config, project_id, region,
            load_balancer, load_balancer_context)


def _delete_load_balancer(
        compute, provider_config, workspace_name,
        load_balancer: Dict[str, Any], context):
    project_id = provider_config["project_id"]
    # If region is None, it is a global load balancer
    region = load_balancer.get("region")
    load_balancer_name = load_balancer["name"]
    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)

    # delete in reverse order
    _delete_proxy_rules(
        compute, project_id, region,
        load_balancer)

    _delete_services(
        compute, provider_config, project_id, region,
        load_balancer)

    # clear all the contexts
    _clear_load_balancer_hash(
        load_balancers_hash_context, load_balancer_name)
    _clear_load_balancer_context(
        load_balancers_context, load_balancer_name)


# Common shared functions

def _get_resources_context(parent_context, resource_context_key):
    return get_config_for_update(
        parent_context, resource_context_key)


def _update_resource_hash(resources_context, resource_key, resource):
    resource_hash = get_json_object_hash(resource)
    resources_context[resource_key] = resource_hash


def _is_resource_updated(resources_context, resource_key, resource):
    old_resource_hash = resources_context.get(resource_key)
    if not old_resource_hash:
        return True
    resource_hash = get_json_object_hash(resource)
    if resource_hash != old_resource_hash:
        return True
    return False


def _clear_resource_hash(resources_context, resource_key):
    if resource_key:
        resources_context.pop(resource_key, None)


def _get_load_balancer_scope_type(provider_config):
    if provider_config.get("prefer_global", True):
        return None
    else:
        return provider_config["region"]


def _get_load_balancer_context(load_balancers_context, load_balancer_name):
    return _get_resources_context(load_balancers_context, load_balancer_name)


def _clear_load_balancer_context(load_balancers_context, load_balancer_name):
    _clear_resource_hash(load_balancers_context, load_balancer_name)


def _get_load_balancers_hash_context(context):
    return _get_resources_context(
        context, LOAD_BALANCERS_HASH_CONTEXT)


def _update_load_balancer_hash(
        load_balancers_hash_context, load_balancer_name, load_balancer):
    _update_resource_hash(
        load_balancers_hash_context, load_balancer_name, load_balancer)


def _is_load_balancer_updated(
        load_balancers_hash_context, load_balancer_name, load_balancer):
    return _is_resource_updated(
        load_balancers_hash_context, load_balancer_name, load_balancer)


def _clear_load_balancer_hash(load_balancers_hash_context, load_balancer_name):
    _clear_resource_hash(load_balancers_hash_context, load_balancer_name)


def _get_load_balancers_context(context):
    return _get_resources_context(
        context, LOAD_BALANCERS_CONTEXT)


# Backend service hash helper functions

def _get_backend_services_hash_context(load_balancer_context):
    return _get_resources_context(
        load_balancer_context, BACKEND_SERVICES_HASH_CONTEXT)


def _update_backend_service_hash(
        backend_services_hash_context, backend_service):
    backend_service_name = backend_service["name"]
    _update_resource_hash(
        backend_services_hash_context, backend_service_name, backend_service)


def _is_backend_service_updated(backend_services_hash_context, backend_service):
    backend_service_name = backend_service["name"]
    return _is_resource_updated(
        backend_services_hash_context, backend_service_name, backend_service)


def _clear_backend_service_hash(backend_services_hash_context, backend_service_name):
    _clear_resource_hash(backend_services_hash_context, backend_service_name)


def _get_load_balancer_protocol(protocol):
    # valid values are: TCP, SSL, HTTP, HTTPS
    if protocol == LOAD_BALANCER_PROTOCOL_TLS:
        return "SSL"
    return protocol


def _get_load_balancer_scheme(load_balancer):
    scheme = load_balancer["scheme"]
    if scheme == LOAD_BALANCER_SCHEME_INTERNET_FACING:
        return "EXTERNAL_MANAGED"
    else:
        return "INTERNAL_MANAGED"


def _get_load_balancer_service_group(load_balancer_config):
    # we have only one service group
    service_groups = get_load_balancer_service_groups(
        load_balancer_config)
    if not service_groups:
        raise RuntimeError(
            "No service group defined for load balancer.")
    return service_groups[0]


def _get_backend_service_of_service(service):
    backend_service = {
        "name": service["name"],
        "protocol": service["protocol"],
        "port": service["port"],
        "targets": service["targets"],
    }
    return backend_service


def _get_backend_services_of(services):
    backend_services = []
    for service in services:
        backend_service = _get_backend_service_of_service(service)
        backend_services.append(backend_service)
    return backend_services


def _get_route_service_of_service(service):
    route_service = {
        "name": service["name"],
    }
    copy_config_key(service, route_service, "route_path")
    copy_config_key(service, route_service, "service_path")
    copy_config_key(service, route_service, "default")
    return route_service


def _get_route_services_of(services):
    route_services = []
    for service in services:
        route_service = _get_route_service_of_service(service)
        route_services.append(route_service)
    return route_services


def _get_forwarding_rule_name(load_balancer_name, listener, public_ip=None):
    # considering listeners for different ip addresses
    if public_ip:
        return "{}-{}-{}".format(
            load_balancer_name, public_ip["id"], listener["port"])
    else:
        return "{}-{}".format(
            load_balancer_name, listener["port"])


def _get_forwarding_rules_of(
        load_balancer_name, service_group, public_ips):

    listeners = get_service_group_listeners(service_group)
    forwarding_rules = []
    if public_ips:
        for public_ip in public_ips:
            for listener in listeners:
                name = _get_forwarding_rule_name(
                    load_balancer_name, listener, public_ip)
                forwarding_rule = {
                    "name": name,
                    "ip_address": public_ip["id"],
                    "protocol": listener["protocol"],
                    "port": listener["port"],
                }
                forwarding_rules.append(forwarding_rule)
    else:
        for listener in listeners:
            name = _get_forwarding_rule_name(
                load_balancer_name, listener)
            forwarding_rule = {
                "name": name,
                "protocol": listener["protocol"],
                "port": listener["port"],
            }
            forwarding_rules.append(forwarding_rule)
    return forwarding_rules


def _get_load_balancer_object(load_balancer_config):
    # Currently no conversion needs to be done
    load_balancer_name = get_load_balancer_config_name(load_balancer_config)
    load_balancer_type = get_load_balancer_config_type(load_balancer_config)
    load_balancer_scheme = get_load_balancer_config_scheme(load_balancer_config)
    load_balancer = {
        "name": load_balancer_name,
        "type": load_balancer_type,
        "scheme": load_balancer_scheme,
    }
    copy_config_key(load_balancer_config, load_balancer, "tags")

    service_group = _get_load_balancer_service_group(
        load_balancer_config)
    services = get_service_group_services(service_group)

    load_balancer["backend_services"] = _get_backend_services_of(services)
    # convert to forwarding rules
    public_ips = None
    if load_balancer_scheme == LOAD_BALANCER_SCHEME_INTERNET_FACING:
        public_ips = get_load_balancer_public_ips(load_balancer_config)
    load_balancer["forwarding_rules"] = _get_forwarding_rules_of(
        load_balancer_name, service_group, public_ips)
    load_balancer["route_services"] = _get_route_services_of(services)
    return load_balancer


def _get_load_balancer_proxy_rules(load_balancer):
    # proxy rules include listeners and route_services
    load_balancer_proxy_rules = {
        "forwarding_rules": load_balancer["forwarding_rules"],
        "route_services": load_balancer["route_services"]
    }
    return load_balancer_proxy_rules


def _get_load_balancer_backend_services(load_balancer):
    return load_balancer["backend_services"]


def _get_load_balancer_route_services(load_balancer):
    return load_balancer["route_services"]


def _get_load_balancer_forwarding_rules(load_balancer):
    return load_balancer["forwarding_rules"]


def _get_service_route_path(service):
    # The route path should not be empty
    return service.get("route_path", "/")


def _get_load_balancer_first_service(load_balancer):
    services = _get_load_balancer_route_services(load_balancer)
    if not services:
        raise RuntimeError(
            "No service defined for load balancer.")
    return services[0]


def _get_load_balancer_config_protocol(load_balancer):
    forwarding_rules = _get_load_balancer_forwarding_rules(load_balancer)
    if not forwarding_rules:
        raise RuntimeError(
            "No listener defined for load balancer.")
    # get the first listener protocol
    # multiple listeners should not mix protocol
    forwarding_rule = forwarding_rules[0]
    return forwarding_rule["protocol"]


def _get_sorted_services(services, reverse=False):
    def sort_by_route_and_name(service):
        service_name = service["name"]
        route_path = _get_service_route_path(service)
        return [route_path, service_name]
    return sorted(services, key=sort_by_route_and_name, reverse=reverse)


def _get_load_balancer_default_service(load_balancer):
    services = _get_load_balancer_route_services(load_balancer)
    if not services:
        raise RuntimeError(
            "No service defined for load balancer.")
    for service in services:
        if service.get("default", False):
            return service
    # get service with the shortest route path
    sorted_services = _get_sorted_services(services)
    return sorted_services[0]


# Load balancer Helper functions

def _execute_and_wait(compute, project_id, func, region=None, zone=None):
    operation = func().execute()
    if zone:
        wait_for_compute_zone_operation(
            project_id, zone, operation, compute)
    elif region:
        wait_for_compute_region_operation(
            project_id, region, operation, compute)
    else:
        wait_for_compute_global_operation(
            project_id, operation, compute)


def _get_load_balancer_info_of(forwarding_rule):
    labels = forwarding_rule.get("labels")
    if not labels:
        return None
    # all these labels must be set
    load_balancer_name = labels.get(CLOUDTIK_TAG_LOAD_BALANCER_NAME)
    load_balancer_type = labels.get(CLOUDTIK_TAG_LOAD_BALANCER_TYPE)
    load_balancer_scheme = labels.get(CLOUDTIK_TAG_LOAD_BALANCER_SCHEME)
    load_balancer_protocol = labels.get(CLOUDTIK_TAG_LOAD_BALANCER_PROTOCOL)
    if (not load_balancer_name
            or not load_balancer_type
            or not load_balancer_scheme
            or not load_balancer_protocol):
        return None

    load_balancer_info = {
        "name": load_balancer_name,
        "type": load_balancer_type,
        "scheme": load_balancer_scheme,
        "protocol": load_balancer_protocol,
        "tags": labels
    }

    region = forwarding_rule.get("region")
    if region:
        load_balancer_info["region"] = region

    return load_balancer_info


def _list_load_balancer_backend_services(
        compute, project_id, region, load_balancer_name):
    # since backend services doesn't have label capabilities
    # we uses load balancer name prefix matching
    if region:
        response = compute.regionBackendServices().list(
            project=project_id, region=region).execute()
    else:
        response = compute.backendServices().list(
            project=project_id).execute()
    all_backend_services = response.get("items", [])

    # match and strip the prefix
    prefix = "{}-".format(load_balancer_name)
    backend_services = []
    for backend_service in all_backend_services:
        backend_service_name = backend_service["name"]
        if backend_service_name.startswith(prefix):
            service_name = backend_service_name[len(prefix):]
            backend_service["name"] = service_name
            backend_services.append(backend_service)
    return backend_services


def _create_services(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, load_balancer_context):
    backend_services_hash_context = _get_backend_services_hash_context(
        load_balancer_context)
    services = _get_load_balancer_backend_services(load_balancer)
    for service in services:
        _create_service(
            compute, provider_config, project_id, region, vpc_name,
            workspace_name, load_balancer, service)
        _update_backend_service_hash(
            backend_services_hash_context, service)


def _update_services(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, load_balancer_context):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    services = _get_load_balancer_backend_services(load_balancer)
    existing_services = _list_load_balancer_backend_services(
        compute, project_id, region, load_balancer_name)
    (backend_services_to_create,
     backend_services_to_update) = _get_backend_services_for_action(
        services, existing_services)

    backend_services_hash_context = _get_backend_services_hash_context(
        load_balancer_context)

    for service in backend_services_to_create:
        _create_service(
            compute, provider_config, project_id, region, vpc_name,
            workspace_name, load_balancer, service)
        _update_backend_service_hash(
            backend_services_hash_context, service)

    for backend_service_to_update in backend_services_to_update:
        service, backend_service = backend_service_to_update
        if _is_backend_service_updated(backend_services_hash_context, service):
            _update_service(
                compute, provider_config, project_id, region, vpc_name,
                workspace_name, load_balancer, service)
            _update_backend_service_hash(
                backend_services_hash_context, service)


def _delete_services(
        compute, provider_config, project_id, region,
        load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    existing_services = _list_load_balancer_backend_services(
        compute, project_id, region, load_balancer_name)

    for service in existing_services:
        _delete_service(
            compute, provider_config, project_id, region,
            load_balancer, service)


def _get_unused_backend_services(backend_services, services_used):
    backend_services_by_name = {
        service["name"]: service
        for service in backend_services}
    service_used_by_name = {
        service["named"]: service
        for service in services_used}
    return {
        service_name: service
        for service_name, service in backend_services_by_name.items()
        if service_name not in service_used_by_name
    }


def _delete_services_unused(
        compute, provider_config, project_id, region,
        load_balancer, load_balancer_context):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    backend_services = _list_load_balancer_backend_services(
        compute, project_id, region, load_balancer_name)
    services = _get_load_balancer_backend_services(load_balancer)
    backend_services_unused = _get_unused_backend_services(
        backend_services, services)
    backend_services_hash_context = _get_backend_services_hash_context(
        load_balancer_context)

    for service_name, service in backend_services_unused.items():
        _delete_service(
            compute, provider_config, project_id, region,
            load_balancer, service)
        _clear_backend_service_hash(
            backend_services_hash_context, service_name)


def _create_proxy_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer):
    load_balancer_type = load_balancer["type"]
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        _create_url_map(
            compute, project_id, region,
            load_balancer)

    _create_target_proxy(
        compute, project_id, region,
        load_balancer)

    _create_forwarding_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer)


def _update_proxy_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer):
    load_balancer_type = load_balancer["type"]
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        # if there are service and meta changes
        _update_url_map(
            compute, project_id, region,
            load_balancer)

    # if there are service name changes for network load balancer
    _update_target_proxy(
        compute, project_id, region,
        load_balancer)

    # if only there are listener changes
    _update_forwarding_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer)


def _delete_proxy_rules(
        compute, project_id, region,
        load_balancer):
    load_balancer_type = load_balancer["type"]
    _delete_forwarding_rules(
        compute, project_id, region,
        load_balancer)

    _delete_target_proxy(
        compute, project_id, region,
        load_balancer)

    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        # if there are service and meta changes
        _delete_url_map(
            compute, project_id, region,
            load_balancer)


def _get_backend_services_for_action(
        services, existing_backend_services):
    backend_services_create = []
    backend_services_update = []

    # convert to dict for fast search
    services_by_key = {
        service["name"]: service
        for service in services
    }
    existing_backend_services_by_key = {
        backend_service["name"]: backend_service
        for backend_service in existing_backend_services
    }
    for service_name, service in services_by_key.items():
        if service_name not in existing_backend_services_by_key:
            backend_services_create.append(service)
        else:
            backend_service = existing_backend_services_by_key[service_name]
            backend_services_update.append((service, backend_service))
    return backend_services_create, backend_services_update


def _create_service(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, service):
    _create_health_check(
        compute, provider_config, project_id, region,
        load_balancer, service)

    _create_or_update_network_endpoint_groups(
        compute, provider_config, project_id, vpc_name,
        workspace_name, load_balancer, service)

    # create backend service using the health check
    _create_backend_service(
        compute, provider_config, project_id, region,
        load_balancer, service)


def _update_service(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, service):
    # for updating service, we currently only update the targets
    _create_or_update_network_endpoint_groups(
        compute, provider_config, project_id, vpc_name,
        workspace_name, load_balancer, service)

    # Currently, all network endpoint groups are created and set when creating
    # Future to support dynamic network endpoint groups


def _delete_service(
        compute, provider_config, project_id, region,
        load_balancer, service):
    # delete all the components related to the service
    # Note the service name will be used

    # delete the backend service
    _delete_backend_service(
        compute, project_id, region,
        load_balancer, service)

    # delete the network endpoint group
    _delete_network_endpoint_groups(
        compute, provider_config, project_id,
        load_balancer, service)

    # delete the health check
    _delete_health_check(
        compute, project_id, region,
        load_balancer, service)


def _get_health_check_name(load_balancer_name, service):
    service_name = service["name"]
    return "{}-{}".format(load_balancer_name, service_name)


def _get_health_check_of_service(load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    # The name must be 1-63 characters long, and comply with RFC1035.
    # For example, a name that is 1-63 characters long, matches the regular expression `[a-z]([-a-z0-9]*[a-z0-9])?
    name = _get_health_check_name(load_balancer_name, service)
    # generate health check body
    # type TCP, SSL, HTTP, HTTPS
    service_protocol = service["protocol"]
    protocol = _get_load_balancer_protocol(service_protocol)
    health_check = {
        "name": name,
        "type": protocol
    }

    health_check_config = {
        # "port": service_port,
        # "portSpecification": "USE_FIXED_PORT"
        "portSpecification": "USE_SERVING_PORT"
    }
    if service_protocol == LOAD_BALANCER_PROTOCOL_TCP:
        health_check["tcpHealthCheck"] = health_check_config
    elif service_protocol == LOAD_BALANCER_PROTOCOL_TLS:
        health_check["sslHealthCheck"] = health_check_config
    elif service_protocol == LOAD_BALANCER_PROTOCOL_HTTP:
        health_check["httpHealthCheck"] = health_check_config
    elif service_protocol == LOAD_BALANCER_PROTOCOL_HTTPS:
        health_check["httpsHealthCheck"] = health_check_config
    return health_check


def _create_health_check(
        compute, provider_config, project_id, region,
        load_balancer, service):
    health_check = _get_health_check_of_service(load_balancer, service)

    def func():
        if region:
            return compute.regionHealthChecks().insert(
                project=project_id, region=region, body=health_check)
        else:
            return compute.healthChecks().insert(
                project=project_id, body=health_check)
    _execute_and_wait(compute, project_id, func, region=region)


def _delete_health_check(
        compute, project_id, region,
        load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    health_check_name = _get_health_check_name(load_balancer_name, service)

    def func():
        if region:
            return compute.regionHealthChecks().delete(
                project=project_id, region=region, healthCheck=health_check_name)
        else:
            return compute.healthChecks().delete(
                project=project_id, healthCheck=health_check_name)
    _execute_and_wait(compute, project_id, func, region=region)


def _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone):
    service_name = service["name"]
    return "{}-{}-{}-{}".format(
        load_balancer_name, service_name, subnet, zone)


def _get_network_endpoint_group_url(project_id, zone, network_endpoint_group_name):
    return f"projects/{project_id}/zones/{zone}/networkEndpointGroups/{network_endpoint_group_name}"


def _get_resource_url(project_id, region, resource_type, resource_name):
    if region:
        return f"projects/{project_id}/global/{resource_type}/{resource_name}"
    else:
        return f"projects/{project_id}/regions/{region}/{resource_type}/{resource_name}"


def _get_health_check_url(project_id, region, health_check_name):
    return _get_resource_url(project_id, region, "healthChecks", health_check_name)


def _get_backend_service_url(project_id, region, backend_service_name):
    return _get_resource_url(project_id, region, "backendServices", backend_service_name)


def _get_url_map_url(project_id, region, url_map_name):
    return _get_resource_url(project_id, region, "urlMaps", url_map_name)


def _get_compute_full_qualified_url(relative_url):
    return "https://www.googleapis.com/compute/v1/" + relative_url


def _get_workspace_subnet_zones(provider_config):
    availability_zone = provider_config["availability_zone"]
    subnets = [GCP_WORKSPACE_PUBLIC_SUBNET, GCP_WORKSPACE_PRIVATE_SUBNET]
    zones = [availability_zone]
    subnet_zones = []
    for subnet in subnets:
        for zone in zones:
            subnet_zone = (subnet, zone)
            subnet_zones.append(subnet_zone)
    return subnet_zones


def _get_head_worker_targets(service):
    # For simplification, we only support targets with node_id and seq_id
    # which is not available for static configurations
    # If we support multiple zones in the future, we may have to list instances
    # for getting the subnet and zone information of each target.
    targets = get_service_targets(service)
    head_targets = []
    worker_targets = []
    for target in targets:
        seq_id = target.get("seq_id")
        node_id = target.get("node_id")
        if not seq_id or not node_id:
            continue
        # Under the assumption that head node seq id will be always 1
        if int(seq_id) == 1:
            head_targets.append(target)
        else:
            worker_targets.append(target)
    return head_targets, worker_targets


def _get_network_endpoints_of_subnet_zones(
        service, subnet_zones):
    # the work here is categorizing the targets into the corresponding subnet and zones
    # and also we need the VM instance name (id) for creating the NEG endpoint
    # Currently, we handle only one zone (all the target are in the same zone)
    head_targets, worker_targets = _get_head_worker_targets(service)
    network_endpoints_map = {}
    for subnet_zone in subnet_zones:
        subnet, zone = subnet_zone
        if subnet == GCP_WORKSPACE_PUBLIC_SUBNET:
            network_endpoints = head_targets
        else:
            network_endpoints = worker_targets
        network_endpoints_map[subnet_zone] = network_endpoints
    return network_endpoints_map


def _create_or_update_network_endpoint_groups(
        compute, provider_config, project_id, vpc_name,
        workspace_name, load_balancer, service):
    # figuring out the network endpoint groups we need to create or update
    # Notes the following:
    # You must specify the name for each VM endpoint.
    # Each endpoint VM must be located in the same zone as the NEG.
    # Every endpoint in the NEG must be a unique IP address and port combination.
    # A unique endpoint IP address and port combination can be referenced by more than one NEG.
    # Each endpoint VM must have a network interface in the same VPC network as the NEG.
    # Endpoint IP addresses must be associated with the same subnet specified in the NEG.

    # get a list of subnets, and get a list of zones
    # figuring out the endpoint for each subnet and each zone
    # currently we have two subnets and a single zone
    subnet_zones = _get_workspace_subnet_zones(provider_config)
    network_endpoints_map = _get_network_endpoints_of_subnet_zones(
        service, subnet_zones)
    for subnet_zone, network_endpoints in network_endpoints_map.items():
        subnet, zone = subnet_zone
        _create_or_update_network_endpoint_group_of_service(
            compute, provider_config, project_id, vpc_name,
            workspace_name, load_balancer, service,
            subnet, zone, network_endpoints)


def _create_or_update_network_endpoint_group_of_service(
        compute, provider_config, project_id, vpc_name,
        workspace_name, load_balancer, service,
        subnet, zone, network_endpoints):
    network_endpoint_group = _get_network_endpoint_group(
        compute, project_id,
        load_balancer, service, subnet, zone)
    if not network_endpoint_group:
        _create_network_endpoint_group(
            compute, provider_config, project_id, vpc_name,
            workspace_name, load_balancer, service, subnet, zone)

        _add_network_endpoint_group_endpoints(
            compute, project_id,
            load_balancer, service,
            subnet, zone, network_endpoints)
    else:
        # update endpoints
        _update_network_endpoint_group_endpoints(
            compute, project_id,
            load_balancer, service,
            subnet, zone, network_endpoints)


def _get_network_endpoint_group_of_service(
        project_id, region, vpc_name,
        workspace_name, load_balancer, service, subnet, zone):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    name = _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone)
    network = get_network_url(project_id, vpc_name)
    subnet_name = get_workspace_subnet_name_of_type(workspace_name, subnet)
    # Zonal NEGs are zonal resources that represent collections of either IP addresses
    # or IP address and port combinations for Google Cloud resources within a single subnet.
    subnetwork = get_subnetwork_url(project_id, region, subnet_name)
    network_endpoint_group = {
        "name": name,
        "networkEndpointType": "GCE_VM_IP_PORT",
        # The URL of the network to which all network endpoints in the NEG belong
        "network": network,
        "defaultPort": service["port"],
        # Optional URL of the subnetwork to which all network endpoints in the NEG belong.
        "subnetwork": subnetwork,
    }
    return network_endpoint_group


def _get_network_endpoint_group(
        compute, project_id,
        load_balancer, service, subnet, zone):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    network_endpoint_group_name = _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone)
    try:
        network_endpoint_group = compute.networkEndpointGroups().get(
            project=project_id,
            zone=zone,
            networkEndpointGroup=network_endpoint_group_name,
        ).execute()
        return network_endpoint_group
    except Exception:
        return None


def _create_network_endpoint_group(
        compute, provider_config, project_id, vpc_name,
        workspace_name, load_balancer, service, subnet, zone):
    # Should not use the region parameter which is a flag to indicate
    # a global or a regional load balancer.
    # while here we always create a zonal NEG which is bind to a zone of
    # a region.
    region = provider_config["region"]
    network_endpoint_group = _get_network_endpoint_group_of_service(
        project_id, region, vpc_name,
        workspace_name, load_balancer, service, subnet, zone)

    def func():
        # Zonal NEG
        return compute.networkEndpointGroups().insert(
            project=project_id, zone=zone, body=network_endpoint_group)

    _execute_and_wait(compute, project_id, func, zone=zone)
    return network_endpoint_group


def _delete_network_endpoint_groups(
        compute, provider_config, project_id,
        load_balancer, service):
    subnet_zones = _get_workspace_subnet_zones(provider_config)
    for subnet_zone in subnet_zones:
        subnet, zone = subnet_zone
        _delete_network_endpoint_group(
            compute, project_id,
            load_balancer, service, subnet, zone)


def _delete_network_endpoint_group(
        compute, project_id,
        load_balancer, service, subnet, zone):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    network_endpoint_group_name = _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone)

    def func():
        # Zonal NEG
        return compute.networkEndpointGroups().delete(
            project=project_id, zone=zone,
            networkEndpointGroup=network_endpoint_group_name)

    _execute_and_wait(compute, project_id, func, zone=zone)


def _get_endpoints_of_targets(targets):
    network_endpoints = []
    for target in targets:
        instance = target["node_id"]
        ip_address = target["address"]
        port = target["port"]
        network_endpoint = {
            "instance": instance,
            "ipAddress": ip_address,
            "port": port,
        }
        network_endpoints.append(network_endpoint)

    endpoints = {
        "networkEndpoints": network_endpoints
    }
    return endpoints


def _update_network_endpoint_group_endpoints(
        compute, project_id,
        load_balancer, service,
        subnet, zone, network_endpoints):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    network_endpoint_group_name = _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone)

    # decide endpoints to attach or detach
    existing_network_endpoints = _list_network_endpoint_group_endpoints(
        compute, project_id, zone,
        network_endpoint_group_name)
    (endpoints_attach,
     endpoints_to_detach) = _get_endpoints_for_action(
        network_endpoints, existing_network_endpoints)

    if endpoints_attach:
        endpoints = _get_endpoints_of_targets(endpoints_attach)
        _attach_network_endpoints(
            compute, project_id, zone,
            network_endpoint_group_name, endpoints)
    if endpoints_to_detach:
        endpoints = _get_endpoints_of_targets(endpoints_to_detach)
        _detach_network_endpoints(
            compute, project_id, zone,
            network_endpoint_group_name, endpoints)


def _get_endpoints_for_action(network_endpoints, existing_network_endpoints):
    endpoints_attach = []
    endpoints_to_detach = []
    # decide the endpoint by address and port
    # convert to dict for fast search
    endpoints_by_key = {
        (endpoint["address"], endpoint["port"]): endpoint
        for endpoint in network_endpoints
    }
    # The existing endpoint is in the format of
    # {
    #   "networkEndpoint": {
    #       "instance": "instance"
    #       "ipAddress": "x.x.x.x",
    #       "port": x
    #   }
    # }
    existing_endpoints = [
        {
            "node_id": existing_endpoint["networkEndpoint"]["instance"],
            "address": existing_endpoint["networkEndpoint"]["ipAddress"],
            "port": existing_endpoint["networkEndpoint"]["port"]
        }
        for existing_endpoint in existing_network_endpoints
    ]
    existing_endpoints_by_key = {
        (existing_endpoint["address"], existing_endpoint["port"]): existing_endpoint
        for existing_endpoint in existing_endpoints
    }
    for endpoint_key, endpoint in endpoints_by_key.items():
        if endpoint_key not in existing_endpoints_by_key:
            endpoints_attach.append(endpoint)
    for endpoint_key, existing_endpoints in existing_endpoints_by_key.items():
        if endpoint_key not in endpoints_by_key:
            endpoints_to_detach.append(existing_endpoints)
    return endpoints_attach, endpoints_to_detach


def _list_network_endpoint_group_endpoints(
        compute, project_id, zone, network_endpoint_group_name):

    endpoints = []
    paged_endpoints = _get_network_endpoint_group_endpoints(
        compute, project_id, zone, network_endpoint_group_name)
    endpoints.extend(paged_endpoints.get("items", []))
    next_page_token = paged_endpoints.get("nextPageToken", None)

    while next_page_token is not None:
        paged_endpoints = _get_network_endpoint_group_endpoints(
            compute, project_id, zone, network_endpoint_group_name,
            next_page_token=next_page_token)
        endpoints.extend(paged_endpoints.get("items", []))
        next_page_token = paged_endpoints.get("nextPageToken", None)

    return endpoints


def _get_network_endpoint_group_endpoints(
        compute, project_id, zone, network_endpoint_group_name,
        next_page_token=None):
    response = compute.networkEndpointGroups().listNetworkEndpoints(
        project=project_id,
        zone=zone,
        networkEndpointGroup=network_endpoint_group_name,
        pageToken=next_page_token
    ).execute()
    return response


def _add_network_endpoint_group_endpoints(
        compute, project_id,
        load_balancer, service,
        subnet, zone, network_endpoints):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    network_endpoint_group_name = _get_network_endpoint_group_name(
        load_balancer_name, service, subnet, zone)
    endpoints = _get_endpoints_of_targets(network_endpoints)
    _attach_network_endpoints(
        compute, project_id, zone,
        network_endpoint_group_name, endpoints)


def _attach_network_endpoints(
        compute, project_id, zone,
        network_endpoint_group_name, endpoints):
    def func():
        # Zonal NEG
        return compute.networkEndpointGroups().attachNetworkEndpoints(
            project=project_id, zone=zone,
            networkEndpointGroup=network_endpoint_group_name,
            body=endpoints)

    _execute_and_wait(compute, project_id, func, zone=zone)
    return endpoints


def _detach_network_endpoints(
        compute, project_id, zone,
        network_endpoint_group_name, endpoints):
    def func():
        # Zonal NEG
        return compute.networkEndpointGroups().detachNetworkEndpoints(
            project=project_id, zone=zone,
            networkEndpointGroup=network_endpoint_group_name,
            body=endpoints)

    _execute_and_wait(compute, project_id, func, zone=zone)
    return endpoints


def _get_backend_service_name(load_balancer_name, service):
    service_name = service["name"]
    return "{}-{}".format(load_balancer_name, service_name)


def _get_backend_service_backends(
        provider_config, project_id,
        load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    load_balancer_type = get_load_balancer_config_type(load_balancer)
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        balancing_mode = "RATE"
    else:
        balancing_mode = "CONNECTION"

    backends = []
    subnet_zones = _get_workspace_subnet_zones(provider_config)
    for subnet_zone in subnet_zones:
        subnet, zone = subnet_zone
        # The fully-qualified URL of an instance group or network endpoint group (NEG) resource.
        # must use the *fully-qualified* URL (starting with https://www.googleapis.com/) to
        # specify the instance group or NEG.
        network_endpoint_group_name = _get_network_endpoint_group_name(
            load_balancer_name, service, subnet, zone)
        network_endpoint_group_url = _get_network_endpoint_group_url(
            project_id, zone, network_endpoint_group_name)
        network_endpoint_group_full_url = _get_compute_full_qualified_url(
            network_endpoint_group_url)
        backend = {
            # balancingMode application load balancer uses "RATE"
            "balancingMode": balancing_mode,
            "group": network_endpoint_group_full_url,
        }
        backends.append(backend)
    return backends


def _get_backend_service(
        provider_config, project_id, region,
        load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    name = _get_backend_service_name(load_balancer_name, service)
    scheme = _get_load_balancer_scheme(load_balancer)

    service_protocol = service["protocol"]
    protocol = _get_load_balancer_protocol(service_protocol)
    backends = _get_backend_service_backends(
        provider_config, project_id,
        load_balancer, service)
    health_check_name = _get_health_check_name(
        load_balancer_name, service)
    health_check_url = _get_health_check_url(
        project_id, region, health_check_name)

    backend_service = {
        "name": name,
        "loadBalancingScheme": scheme,
        "protocol": protocol,
        # ROUND_ROBIN, LEAST_REQUEST, RING_HASH, RANDOM, ORIGINAL_DESTINATION, MAGLEV
        "localityLbPolicy": "ROUND_ROBIN",
        "backends": backends,
        "healthChecks": [
            # Currently, at most one health check can be specified for each backend service.
            health_check_url,
        ]
    }
    return backend_service


def _create_backend_service(
        compute, provider_config, project_id, region,
        load_balancer, service):
    backend_service = _get_backend_service(
        provider_config, project_id, region,
        load_balancer, service)

    def func():
        if region:
            return compute.regionBackendServices().insert(
                project=project_id, region=region, body=backend_service)
        else:
            return compute.backendServices().insert(
                project=project_id, body=backend_service)

    _execute_and_wait(compute, project_id, func, region=region)
    return backend_service


def _delete_backend_service(
        compute, project_id, region,
        load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    backend_service_name = _get_backend_service_name(
        load_balancer_name, service)

    def func():
        if region:
            return compute.regionBackendServices().delete(
                project=project_id, region=region, backendService=backend_service_name)
        else:
            return compute.backendServices().delete(
                project=project_id, backendService=backend_service_name)

    _execute_and_wait(compute, project_id, func, region=region)


def _get_target_proxy_name(load_balancer_name):
    return load_balancer_name


def _get_target_proxy(
        project_id, region,
        load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    name = _get_target_proxy_name(load_balancer_name)

    target_proxy = {
        "name": name,
    }

    load_balancer_type = get_load_balancer_config_type(load_balancer)
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        # URL to the BackendService resource.
        # there is only one service
        service = _get_load_balancer_first_service(load_balancer)
        backend_service_name = _get_backend_service_name(
            load_balancer_name, service)
        backend_service_url = _get_backend_service_url(
            project_id, region, backend_service_name)
        target_proxy["service"] = backend_service_url
    else:
        # # URL to the UrlMap resource that defines the mapping from URL to the BackendService.
        url_map_name = _get_url_map_name(
            load_balancer_name)
        url_map_url = _get_url_map_url(
            project_id, region, url_map_name)
        target_proxy["urlMap"] = url_map_url
    return target_proxy


def _get_target_proxy_url(
        project_id, region, load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    target_proxy_name = _get_target_proxy_name(
        load_balancer_name)

    load_balancer_type = get_load_balancer_config_type(load_balancer)
    load_balancer_protocol = _get_load_balancer_config_protocol(load_balancer)
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        resource_type = "targetTcpProxies"
    else:
        if load_balancer_protocol == LOAD_BALANCER_PROTOCOL_HTTP:
            resource_type = "targetHttpProxies"
        else:
            resource_type = "targetHttpsProxies"
    return _get_resource_url(project_id, region, resource_type, target_proxy_name)


def _get_target_proxy_api(
        compute, region, load_balancer_type, load_balancer_protocol):
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        if region:
            return compute.regionTargetTcpProxies()
        else:
            return compute.targetTcpProxies()
    else:
        if load_balancer_protocol == LOAD_BALANCER_PROTOCOL_HTTP:
            if region:
                return compute.regionTargetHttpProxies()
            else:
                return compute.targetHttpProxies()
        else:
            if region:
                return compute.regionTargetHttpsProxies()
            else:
                return compute.targetHttpsProxies()


def _create_target_proxy(
        compute, project_id, region,
        load_balancer):
    load_balancer_type = get_load_balancer_config_type(load_balancer)
    load_balancer_protocol = _get_load_balancer_config_protocol(load_balancer)
    target_proxy = _get_target_proxy(
        project_id, region,
        load_balancer)

    def func():
        target_proxy_api = _get_target_proxy_api(
            compute, region, load_balancer_type, load_balancer_protocol)
        if region:
            return target_proxy_api.insert(
                project=project_id, region=region, body=target_proxy)
        else:
            return target_proxy_api.insert(
                project=project_id, body=target_proxy)

    _execute_and_wait(compute, project_id, func, region=region)
    return target_proxy


def _update_target_proxy(
        compute, project_id, region,
        load_balancer):
    load_balancer_type = get_load_balancer_config_type(load_balancer)
    # TODO: why region target TCP proxy doesn't have method for set the backend service
    if load_balancer_type != LOAD_BALANCER_TYPE_NETWORK or region:
        return

    load_balancer_name = get_load_balancer_config_name(load_balancer)
    target_proxy_name = _get_target_proxy_name(load_balancer_name)

    # only for the case the backend service changed
    service = _get_load_balancer_first_service(load_balancer)
    backend_service_name = _get_backend_service_name(
        load_balancer_name, service)
    backend_service_url = _get_backend_service_url(
        project_id, region, backend_service_name)
    set_backend_service = {
        # The URL of the new BackendService resource for the targetTcpProxy.
        "service": backend_service_url,
    }

    def func():
        return compute.targetTcpProxies().setBackendService(
            project=project_id, targetTcpProxy=target_proxy_name, body=set_backend_service)

    _execute_and_wait(compute, project_id, func, region=region)
    return set_backend_service


def _delete_target_proxy(
        compute, project_id, region,
        load_balancer):
    load_balancer_type = get_load_balancer_config_type(load_balancer)
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    target_proxy_name = _get_target_proxy_name(load_balancer_name)

    # This is set load balancer label and returned in the list
    load_balancer_protocol = load_balancer["protocol"]

    def func():
        if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
            if region:
                return compute.regionTargetTcpProxies().delete(
                    project=project_id, region=region, targetTcpProxy=target_proxy_name)
            else:
                return compute.targetTcpProxies().delete(
                    project=project_id, targetTcpProxy=target_proxy_name)
        else:
            if load_balancer_protocol == LOAD_BALANCER_PROTOCOL_HTTP:
                if region:
                    return compute.regionTargetHttpProxies().delete(
                        project=project_id, region=region, targetHttpProxy=target_proxy_name)
                else:
                    return compute.targetHttpProxies().delete(
                        project=project_id, targetHttpProxy=target_proxy_name)
            else:
                if region:
                    return compute.regionTargetHttpsProxies().delete(
                        project=project_id, region=region, targetHttpsProxy=target_proxy_name)
                else:
                    return compute.targetHttpsProxies().delete(
                        project=project_id, targetHttpsProxy=target_proxy_name)
    _execute_and_wait(compute, project_id, func, region=region)


def _create_forwarding_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer):
    forwarding_rules = _get_load_balancer_forwarding_rules(load_balancer)
    for forwarding_rule in forwarding_rules:
        _create_forwarding_rule(
            compute, provider_config, project_id, region, vpc_name,
            workspace_name, load_balancer, forwarding_rule)


def _update_forwarding_rules(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer):
    forwarding_rules = _get_load_balancer_forwarding_rules(load_balancer)
    existing_forwarding_rules = _list_load_balancer_forwarding_rules(
        compute, project_id, region, load_balancer)

    (forwarding_rules_to_create,
     forwarding_rules_to_update,
     forwarding_rules_to_delete) = _get_forwarding_rules_for_action(
        forwarding_rules, existing_forwarding_rules)

    for forwarding_rule in forwarding_rules_to_create:
        _create_forwarding_rule(
            compute, provider_config, project_id, region, vpc_name,
            workspace_name, load_balancer, forwarding_rule)

    # currently we don't update a forwarding rule

    for forwarding_rule in forwarding_rules_to_delete:
        _delete_forwarding_rule(
            compute, project_id, region, forwarding_rule)


def _delete_forwarding_rules(
        compute, project_id, region,
        load_balancer):
    existing_forwarding_rules = _list_load_balancer_forwarding_rules(
        compute, project_id, region, load_balancer)
    for forwarding_rule in existing_forwarding_rules:
        _delete_forwarding_rule(
            compute, project_id, region, forwarding_rule)


def _get_forwarding_rules_for_action(forwarding_rules, existing_forwarding_rules):
    forwarding_rules_create = []
    forwarding_rules_update = []
    forwarding_rules_to_delete = []
    # decide the forwarding_rule by protocol and port
    # convert to dict for fast search
    forwarding_rules_by_key = {
        forwarding_rule["name"]: forwarding_rule
        for forwarding_rule in forwarding_rules
    }
    existing_forwarding_rules_by_key = {
        forwarding_rule["name"]: forwarding_rule
        for forwarding_rule in existing_forwarding_rules
    }
    for forwarding_rule_key, forwarding_rule in forwarding_rules_by_key.items():
        if forwarding_rule_key not in existing_forwarding_rules_by_key:
            forwarding_rules_create.append(forwarding_rule)
        else:
            load_balancer_forwarding_rule = existing_forwarding_rules_by_key[forwarding_rule_key]
            forwarding_rules_update.append((forwarding_rule, load_balancer_forwarding_rule))

    for forwarding_rule_key, existing_forwarding_rules in existing_forwarding_rules_by_key.items():
        if forwarding_rule_key not in forwarding_rules_by_key:
            forwarding_rules_to_delete.append(existing_forwarding_rules)
    return forwarding_rules_create, forwarding_rules_update, forwarding_rules_to_delete


def _list_forwarding_rules(
        compute, project_id, region, filter_expr):
    if region:
        response = compute.forwardingRules().list(
            project=project_id, region=region, filter=filter_expr).execute()
    else:
        response = compute.globalForwardingRules().list(
            project=project_id, filter=filter_expr).execute()
    return response.get("items", [])


def _list_load_balancer_forwarding_rules(
        compute, project_id, region, load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    filter_expr = '(labels.{key} = {value})'.format(
        key=CLOUDTIK_TAG_LOAD_BALANCER_NAME, value=load_balancer_name)
    return _list_forwarding_rules(
        compute, project_id, region, filter_expr)


def _list_workspace_forwarding_rules(
        compute, project_id, region, workspace_name):
    filter_expr = '(labels.{key} = {value})'.format(
        key=CLOUDTIK_TAG_WORKSPACE_NAME, value=workspace_name)
    return _list_forwarding_rules(
        compute, project_id, region, filter_expr)


def _get_forwarding_rule(
        provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, forwarding_rule):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    load_balancer_type = get_load_balancer_config_type(load_balancer)
    load_balancer_scheme = get_load_balancer_config_scheme(load_balancer)
    load_balancer_protocol = _get_load_balancer_config_protocol(load_balancer)

    scheme = _get_load_balancer_scheme(load_balancer)
    name = forwarding_rule["name"]
    listener_port = str(forwarding_rule["port"])

    target_proxy_url = _get_target_proxy_url(
        project_id, region, load_balancer)

    labels = {
        CLOUDTIK_TAG_WORKSPACE_NAME: workspace_name,
        CLOUDTIK_TAG_LOAD_BALANCER_NAME: load_balancer_name,
        CLOUDTIK_TAG_LOAD_BALANCER_TYPE: load_balancer_type,
        CLOUDTIK_TAG_LOAD_BALANCER_SCHEME: load_balancer_scheme,
        CLOUDTIK_TAG_LOAD_BALANCER_PROTOCOL: load_balancer_protocol,
    }
    tags = load_balancer.get("tags")
    if tags:
        labels.update(tags)

    # The range with the same value for single port
    port_range = "{}-{}".format(listener_port, listener_port)

    network_tier = provider_config.get("network_tier")
    if not network_tier:
        network_tier = "PREMIUM"

    forwarding_rule_body = {
        "name": name,
        # The IP protocol to which this rule applies
        # TCP or (SSL - for only Global external proxy Network Load Balancer)
        "IPProtocol": "TCP",
        # IP address for which this forwarding rule accepts traffic.
        # You can optionally specify an IP address that references an existing static (reserved) IP address resource.
        # When omitted, Google Cloud assigns an ephemeral IP address.
        # "IPAddress": "A String",
        # port range format, for example: "80-80"
        "portRange": port_range,
        "loadBalancingScheme": scheme,
        # The URL of the target resource to receive the matched traffic.
        # For regional forwarding rules, this target must be in the same region as the forwarding rule.
        # For global forwarding rules, this target must be a global load balancing resource.
        "target": target_proxy_url,
        "labels": labels,
        # This signifies the networking tier used for configuring this load balancer
        # and can only take the following values: PREMIUM, STANDARD.
        # For regional ForwardingRule, the valid values are PREMIUM and STANDARD.
        # For GlobalForwardingRule, the valid value is PREMIUM.
        # If this field is not specified, it is assumed to be PREMIUM.
        # If IPAddress is specified, this value must be equal to the networkTier of the Address.
        "networkTier": network_tier,
    }

    # handle static IP address assignment
    ip_address = forwarding_rule.get("ip_address")
    if ip_address:
        # static IP address assignment if there is one
        forwarding_rule_body["IPAddress"] = ip_address

    # This field is not used for global external load balancing.
    # If the subnetwork is specified, the network of the subnetwork will be used.
    # If neither subnetwork nor this field is specified, the default network will be used.
    if (load_balancer_scheme != LOAD_BALANCER_SCHEME_INTERNET_FACING
            or region):
        network = get_network_url(project_id, vpc_name)
        forwarding_rule_body["network"] = network

        # This field identifies the subnetwork that the load balanced IP
        # should belong to for this forwarding rule, used with internal
        # load balancers and external passthrough Network Load Balancers with IPv6.

        # Usually we can just use the workspace private subnet to allocate the private
        # IP address.

        # external application load balancers or external proxy network load balancers doesn't
        # to specify the subnetwork field
        # TODO: DOC error which specify subnetwork for external regional proxy network load balancer?
        if load_balancer_scheme == LOAD_BALANCER_SCHEME_INTERNAL:
            subnet_name = get_workspace_subnet_name(workspace_name)
            subnetwork = get_subnetwork_url(project_id, region, subnet_name)
            forwarding_rule_body["subnetwork"] = subnetwork
    return forwarding_rule_body


def _create_forwarding_rule(
        compute, provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, forwarding_rule):
    forwarding_rule_body = _get_forwarding_rule(
        provider_config, project_id, region, vpc_name,
        workspace_name, load_balancer, forwarding_rule)

    def func():
        if region:
            return compute.forwardingRules().insert(
                project=project_id, region=region, body=forwarding_rule_body)
        else:
            return compute.globalForwardingRules().insert(
                project=project_id, body=forwarding_rule_body)

    _execute_and_wait(compute, project_id, func, region=region)
    return forwarding_rule_body


def _delete_forwarding_rule(
        compute, project_id, region,
        forwarding_rule):
    forwarding_rule_name = forwarding_rule["name"]

    def func():
        if region:
            return compute.forwardingRules().delete(
                project=project_id, region=region, forwardingRule=forwarding_rule_name)
        else:
            return compute.globalForwardingRules().insert(
                project=project_id, forwardingRule=forwarding_rule_name)

    _execute_and_wait(compute, project_id, func, region=region)


def _get_url_map_name(
        load_balancer_name):
    return load_balancer_name


def _get_url_map(
        project_id, region,
        load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    name = _get_url_map_name(load_balancer_name)
    path_rules = _get_path_rules(
        project_id, region,
        load_balancer)

    default_service = _get_load_balancer_default_service(load_balancer)
    default_backend_service_name = _get_backend_service_name(
        load_balancer_name, default_service)
    default_backend_service_url = _get_backend_service_url(
        project_id, region, default_backend_service_name)

    url_map = {
        "name": name,
        # The full or partial URL of the defaultService resource to which traffic is directed
        # if none of the hostRules match.
        "defaultService": default_backend_service_url,
        # The list of host rules to use against the URL.
        "hostRules": [  # The list of host rules to use against the URL.
            {
                # The list of host patterns to match.
                # They must be valid hostnames with optional port numbers
                # in the format host:port. * matches any string of ([a-z0-9-.]*).
                "hosts": ["*"],
                # The name of the PathMatcher to use to match the path portion of the URL
                # if the hostRule matches the URL's host portion.
                "pathMatcher": "allpaths",
            },
        ],
        # The list of named PathMatchers to use against the URL.
        "pathMatchers": [
            {
                # The name to which this PathMatcher is referred by the HostRule.
                "name": "allpaths",
                # The full or partial URL to the BackendService resource.
                # This URL is used if none of the pathRules or routeRules defined by this PathMatcher are matched.
                "defaultService": default_backend_service_url,
                # The list of path rules.
                # Use this list instead of routeRules when routing based on simple
                # path matching is all that's required.
                "pathRules": path_rules,
            }
        ]
    }

    default_route_action = _get_rule_action_of_service(default_service)
    if default_route_action:
        # WARNING: the default service should route by path / otherwise
        # not sure how it works otherwise as to the pathPrefixRewrite for default service.
        # defaultRouteAction takes effect when none of the hostRules match.
        url_map["defaultRouteAction"] = default_route_action
        # defaultRouteAction takes effect when none of the pathRules or routeRules match.
        url_map["pathMatchers"][0]["defaultRouteAction"] = default_route_action
    return url_map


def _get_path_rules(
        project_id, region,
        load_balancer):
    path_rules = []
    services = _get_load_balancer_route_services(load_balancer)
    for service in services:
        path_rule = _get_path_rule_of_service(
            project_id, region,
            load_balancer, service)
        path_rules.append(path_rule)
    return path_rules


def _get_path_rule_of_service(
        project_id, region,
        load_balancer, service):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    route_path = _get_service_route_path(service)
    if route_path.endswith('/'):
        # if the route path ends with /, we don't need two paths to match
        # /abc/ will use /abc/* to match (it will not match /abc)
        # / will use /* to match
        paths = [route_path + '*']
    else:
        paths = [route_path, route_path + '/*']

    backend_service_name = _get_backend_service_name(
        load_balancer_name, service)
    backend_service_url = _get_backend_service_url(
        project_id, region, backend_service_name)
    path_rule = {
        # The list of path patterns to match. Each must start with / and the only place a * is allowed
        # is at the end following a /. The string fed to the path matcher does not include any text
        # after the first ? or #, and those chars are not allowed here.
        "paths": paths,
        "service": backend_service_url,
    }

    # check whether url rewrite is needed
    route_action = _get_rule_action_of_service(service)
    if route_action:
        path_rule["routeAction"] = route_action
    return path_rule


def _get_rule_action_of_service(service):
    # check whether url rewrite is needed
    route_path = _get_service_route_path(service)
    strip_path = route_path
    if strip_path:
        # this makes /abc/ to /abc or / to empty string (which means no strip)
        strip_path = strip_path.rstrip('/')
    service_path = service.get("service_path")
    if not strip_path and not service_path:
        return None
    # handle url rewrite using prefix rewrite
    # Before forwarding the request to the selected backend service,
    # the matching portion of the request's path is replaced by pathPrefixRewrite.
    # The value must be from 1 to 1024 characters.
    # This means that if /abc/* matches "/abc/" and is replaced by service_path/
    # The original service path is striped with / and now we add it backed
    rewrite_path = service_path or ""
    rewrite_path += "/"
    route_action = {
        "urlRewrite": {
            "pathPrefixRewrite": rewrite_path
        }
    }
    return route_action


def _create_url_map(
        compute, project_id, region,
        load_balancer):
    url_map = _get_url_map(
        project_id, region,
        load_balancer)

    def func():
        if region:
            return compute.regionUrlMaps().insert(
                project=project_id, region=region, body=url_map)
        else:
            return compute.urlMaps().insert(
                project=project_id, body=url_map)

    _execute_and_wait(compute, project_id, func, region=region)
    return url_map


def _update_url_map(
        compute, project_id, region,
        load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    url_map_name = _get_url_map_name(load_balancer_name)
    url_map = _get_url_map(
        project_id, region,
        load_balancer)

    def func():
        if region:
            return compute.regionUrlMaps().update(
                project=project_id, region=region, urlMap=url_map_name,
                body=url_map)
        else:
            return compute.urlMaps().update(
                project=project_id, urlMap=url_map_name, body=url_map)

    _execute_and_wait(compute, project_id, func, region=region)
    return url_map


def _delete_url_map(
        compute, project_id, region,
        load_balancer):
    load_balancer_name = get_load_balancer_config_name(load_balancer)
    url_map_name = _get_url_map_name(load_balancer_name)

    def func():
        if region:
            return compute.regionUrlMaps().delete(
                project=project_id, region=region, urlMap=url_map_name)
        else:
            return compute.urlMaps().delete(
                project=project_id, urlMap=url_map_name)

    _execute_and_wait(compute, project_id, func, region=region)

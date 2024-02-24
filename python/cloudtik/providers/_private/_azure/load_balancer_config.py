from typing import Dict, Any

from cloudtik.core._private.util.core_utils import get_json_object_hash, get_config_for_update, copy_config_key
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_TYPE_NETWORK, LOAD_BALANCER_SCHEMA_INTERNET_FACING, \
    LOAD_BALANCER_PROTOCOL_TCP, LOAD_BALANCER_PROTOCOL_UDP, LOAD_BALANCER_PROTOCOL_TLS, LOAD_BALANCER_PROTOCOL_HTTP, \
    LOAD_BALANCER_PROTOCOL_HTTPS
from cloudtik.providers._private._azure.config import get_virtual_network_name, get_workspace_subnet_name

BACKEND_POOLS_HASH_CONTEXT = "backend_pools_hash"


def _get_resources_context(parent_context, resource_context_key):
    return get_config_for_update(
        parent_context, resource_context_key)


def _update_resource_last_hash(resources_context, resource_key, resource):
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


def _clear_resource_last_hash(resources_context, resource_key):
    if resource_key:
        resources_context.pop(resource_key, None)


def _bootstrap_load_balancer_config(
        config: Dict[str, Any], provider_config: Dict[str, Any]):
    cluster_provider_config = get_provider_config(config)
    # copy the related information from cluster provider config to provider config
    copy_config_key(
        cluster_provider_config, provider_config, "type")
    copy_config_key(
        cluster_provider_config, provider_config, "subscription_id")
    copy_config_key(
        cluster_provider_config, provider_config, "resource_group")
    copy_config_key(
        cluster_provider_config, provider_config, "location")
    copy_config_key(
        cluster_provider_config, provider_config, "use_working_vpc")
    copy_config_key(
        cluster_provider_config, provider_config, "managed_identity_client_id")
    return provider_config


def _get_load_balancer_context(context, load_balancer_name):
    return _get_resources_context(context, load_balancer_name)


def _clear_load_balancer_context(context, load_balancer_name):
    _clear_resource_last_hash(context, load_balancer_name)


def _list_load_balancers(network_client, resource_group_name):
    load_balancers = _list_network_load_balancers(
        network_client, resource_group_name)
    return load_balancers


def _get_load_balancer(
        network_client, resource_group_name, load_balancer_name,
        load_balancer_type):
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        return _get_network_load_balancer(
            network_client, resource_group_name, load_balancer_name)


def _create_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_type = load_balancer_config["type"]
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        return _create_network_load_balancer(
            network_client, provider_config, workspace_name,
            load_balancer_config, context)


def _update_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_type = load_balancer_config["type"]
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        return _update_network_load_balancer(
            network_client, provider_config, workspace_name,
            load_balancer_config, context)


def _delete_load_balancer(
        network_client, resource_group_name,
        load_balancer: Dict[str, Any], context):
    load_balancer_type = load_balancer["type"]
    load_balancer_name = load_balancer["name"]
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        _delete_network_load_balancer(
            network_client, resource_group_name, load_balancer_name)

    _clear_load_balancer_context(context, load_balancer_name)


###############################
# Azure Load Balancer functions
###############################

def _list_network_load_balancers(network_client, resource_group_name):
    load_balancers = _list_workspace_load_balancers(
        network_client, resource_group_name)
    load_balancer_map = {}
    for load_balancer in load_balancers:
        load_balancer_name = _get_load_balancer_name(load_balancer)
        load_balancer_info = _get_load_balancer_info_of(load_balancer)
        load_balancer_map[load_balancer_name] = load_balancer_info
    return load_balancer_map


def _get_network_load_balancer(
        network_client, resource_group_name, load_balancer_name):
    load_balancer = _get_load_balancer_by_name(
        network_client, resource_group_name, load_balancer_name)
    if not load_balancer:
        return None
    return _get_load_balancer_info_of(load_balancer)


def _create_network_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_name = load_balancer_config["name"]

    load_balancer = _get_load_balancer_object(
        provider_config, workspace_name, load_balancer_config)

    resource_group_name = provider_config["resource_group"]
    _create_or_update_load_balancer(
        network_client, resource_group_name,
        load_balancer_name, load_balancer)


def _update_network_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_name = load_balancer_config["name"]

    load_balancer = _get_load_balancer_object(
        provider_config, workspace_name, load_balancer_config)

    resource_group_name = provider_config["resource_group"]
    _create_or_update_load_balancer(
        network_client, resource_group_name,
        load_balancer_name, load_balancer)


def _delete_network_load_balancer(
        network_client, resource_group_name, load_balancer_name):
    load_balancer = _get_load_balancer_by_name(
        network_client, resource_group_name, load_balancer_name)
    if not load_balancer:
        return
    network_client.load_balancers.begin_delete(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
    ).result()


# Azure load balancer Helper functions

def _get_load_balancer_protocol(protocol):
    if protocol == LOAD_BALANCER_PROTOCOL_TCP:
        return "Tcp"
    elif protocol == LOAD_BALANCER_PROTOCOL_TLS:
        return "Tls"
    elif protocol == LOAD_BALANCER_PROTOCOL_UDP:
        return "Udp"
    elif protocol == LOAD_BALANCER_PROTOCOL_HTTP:
        return "Http"
    elif protocol == LOAD_BALANCER_PROTOCOL_HTTPS:
        return "Https"
    else:
        raise ValueError(
            "Invalid protocol: {}".format(protocol))


def _get_load_balancer_id(load_balancer):
    return load_balancer.id


def _get_load_balancer_name(load_balancer):
    return load_balancer.name


def _list_workspace_load_balancers(network_client, resource_group_name):
    load_balancers = network_client.load_balancers.list(
        resource_group_name=resource_group_name,
    )
    if load_balancers is None:
        return []
    return list(load_balancers)


def _get_load_balancer_by_name(
        network_client, resource_group_name, load_balancer_name):
    try:
        response = network_client.load_balancers.get(
            resource_group_name=resource_group_name,
            load_balancer_name=load_balancer_name,
        )
        return response
    except Exception:
        return None


def _get_load_balancer_info_of(load_balancer):
    load_balancer_id = _get_load_balancer_id(load_balancer)
    load_balancer_name = _get_load_balancer_name(load_balancer)
    load_balancer_type = LOAD_BALANCER_TYPE_NETWORK
    # TODO: decide the schema
    load_balancer_schema = LOAD_BALANCER_SCHEMA_INTERNET_FACING
    load_balancer_info = {
        "id": load_balancer_id,
        "name": load_balancer_name,
        "type": load_balancer_type,
        "schema": load_balancer_schema,
    }
    tags = load_balancer.tags
    if tags:
        load_balancer_info["tags"] = tags
    return load_balancer_info


def _get_load_balancer_object(
        provider_config, workspace_name, load_balancer_config):
    location = provider_config["location"]
    tags = load_balancer_config.get("tags", None)

    virtual_network_name = get_virtual_network_name(
        provider_config, workspace_name)

    frontend_ip_configurations = _get_load_balancer_frontend_ip_configurations(
        provider_config, workspace_name, load_balancer_config,
        virtual_network_name)
    backend_address_pools = _get_load_balancer_backend_address_pools(
        provider_config, load_balancer_config, virtual_network_name)
    load_balancing_rules = _get_load_balancer_load_balancing_rules(
        provider_config, load_balancer_config, frontend_ip_configurations)
    probes = _get_load_balancer_probes(
        load_balancer_config)

    load_balancer_object = {
        "location": location,
        "sku": {"name": "Standard"},
        "tags": tags,
        "properties": {
            "frontendIPConfigurations": frontend_ip_configurations,
            "backendAddressPools": backend_address_pools,
            "loadBalancingRules": load_balancing_rules,
            "probes": probes,
        }
    }
    return load_balancer_object


def _get_load_balancer_frontend_ip_configurations(
        provider_config, workspace_name, virtual_network_name, load_balancer_config):
    frontend_ip_configurations = []
    # for an internet facing load balancer, it should have public IP configuration,
    # while for internal load balancer, it should have private ip configuration
    load_balancer_schema = load_balancer_config["schema"]
    if load_balancer_schema == LOAD_BALANCER_SCHEMA_INTERNET_FACING:
        # TODO: need to configure a public IP (passed from user)
        # Currently we support one IP, it can be more than one
        pass
    else:
        frontend_ip_configuration = _get_private_front_ip_configuration(
            provider_config, workspace_name, virtual_network_name)
        frontend_ip_configurations.append(frontend_ip_configuration)

    return frontend_ip_configurations


def _get_virtual_network_resource_id(provider_config, virtual_network):
    subscription_id = provider_config["subscription_id"]
    resource_group_name = provider_config["resource_group"]
    return "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/virtualNetworks/{}".format(
        subscription_id, resource_group_name, virtual_network)


def _get_private_front_ip_configuration(
        provider_config, workspace_name, virtual_network_name):
    # use the private subnet
    subnet_name = get_workspace_subnet_name(
        workspace_name, is_private=True)
    virtual_network_id = _get_virtual_network_resource_id(
        provider_config, virtual_network_name)
    subnet_id = virtual_network_id + "/subnets/{}".format(subnet_name)
    frontend_ip_name = "ip-1"
    frontend_ip_configuration = {
        "name": frontend_ip_name,
        "properties": {
            "subnet": {
                "id": subnet_id,
            },
            "privateIPAllocationMethod": "Dynamic",
        },
    }
    return frontend_ip_configuration


def _get_load_balancer_backend_address_pools(
        provider_config, load_balancer_config, virtual_network_name):
    backend_address_pools = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        backend_address_pool = _get_backend_address_pool_of_service(
            provider_config, virtual_network_name, service_name, service)
        backend_address_pools.append(backend_address_pool)
    return backend_address_pools


def _get_backend_address_pool_of_service(
        provider_config, virtual_network_name,  service_name, service):
    # The name of the resource that is unique within the set of backend address pools
    # used by the load balancer. This name can be used to access the resource.
    load_balancer_backend_addresses = _get_load_balancer_backend_addresses(
        provider_config, virtual_network_name, service)
    backend_address_pool = {
        "name": service_name,
        "properties": {
          "loadBalancerBackendAddresses": load_balancer_backend_addresses,
        }
    }
    return backend_address_pool


def _get_load_balancer_backend_addresses(
        provider_config, virtual_network_name, service):
    load_balancer_backend_addresses = []
    targets = service.get("targets", [])
    virtual_network_id = _get_virtual_network_resource_id(
        provider_config, virtual_network_name)
    service_name = service["name"]
    for i, target in enumerate(targets, start=1):
        ip_address = target[0]
        name = "{}-address-{}".format(service_name, i)
        load_balancer_backend_address = {
            "name": name,
            "properties": {
                "ip_address": ip_address,
                "virtualNetwork": {
                    "id": virtual_network_id,
                },
            }
        }
        load_balancer_backend_addresses.append(load_balancer_backend_address)
    return load_balancer_backend_addresses


def _get_load_balancer_load_balancing_rules(
        provider_config, load_balancer_config, frontend_ip_configurations):
    load_balancer_name = load_balancer_config["name"]
    load_balancing_rules = []

    # rules repeated based on the number of front ip configurations
    for frontend_ip_configuration in frontend_ip_configurations:
        service_groups = load_balancer_config.get("service_groups", [])
        for service_group in service_groups:
            # The listeners of the service group cannot overlap
            listeners = service_group.get("listeners", [])
            for listener in listeners:
                load_balancing_rule = _get_service_group_load_balancing_rule(
                    provider_config, load_balancer_name,
                    frontend_ip_configuration, service_group, listener)
                load_balancing_rules.append(load_balancing_rule)
    return load_balancing_rules


def _get_load_balancer_resource_id(provider_config, load_balancer_name):
    subscription_id = provider_config["subscription_id"]
    resource_group_name = provider_config["resource_group"]
    return "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.Network/loadBalancers/{}".format(
        subscription_id, resource_group_name, load_balancer_name)


def _get_load_balancing_rule_name(frontend_ip_name, listener, service):
    service_name = service["name"]
    protocol = listener["protocol"]
    port = listener["port"]
    return "{}-{}-{}-{}".format(service_name, frontend_ip_name, protocol, port)


def _get_service_group_load_balancing_rule(
        provider_config, load_balancer_name,
        frontend_ip_configuration, service_group, listener):
    # this will remove unrelated attributes for listener
    services = _get_service_group_services(service_group)
    if not services:
        raise RuntimeError(
            "No service defined for service group.")
    # There should be only one service
    service = services[0]
    protocol = _get_load_balancer_protocol(listener["protocol"])

    load_balancer_resource_id = _get_load_balancer_resource_id(
        provider_config, load_balancer_name)

    service_name = service["name"]
    backend_address_pool_id = load_balancer_resource_id + "/backendAddressPools/{}".format(
        service_name)
    probe_id = load_balancer_resource_id + "probes/{}".format(
        service_name)

    frontend_ip_name = frontend_ip_configuration["name"]
    front_ip_configuration_id = load_balancer_resource_id + "frontendIPConfigurations/{}".format(
        frontend_ip_name)

    name = _get_load_balancing_rule_name(
        frontend_ip_name, listener, service)

    load_balancing_rule = {
        "name": name,
        "properties": {
            "protocol": protocol,
            "frontendPort": listener["port"],
            "backendPort": service["port"],
            "frontendIPConfiguration": {
                "id": front_ip_configuration_id
            },
            "backendAddressPool": {
                "id": backend_address_pool_id
            },
            "probe": {
                "id": probe_id
            },
            "enableFloatingIP": True,
            "enableTcpReset": False,
            "idleTimeoutInMinutes": 5,
            "loadDistribution": "Default",
        }
    }
    return load_balancing_rule


def _get_load_balancer_probes(
        load_balancer_config):
    probes = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        probe = _get_probe_of_service(
            service_name, service)
        probes.append(probe)
    return probes


def _get_probe_of_service(
            service_name, service):
    probe = {
        "name": service_name,
        "properties": {
            "protocol": 'Tcp',
            "port": service["port"],
            "intervalInSeconds": 15,
            "numberOfProbes": 2,
            "probeThreshold": 1,
        },
    }
    return probe


def _create_or_update_load_balancer(
        network_client, resource_group_name,
        load_balancer_name, load_balancer):
    response = network_client.load_balancers.begin_create_or_update(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
        parameters=load_balancer).result()
    return response


def _get_service_group_services(service_group):
    return service_group.get("services", [])


def _get_load_balancer_services(load_balancer_config):
    service_groups = load_balancer_config.get("service_groups", [])
    load_balancer_services = {}
    for service_group in service_groups:
        services = _get_service_group_services(service_group)
        load_balancer_services.update(
            {service["name"]: service for service in services})
    return load_balancer_services


def _get_backend_pool_name(
        load_balancer_name, service):
    service_name = service["name"]
    return "{}-{}".format(load_balancer_name, service_name)

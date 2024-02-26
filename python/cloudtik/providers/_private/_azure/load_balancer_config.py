from typing import Dict, Any

from cloudtik.core._private.util.core_utils import get_json_object_hash, get_config_for_update, copy_config_key
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_TYPE_NETWORK, LOAD_BALANCER_SCHEMA_INTERNET_FACING, \
    LOAD_BALANCER_PROTOCOL_TCP, LOAD_BALANCER_PROTOCOL_UDP, LOAD_BALANCER_PROTOCOL_TLS, LOAD_BALANCER_PROTOCOL_HTTP, \
    LOAD_BALANCER_PROTOCOL_HTTPS, LOAD_BALANCER_TYPE_APPLICATION, LOAD_BALANCER_SCHEMA_INTERNAL
from cloudtik.providers._private._azure.config import get_virtual_network_name, get_workspace_subnet_name
from cloudtik.providers._private._azure.utils import get_virtual_network_resource_id, get_network_resource_id

LOAD_BALANCERS_HASH_CONTEXT = "load_balancers_hash"
LOAD_BALANCERS_CONTEXT = "load_balancers"


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


def _get_load_balancer_id(load_balancer):
    return load_balancer.id


def _get_load_balancer_name(load_balancer):
    return load_balancer.name


def _get_load_balancer_context(load_balancers_context, load_balancer_name):
    return _get_resources_context(load_balancers_context, load_balancer_name)


def _clear_load_balancer_context(load_balancers_context, load_balancer_name):
    _clear_resource_last_hash(load_balancers_context, load_balancer_name)


def _get_load_balancers_hash_context(context):
    return _get_resources_context(
        context, LOAD_BALANCERS_HASH_CONTEXT)


def _update_load_balancer_last_hash(
        load_balancers_hash_context, load_balancer):
    load_balancer_name = load_balancer["name"]
    _update_resource_last_hash(
        load_balancers_hash_context, load_balancer_name, load_balancer)


def _is_load_balancer_updated(load_balancers_hash_context, load_balancer):
    load_balancer_name = load_balancer["name"]
    return _is_resource_updated(
        load_balancers_hash_context, load_balancer_name, load_balancer)


def _clear_load_balancer_last_hash(load_balancers_hash_context, load_balancer_name):
    _clear_resource_last_hash(load_balancers_hash_context, load_balancer_name)


def _get_load_balancers_context(context):
    return _get_resources_context(
        context, LOAD_BALANCERS_CONTEXT)


def _list_load_balancers(network_client, resource_group_name):
    load_balancers = _list_network_load_balancers(
        network_client, resource_group_name)
    # list both network load balancers and application load balancers (gateways)
    application_load_balancers = _list_application_load_balancers(
        network_client, resource_group_name)
    load_balancers.update(application_load_balancers)
    return load_balancers


def _get_load_balancer(
        network_client, resource_group_name, load_balancer_name,
        load_balancer_type):
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        return _get_network_load_balancer(
            network_client, resource_group_name, load_balancer_name)
    else:
        return _get_application_load_balancer(
            network_client, resource_group_name, load_balancer_name)


def _create_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_type = load_balancer_config["type"]
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        return _create_network_load_balancer(
            network_client, provider_config, workspace_name,
            load_balancer_config, context)
    else:
        return _create_application_load_balancer(
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
    else:
        return _update_application_load_balancer(
            network_client, provider_config, workspace_name,
            load_balancer_config, context)


def _delete_load_balancer(
        network_client, resource_group_name,
        load_balancer: Dict[str, Any], context):
    load_balancer_type = load_balancer["type"]
    load_balancer_name = load_balancer["name"]
    if load_balancer_type == LOAD_BALANCER_TYPE_NETWORK:
        _delete_network_load_balancer(
            network_client, resource_group_name, load_balancer_name,
            context)
    else:
        _delete_application_load_balancer(
            network_client, resource_group_name, load_balancer_name)


# Common shared functions


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


def _get_frontend_ip_configurations(
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
    return get_virtual_network_resource_id(
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


def _get_load_balancer_schema(application_gateway):
    frontend_ip_configurations = application_gateway.frontend_ip_configurations
    if not frontend_ip_configurations:
        # should not happen
        return LOAD_BALANCER_SCHEMA_INTERNAL

    for frontend_ip_configuration in frontend_ip_configurations:
        if not frontend_ip_configuration.public_ip_address:
            return LOAD_BALANCER_SCHEMA_INTERNET_FACING
        else:
            return LOAD_BALANCER_SCHEMA_INTERNAL


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
    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)

    load_balancer = _get_load_balancer_object(
        provider_config, workspace_name, load_balancer_config)

    resource_group_name = provider_config["resource_group"]
    _create_or_update_load_balancer(
        network_client, resource_group_name,
        load_balancer_name, load_balancer)

    # update the load balancer hash
    load_balancer_properties = load_balancer["properties"]
    backend_address_pools = load_balancer_properties["backendAddressPools"]
    load_balancer_properties.pop("backendAddressPools", None)
    _update_load_balancer_last_hash(
        load_balancers_hash_context, load_balancer)

    # update the backend pools hash
    _clear_and_update_backend_pools_hash(
        backend_address_pools, load_balancers_context, load_balancer_name)


def _update_network_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    resource_group_name = provider_config["resource_group"]
    load_balancer_name = load_balancer_config["name"]
    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)

    load_balancer = _get_load_balancer_object(
        provider_config, workspace_name, load_balancer_config)

    load_balancer_properties = load_balancer["properties"]
    backend_address_pools = load_balancer_properties["backendAddressPools"]
    # check without backend address pools
    load_balancer_properties.pop("backendAddressPools", None)

    if _is_load_balancer_updated(
            load_balancers_hash_context, load_balancer):
        # put back the backend address pool when updating
        load_balancer_properties["backendAddressPools"] = backend_address_pools
        _create_or_update_load_balancer(
            network_client, resource_group_name,
            load_balancer_name, load_balancer)

        load_balancer_properties.pop("backendAddressPools", None)
        _update_load_balancer_last_hash(
            load_balancers_hash_context, load_balancer)

        _clear_and_update_backend_pools_hash(
            backend_address_pools, load_balancers_context, load_balancer_name)
    else:
        # only backend pool addresses changed
        _update_load_balancer_backend_address_pools(
            network_client, resource_group_name,
            backend_address_pools, load_balancers_context, load_balancer_name)


def _delete_network_load_balancer(
        network_client, resource_group_name, load_balancer_name,
        context):
    load_balancer = _get_load_balancer_by_name(
        network_client, resource_group_name, load_balancer_name)
    if not load_balancer:
        return
    network_client.load_balancers.begin_delete(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
    ).result()

    load_balancers_hash_context = _get_load_balancers_hash_context(
        context)
    load_balancers_context = _get_load_balancers_context(context)
    _clear_load_balancer_last_hash(
        load_balancers_hash_context, load_balancer_name)
    _clear_load_balancer_context(
        load_balancers_context, load_balancer_name)


# Azure load balancer Helper functions

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
    # decide the schema
    load_balancer_schema = _get_load_balancer_schema(load_balancer)
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

    frontend_ip_configurations = _get_frontend_ip_configurations(
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


def _create_or_update_load_balancer(
        network_client, resource_group_name,
        load_balancer_name, load_balancer):
    response = network_client.load_balancers.begin_create_or_update(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
        parameters=load_balancer).result()
    return response


def _get_load_balancer_backend_address_pools(
        provider_config, load_balancer_config, virtual_network_name):
    backend_address_pools = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        backend_address_pool = _get_load_balancer_backend_address_pool(
            provider_config, virtual_network_name, service_name, service)
        backend_address_pools.append(backend_address_pool)
    return backend_address_pools


def _get_load_balancer_backend_address_pool(
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
                "ipAddress": ip_address,
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
            listeners = _get_service_group_listeners(service_group)
            for listener in listeners:
                load_balancing_rule = _get_service_group_load_balancing_rule(
                    provider_config, load_balancer_name,
                    frontend_ip_configuration, service_group, listener)
                load_balancing_rules.append(load_balancing_rule)
    return load_balancing_rules


def _get_load_balancer_resource_id(provider_config, load_balancer_name):
    subscription_id = provider_config["subscription_id"]
    resource_group_name = provider_config["resource_group"]
    return get_network_resource_id(
        subscription_id, resource_group_name, "loadBalancers", load_balancer_name)


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
    backend_address_pool_id = (
            load_balancer_resource_id +
            "/backendAddressPools/{}".format(service_name))
    probe_id = load_balancer_resource_id + "probes/{}".format(
        service_name)

    frontend_ip_name = frontend_ip_configuration["name"]
    front_ip_configuration_id = (
            load_balancer_resource_id +
            "frontendIPConfigurations/{}".format(frontend_ip_name))

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


def _get_service_group_services(service_group):
    return service_group.get("services", [])


def _get_service_group_listeners(service_group):
    return service_group.get("listeners", [])


def _get_load_balancer_services(load_balancer_config):
    service_groups = load_balancer_config.get("service_groups", [])
    load_balancer_services = {}
    for service_group in service_groups:
        services = _get_service_group_services(service_group)
        load_balancer_services.update(
            {service["name"]: service for service in services})
    return load_balancer_services


def _update_backend_address_pool_last_hash(
        backend_pools_hash_context, backend_address_pool):
    backend_address_pool_name = backend_address_pool["name"]
    _update_resource_last_hash(
        backend_pools_hash_context, backend_address_pool_name,
        backend_address_pool)


def _is_backend_address_pool_updated(
        backend_pools_hash_context, backend_address_pool):
    backend_address_pool_name = backend_address_pool["name"]
    return _is_resource_updated(
        backend_pools_hash_context, backend_address_pool_name,
        backend_address_pool)


def _clear_and_update_backend_pools_hash(
        backend_address_pools, load_balancers_context, load_balancer_name):
    _clear_load_balancer_context(
        load_balancers_context, load_balancer_name)
    backend_pools_hash_context = _get_load_balancer_context(
        load_balancers_context, load_balancer_name)
    for backend_address_pool in backend_address_pools:
        _update_backend_address_pool_last_hash(
            backend_pools_hash_context, backend_address_pool)


def _update_load_balancer_backend_address_pools(
        network_client, resource_group_name,
        backend_address_pools, load_balancers_context, load_balancer_name):
    backend_pools_hash_context = _get_load_balancer_context(
        load_balancers_context, load_balancer_name)
    for backend_address_pool in backend_address_pools:
        if _is_backend_address_pool_updated(
                backend_pools_hash_context, backend_address_pool):
            _create_or_update_backend_address_pool(
                network_client, resource_group_name,
                load_balancer_name, backend_address_pool)
            _update_backend_address_pool_last_hash(
                backend_pools_hash_context, backend_address_pool)


def _create_or_update_backend_address_pool(
        network_client, resource_group_name,
        load_balancer_name, backend_address_pool):
    backend_address_pool_name = backend_address_pool["name"]
    response = network_client.load_balancer_backend_address_pools.begin_create_or_update(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
        backend_address_pool_name=backend_address_pool_name,
        parameters=backend_address_pool).result()
    return response


#####################################
# Azure Application Gateway functions
#####################################


def _list_application_load_balancers(network_client, resource_group_name):
    load_balancers = _list_workspace_application_gateways(
        network_client, resource_group_name)
    load_balancer_map = {}
    for load_balancer in load_balancers:
        load_balancer_name = _get_load_balancer_name(load_balancer)
        load_balancer_info = _get_load_balancer_info_of(load_balancer)
        load_balancer_map[load_balancer_name] = load_balancer_info
    return load_balancer_map


def _get_application_load_balancer(
        network_client, resource_group_name, load_balancer_name):
    application_gateway = _get_application_gateway_by_name(
        network_client, resource_group_name, load_balancer_name)
    if not application_gateway:
        return None
    return _get_application_gateway_info_of(application_gateway)


def _create_application_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_name = load_balancer_config["name"]

    application_gateway = _get_application_gateway_object(
        provider_config, workspace_name, load_balancer_config)

    resource_group_name = provider_config["resource_group"]
    _create_or_update_application_gateway(
        network_client, resource_group_name,
        load_balancer_name, application_gateway)


def _update_application_load_balancer(
        network_client, provider_config, workspace_name,
        load_balancer_config, context):
    load_balancer_name = load_balancer_config["name"]

    application_gateway = _get_application_gateway_object(
        provider_config, workspace_name, load_balancer_config)

    resource_group_name = provider_config["resource_group"]
    _create_or_update_application_gateway(
        network_client, resource_group_name,
        load_balancer_name, application_gateway)


def _delete_application_load_balancer(
        network_client, resource_group_name, load_balancer_name):
    load_balancer = _get_load_balancer_by_name(
        network_client, resource_group_name, load_balancer_name)
    if not load_balancer:
        return
    network_client.load_balancers.begin_delete(
        resource_group_name=resource_group_name,
        load_balancer_name=load_balancer_name,
    ).result()


# Azure Application Gateway Helper functions

def _list_workspace_application_gateways(network_client, resource_group_name):
    application_gateways = network_client.application_gateways.list(
        resource_group_name=resource_group_name,
    )
    if application_gateways is None:
        return []
    return list(application_gateways)


def _get_application_gateway_by_name(
        network_client, resource_group_name, application_gateway_name):
    try:
        response = network_client.application_gateways.get(
            resource_group_name=resource_group_name,
            application_gateway_name=application_gateway_name,
        )
        return response
    except Exception:
        return None


def _get_application_gateway_info_of(application_gateway):
    load_balancer_id = _get_load_balancer_id(application_gateway)
    load_balancer_name = _get_load_balancer_name(application_gateway)
    load_balancer_type = LOAD_BALANCER_TYPE_APPLICATION
    # decide the schema
    load_balancer_schema = _get_load_balancer_schema(application_gateway)
    load_balancer_info = {
        "id": load_balancer_id,
        "name": load_balancer_name,
        "type": load_balancer_type,
        "schema": load_balancer_schema,
    }
    tags = application_gateway.tags
    if tags:
        load_balancer_info["tags"] = tags
    return load_balancer_info


def _get_application_gateway_object(
        provider_config, workspace_name, load_balancer_config):
    location = provider_config["location"]
    tags = load_balancer_config.get("tags", None)

    virtual_network_name = get_virtual_network_name(
        provider_config, workspace_name)

    gateway_ip_configurations = _get_application_gateway_ip_configurations(
        provider_config, workspace_name, virtual_network_name)
    frontend_ip_configurations = _get_frontend_ip_configurations(
        provider_config, workspace_name, load_balancer_config,
        virtual_network_name)
    frontend_ports = _get_application_gateway_frontend_ports(
        load_balancer_config)
    backend_address_pools = _get_application_gateway_backend_address_pools(
        load_balancer_config)
    backend_http_settings = _get_application_gateway_backend_http_settings(
        load_balancer_config)
    http_listeners = _get_application_gateway_http_listeners(
        provider_config, load_balancer_config, frontend_ip_configurations)
    rewrite_rule_sets = _get_application_gateway_rewrite_rule_sets(
        load_balancer_config)
    url_path_maps = _get_application_gateway_url_path_maps(
        provider_config, load_balancer_config)
    request_routing_rules = _get_application_gateway_request_routing_rules(
        provider_config, load_balancer_config, frontend_ip_configurations)

    application_gateway_object = {
        "location": location,
        "tags": tags,
        "properties": {
            "sku": {
                "name": "Standard_v2",
                "tier": "Standard_v2",
            },
            "gatewayIPConfigurations": gateway_ip_configurations,
            "frontendIPConfigurations": frontend_ip_configurations,
            "frontendPorts": frontend_ports,
            "backendAddressPools": backend_address_pools,
            "backendHttpSettingsCollection": backend_http_settings,
            "httpListeners": http_listeners,
            "rewriteRuleSets": rewrite_rule_sets,
            "urlPathMaps": url_path_maps,
            "requestRoutingRules": request_routing_rules,
            "globalConfiguration": {
              "enableRequestBuffering": True,
              "enableResponseBuffering": True
            }
        }
    }
    return application_gateway_object


def _create_or_update_application_gateway(
        network_client, resource_group_name,
        application_gateway_name, application_gateway):
    response = network_client.application_gateways.begin_create_or_update(
        resource_group_name=resource_group_name,
        application_gateway_name=application_gateway_name,
        parameters=application_gateway).result()
    return response


def _get_application_gateway_ip_configurations(
        provider_config, workspace_name, virtual_network_name):
    gateway_ip_configurations = []
    # Currently there is only one gateway ip configuration
    gateway_ip_configuration = _get_application_gateway_ip_configuration(
        provider_config, workspace_name, virtual_network_name)
    gateway_ip_configurations.append(gateway_ip_configuration)
    return gateway_ip_configurations


def _get_application_gateway_ip_configuration(
        provider_config, workspace_name, virtual_network_name):
    # use the private subnet
    subnet_name = get_workspace_subnet_name(
        workspace_name, is_private=True)
    virtual_network_id = _get_virtual_network_resource_id(
        provider_config, virtual_network_name)
    subnet_id = virtual_network_id + "/subnets/{}".format(subnet_name)
    gateway_ip_name = subnet_name
    gateway_ip_configuration = {
        "name": gateway_ip_name,
        "properties": {
            "subnet": {
                "id": subnet_id
            }
        }
    }
    return gateway_ip_configuration


def _get_application_gateway_frontend_ports(
        load_balancer_config):
    frontend_ports = []
    service_groups = load_balancer_config.get("service_groups", [])
    for service_group in service_groups:
        # The listeners of the service group cannot overlap
        listeners = _get_service_group_listeners(service_group)
        for listener in listeners:
            frontend_port = _get_application_gateway_frontend_port(listener)
            frontend_ports.append(frontend_port)
    return frontend_ports


def _get_application_gateway_frontend_port_name(
        listener):
    # the frontend port name a combination of protocol and port
    # although the frontend port doesn't include the protocol
    protocol = listener["protocol"]
    port = listener["port"]
    return "{}-{}".format(protocol, port)


def _get_application_gateway_frontend_port(
        listener):
    # Name of the frontend port that is unique within an Application Gateway.
    name = _get_application_gateway_frontend_port_name(listener)
    frontend_port = {
        "name": name,
        "properties": {
            "port": listener["port"]
        }
    }
    return frontend_port


def _get_application_gateway_backend_address_pools(
        load_balancer_config):
    backend_address_pools = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        backend_address_pool = _get_application_gateway_backend_address_pool(
            service_name, service)
        backend_address_pools.append(backend_address_pool)
    return backend_address_pools


def _get_application_gateway_backend_address_pool(
        service_name, service):
    # Name of the backend address pool that is unique within an Application Gateway.
    backend_addresses = _get_application_gateway_backend_addresses(
        service)
    backend_address_pool = {
        "name": service_name,
        "properties": {
            "backendAddresses": backend_addresses,
        }
    }
    return backend_address_pool


def _get_application_gateway_backend_addresses(
        service):
    backend_addresses = []
    targets = service.get("targets", [])
    for target in targets:
        ip_address = target[0]
        backend_address = {
            "ipAddress": ip_address,
        }
        backend_addresses.append(backend_address)
    return backend_addresses


def _get_application_gateway_backend_http_settings(
        load_balancer_config):
    backend_http_settings = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        backend_http_setting = _get_application_gateway_backend_http_setting(
            service_name, service)
        backend_http_settings.append(backend_http_setting)
    return backend_http_settings


def _get_application_gateway_backend_http_setting(
        service_name, service):
    protocol = _get_load_balancer_protocol(service["protocol"])
    backend_http_settings = {
        "name": service_name,
        "properties": {
            "protocol": protocol,
            "port": service["port"],
            "cookieBasedAffinity": "Disabled",
            "requestTimeout": 30
        }
    }
    return backend_http_settings


def _get_application_gateway_http_listeners(
        provider_config, load_balancer_config,
        frontend_ip_configurations):
    application_gateway_name = load_balancer_config["name"]
    http_listeners = []

    # The listeners repeated based on the number of front ip configurations
    for frontend_ip_configuration in frontend_ip_configurations:
        service_groups = load_balancer_config.get("service_groups", [])
        for service_group in service_groups:
            # The listeners of the service group cannot overlap
            listeners = _get_service_group_listeners(service_group)
            for listener in listeners:
                http_listener = _get_application_gateway_http_listener(
                    provider_config, application_gateway_name,
                    frontend_ip_configuration, listener)
                http_listeners.append(http_listener)
    return http_listeners


def _get_application_gateway_resource_id(
        provider_config, application_gateway_name):
    subscription_id = provider_config["subscription_id"]
    resource_group_name = provider_config["resource_group"]
    return get_network_resource_id(
        subscription_id, resource_group_name,
        "applicationGateways", application_gateway_name)


def _get_application_gateway_http_listener_name(
        frontend_ip_name, frontend_port_name):
    return "{}-{}".format(frontend_ip_name, frontend_port_name)


def _get_application_gateway_http_listener(
        provider_config, application_gateway_name,
        frontend_ip_configuration, listener):
    # this will remove unrelated attributes for listener
    protocol = _get_load_balancer_protocol(listener["protocol"])
    application_gateway_resource_id = _get_application_gateway_resource_id(
        provider_config, application_gateway_name)

    frontend_ip_name = frontend_ip_configuration["name"]
    front_ip_configuration_id = (
            application_gateway_resource_id +
            "frontendIPConfigurations/{}".format(frontend_ip_name))
    frontend_port_name = _get_application_gateway_frontend_port_name(listener)
    front_port_configuration_id = (
            application_gateway_resource_id +
            "frontendPorts/{}".format(frontend_port_name))

    name = _get_application_gateway_http_listener_name(
        frontend_ip_name, frontend_port_name)

    http_listener = {
        "name": name,
        "properties": {
            "protocol": protocol,
            "frontendIPConfiguration": {
                "id": front_ip_configuration_id
            },
            "frontendPort": {
                "id": front_port_configuration_id
            },
            "requireServerNameIndication": False
        }
    }
    return http_listener


def _get_application_gateway_rewrite_rule_sets(
        load_balancer_config):
    rewrite_rule_sets = []
    services = _get_load_balancer_services(load_balancer_config)
    for service_name, service in services:
        # each service will have two rewrite rules in the rule set
        rewrite_rule_set = _get_service_rewrite_rule_set(
            service_name, service)
        if rewrite_rule_set:
            rewrite_rule_sets.append(rewrite_rule_set)
    return rewrite_rule_sets


def _is_service_rewrite_needed(service):
    strip_path = service.get("route_path")
    # Note that right / is by default handled
    # because strip /abc/ from /abc/xyz is equivalent with stripping /abc from it.
    # you cannot make a http request without / in the front of the path.
    if strip_path:
        # this makes /abc/ to /abc or / to empty string (which means no strip)
        strip_path = strip_path.rstrip('/')
    service_path = service.get("service_path")
    if strip_path or service_path:
        return True
    return False


def _get_service_rewrite_rule_set(
        service_name, service):
    strip_path = service.get("route_path")
    # Note that right / is by default handled
    # because strip /abc/ from /abc/xyz is equivalent with stripping /abc from it.
    # you cannot make a http request without / in the front of the path.
    if strip_path:
        # this makes /abc/ to /abc or / to empty string (which means no strip)
        strip_path = strip_path.rstrip('/')
    service_path = service.get("service_path")
    if not strip_path and not service_path:
        # no need rewrite rule set
        return None

    # for detailed rewrite instructions:
    # https://learn.microsoft.com/en-us/azure/application-gateway/rewrite-http-headers-url
    rewrite_rule_exact = _get_service_rewrite_rule_exact(
        service_name, strip_path, service_path)
    rewrite_rule_prefix = _get_service_rewrite_rule_prefix(
        service_name, strip_path, service_path)
    rewrite_rules = [rewrite_rule_exact, rewrite_rule_prefix]
    rewrite_rule_set = {
        "name": service_name,
        "properties": {
            "rewriteRules": rewrite_rules
        }
    }
    return rewrite_rule_set


def _get_service_rewrite_rule_exact(
        service_name, strip_path, service_path):
    if strip_path:
        pattern = "^" + strip_path + "$"
    else:
        pattern = "^/$"
    if service_path:
        modified_path = service_path
    else:
        modified_path = "/"

    name = "{}-exact".format(service_name)
    rewrite_rule_exact = {
        "name": name,
        "conditions": [
            {
                "variable": "uri_path",
                "pattern": pattern,
            }
        ],
        "actionSet": {
            "urlConfiguration": {
                "modifiedPath": modified_path
            }
        }
    }
    return rewrite_rule_exact


def _get_service_rewrite_rule_prefix(
        service_name, strip_path, service_path):
    # To capture a substring for later use, put parentheses around
    # the subpattern that matches it in the condition regex definition.
    if strip_path:
        pattern = "^" + strip_path + "/(.*)"
    else:
        pattern = "^/(.*)"

    # reference them in the action set using the following format:
    # For a server variable, you must use {var_serverVariableName_groupNumber}.
    # For example, {var_uri_path_1} or {var_uri_path_2}
    if service_path:
        modified_path = service_path + "/{var_uri_path_1}"
    else:
        modified_path = "/{var_uri_path_1}"
    name = "{}-prefix".format(service_name)
    rewrite_rule_prefix = {
        "name": name,
        "conditions": [
            {
                "variable": "uri_path",
                "pattern": pattern,
            }
        ],
        "actionSet": {
            "urlConfiguration": {
                "modifiedPath": modified_path
            }
        }
    }
    return rewrite_rule_prefix


def _get_application_gateway_url_path_maps(
        provider_config, load_balancer_config):
    application_gateway_name = load_balancer_config["name"]
    # create one urlPathMap for each service group
    url_path_maps = []
    service_groups = load_balancer_config.get("service_groups", [])
    for service_group in service_groups:
        url_path_map = _get_application_gateway_url_path_map(
            provider_config, application_gateway_name, service_group)
        url_path_maps.append(url_path_map)
    return url_path_maps


def _get_application_default_service(services):
    if not services:
        return None
    for service in services:
        if service.get("default", False):
            return service
    return None


def _get_application_gateway_url_path_map_name(service_group):
    # TODO: handle properly for service group name
    # currently we have only one listener for each service group
    # and we can use the listener to name the service group
    listeners = _get_service_group_listeners(service_group)
    listener = listeners[0]
    protocol = listener["protocol"]
    port = listener["port"]
    return "{}-{}".format(protocol, port)


def _get_application_gateway_url_path_map(
        provider_config, application_gateway_name, service_group):
    path_rules = _get_application_gateway_path_rules(
        provider_config, application_gateway_name, service_group)
    name = _get_application_gateway_url_path_map_name(service_group)
    url_path_map = {
        "name": name,
        "properties": {
            "pathRules": path_rules
        }
    }

    # TODO: Is there must be a default service?
    services = _get_service_group_services(service_group)
    default_service = _get_application_default_service(
        services)
    if default_service:
        service_name = default_service["name"]
        application_gateway_resource_id = _get_application_gateway_resource_id(
            provider_config, application_gateway_name)
        backend_address_pool_id = (
                application_gateway_resource_id +
                "/backendAddressPools/{}".format(service_name))
        backend_https_settings_id = (
                application_gateway_resource_id +
                "/backendHttpSettingsCollection/{}".format(service_name))

        properties = url_path_map["properties"]
        properties["defaultBackendAddressPool"] = {
            "id": backend_address_pool_id
        }
        properties["defaultBackendHttpSettings"] = {
            "id": backend_https_settings_id
        }

        if _is_service_rewrite_needed(default_service):
            rewrite_rule_set_id = (
                    application_gateway_resource_id +
                    "/rewriteRuleSet/{}".format(service_name))
            properties["defaultRewriteRuleSet"] = {
                "id": rewrite_rule_set_id
            }
    return url_path_map


def _get_sorted_services(services):
    def sort_by_route_and_name(service):
        service_name = service["name"]
        route_path = service["route_path"]
        return [route_path, service_name]

    return sorted(services, key=sort_by_route_and_name, reverse=True)


def _get_application_gateway_path_rules(
        provider_config, application_gateway_name, service_group):
    # Path rules are processed in order, based on how they're listed.
    # The least specific path (with wildcards) should be at the end of the list,
    # so that it will be processed last.
    # We should sort the services by the route path in reverse order
    path_rules = []
    services = _get_service_group_services(service_group)
    sorted_services = _get_sorted_services(services)
    for service in sorted_services:
        path_rule = _get_application_gateway_path_rule(
            provider_config, application_gateway_name, service)
        path_rules.append(path_rule)
    return path_rules


def _get_application_gateway_path_rule(
        provider_config, application_gateway_name, service):
    service_name = service["name"]
    route_path = service["route_path"]

    application_gateway_resource_id = _get_application_gateway_resource_id(
        provider_config, application_gateway_name)
    backend_address_pool_id = (
            application_gateway_resource_id +
            "/backendAddressPools/{}".format(service_name))
    backend_https_settings_id = (
            application_gateway_resource_id +
            "/backendHttpSettingsCollection/{}".format(service_name))
    # Note: PathPattern is a list of path patterns to match.
    # Each path must start with / and may use * as a wildcard character.
    # The string fed to the path matcher doesn't include any text after the first ? or #,
    # and those chars aren't allowed here.

    if route_path.endswith('/'):
        # if the route path ends with /, we don't need two paths to match
        # /abc/ will use /abc/* to match (it will not match /abc)
        # / will use /* to match
        paths = [route_path + '*']
    else:
        paths = [route_path, route_path + '/*']

    path_rule = {
        "name": service_name,
        "properties": {
            "paths": paths,
            "backendAddressPool": {
                "id": backend_address_pool_id
            },
            "backendHttpSettings": {
                "id": backend_https_settings_id
            }
        }
    }

    if _is_service_rewrite_needed(service):
        properties = path_rule["properties"]
        rewrite_rule_set_id = (
                application_gateway_resource_id +
                "/rewriteRuleSets/{}".format(service_name))
        properties["defaultRewriteRuleSet"] = {
            "id": rewrite_rule_set_id
        }

    return path_rule


def _get_application_gateway_request_routing_rules(
        provider_config, load_balancer_config, frontend_ip_configurations):
    application_gateway_name = load_balancer_config["name"]
    request_routing_rules = []

    # The routing rules repeated based on the number of front ip configurations
    for frontend_ip_configuration in frontend_ip_configurations:
        service_groups = load_balancer_config.get("service_groups", [])
        for service_group in service_groups:
            # The listeners of the service group cannot overlap
            listeners = _get_service_group_listeners(service_group)
            for listener in listeners:
                request_routing_rule = _get_application_gateway_request_routing_rule(
                    provider_config, application_gateway_name,
                    frontend_ip_configuration, service_group, listener)
                request_routing_rules.append(request_routing_rule)
    return request_routing_rules


def _get_application_gateway_request_routing_rule_name(
        http_listener_name, url_path_map_name):
    # TODO: handle properly for service group name
    # Considering that the listener name includes the protocol and port
    # The url path map assume one service group listener and it uses the
    # protocol and port of the only listener
    return http_listener_name


def _get_application_gateway_request_routing_rule(
        provider_config, application_gateway_name,
        frontend_ip_configuration, service_group, listener):
    application_gateway_resource_id = _get_application_gateway_resource_id(
        provider_config, application_gateway_name)

    frontend_ip_name = frontend_ip_configuration["name"]
    frontend_port_name = _get_application_gateway_frontend_port_name(listener)
    http_listener_name = _get_application_gateway_http_listener_name(
        frontend_ip_name, frontend_port_name)
    http_listener_id = (
            application_gateway_resource_id +
            "httpListeners/{}".format(http_listener_name))

    url_path_map_name = _get_application_gateway_url_path_map_name(service_group)
    url_path_map_id = (
            application_gateway_resource_id +
            "urlPathMaps/{}".format(url_path_map_name))

    name = _get_application_gateway_request_routing_rule_name(
        http_listener_name, url_path_map_name)
    request_routing_rule = {
        "name": name,
        "properties": {
            "ruleType": "PathBasedRouting",
            "priority": 10,
            "httpListener": {
                "id": http_listener_id
            },
            "urlPathMap": {
                "id": url_path_map_id
            },
        }
    }
    return request_routing_rule

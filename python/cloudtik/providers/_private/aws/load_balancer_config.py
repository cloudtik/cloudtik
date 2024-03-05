from typing import Dict, Any

import botocore

from cloudtik.core._private.util.core_utils import batch_list, get_json_object_hash, get_config_for_update
from cloudtik.core._private.util.load_balancer import get_service_group_services, get_load_balancer_service_groups, \
    get_service_group_listeners, get_load_balancer_public_ips, get_load_balancer_config_name, \
    get_load_balancer_config_type, get_load_balancer_config_scheme, get_service_targets
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.load_balancer_provider import LOAD_BALANCER_TYPE_APPLICATION, LOAD_BALANCER_TYPE_NETWORK, \
    LOAD_BALANCER_SCHEME_INTERNET_FACING
from cloudtik.core.tags import CLOUDTIK_TAG_WORKSPACE_NAME
from cloudtik.providers._private.aws.config import _is_workspace_tagged, _get_response_object, \
    _get_ordered_workspace_subnets, _get_workspace_security_group
from cloudtik.providers._private.aws.utils import tags_list_to_dict, _make_resource, get_boto_error_code

CLOUDTIK_TAG_LOAD_BALANCER_NAME = "cloudtik-load-balancer"
CLOUDTIK_TAG_SERVICE_NAME = "cloudtik-service"

TARGET_GROUPS_HASH_CONTEXT = "target_groups_hash"
LISTENERS_HASH_CONTEXT = "listeners_hash"
LISTENERS_CONTEXT = "listeners"
LISTENER_RULES_HASH_CONTEXT = "rules_hash"


"""

Key Concepts to note for AWS load balancers:

AWS load balancer has a granular API for create or update the configurations.

The Network load balancer and Application load share the same API and only
differs at some points for configurations.

The target group is by VPC.

Notes for Public IP:
You can't assign a static IP address to an Application Load Balancer.
If Application Load Balancer requires a static IP address, then it's a best practice
to register it behind a Network Load Balancer.

"""


def _get_response_list(response, name):
    if not response:
        return None
    return response.get(name)


def _get_tag_value(tags, tag_name):
    # get the tag for the service name
    if not tags:
        return None
    for tag in tags:
        if tag["Key"] == tag_name:
            return tag.get("Value")
    return None


def _is_tagged_by(tags, tag_name, tag_value):
    return True if _get_tag_value(tags, tag_name) == tag_value else False


def _get_tagged_service_name(resource_object):
    tags = resource_object.get("Tags", [])
    return _get_tag_value(tags, CLOUDTIK_TAG_SERVICE_NAME)


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


def _bootstrap_load_balancer_config(
        config: Dict[str, Any], provider_config: Dict[str, Any]):
    cluster_provider_config = get_provider_config(config)
    # copy the related information from cluster provider config to provider config
    provider_config["type"] = cluster_provider_config["type"]
    provider_config["region"] = cluster_provider_config["region"]
    return provider_config


def _get_resource_tags(elb_client, resource_objects, resource_id_name):
    for resource_object_batch in batch_list(resource_objects, 20):
        # can specify up to 20 resources in a single call
        _get_resource_tags_batch(
            elb_client, resource_object_batch, resource_id_name)


def _get_resource_tags_batch(elb_client, resource_objects, resource_id_name):
    resource_ids = [resource_object[resource_id_name] for resource_object in resource_objects]
    response = elb_client.describe_tags(ResourceArns=resource_ids)
    resource_object_tags = {
        tag_desc["ResourceArn"]: tag_desc.get(
            "Tags") for tag_desc in response.get("TagDescriptions", [])}

    for resource_object in resource_objects:
        resource_object_id = resource_object[resource_id_name]
        tags = resource_object_tags.get(resource_object_id)
        if tags:
            resource_object["Tags"] = tags


def _get_load_balancer_id(load_balancer):
    return load_balancer["LoadBalancerArn"]


def _get_load_balancer_name(load_balancer):
    return load_balancer["LoadBalancerName"]


def _list_workspace_load_balancers(elb_client, workspace_name):
    load_balancers = elb_client.describe_load_balancers().get("LoadBalancers", [])
    _get_resource_tags(elb_client, load_balancers, "LoadBalancerArn")
    return [
        load_balancer for load_balancer in load_balancers
        if _is_workspace_tagged(load_balancer.get("Tags"), workspace_name)]


def _list_load_balancers(elb_client, workspace_name):
    load_balancers = _list_workspace_load_balancers(
        elb_client, workspace_name)
    load_balancer_map = {}
    for load_balancer in load_balancers:
        load_balancer_name = _get_load_balancer_name(load_balancer)
        load_balancer_info = _get_load_balancer_info_of(load_balancer)
        load_balancer_map[load_balancer_name] = load_balancer_info
    return load_balancer_map


def _get_load_balancer_by_name(elb_client, load_balancer_name):
    response = elb_client.describe_load_balancers(
        Names=[load_balancer_name])
    return _get_response_object(response, "LoadBalancers")


def _get_load_balancer_info_of(load_balancer):
    load_balancer_id = _get_load_balancer_id(load_balancer)
    load_balancer_name = _get_load_balancer_name(load_balancer)
    load_balancer_type = load_balancer["Type"]
    load_balancer_scheme = load_balancer["Scheme"]
    load_balancer_info = {
        "id": load_balancer_id,
        "name": load_balancer_name,
        "type": load_balancer_type,
        "scheme": load_balancer_scheme,
    }
    tag_list = load_balancer.get("Tags")
    if tag_list:
        tags = tags_list_to_dict(tag_list)
        load_balancer_info["tags"] = tags
    return load_balancer_info


def _get_load_balancer_listener_info(load_balancer_listeners):
    return [
        {
            "protocol": load_balancer_listener["Protocol"],
            "port": load_balancer_listener["Port"]
        } for load_balancer_listener in load_balancer_listeners
    ]


def _get_load_balancer(elb_client, load_balancer_name):
    load_balancer = _get_load_balancer_by_name(elb_client, load_balancer_name)
    if not load_balancer:
        return None
    _get_resource_tags(elb_client, [load_balancer], "LoadBalancerArn")
    return _get_load_balancer_info_of(load_balancer)


def _get_load_balancer_subnet_ids(
        provider_config, workspace_name, vpc_id):
    ec2 = _make_resource("ec2", provider_config)
    public_subnets, private_subnets = _get_ordered_workspace_subnets(
        workspace_name, ec2, vpc_id)
    subnet_ids = [private_subnet.id for private_subnet in private_subnets]
    return subnet_ids


def get_load_balancer_security_group_id(provider_config, vpc_id, workspace_name):
    security_group = _get_workspace_security_group(
        provider_config, vpc_id, workspace_name)
    return security_group.id


def _get_load_balancer_context(context, load_balancer_name):
    return _get_resources_context(context, load_balancer_name)


def _clear_load_balancer_context(context, load_balancer_name):
    _clear_resource_hash(context, load_balancer_name)


def _create_load_balancer(
        elb_client, provider_config,
        workspace_name, load_balancer_config,
        vpc_id, context):
    subnet_ids = _get_load_balancer_subnet_ids(
        provider_config, workspace_name, vpc_id)
    security_group_id = get_load_balancer_security_group_id(
        provider_config, vpc_id, workspace_name)

    tags = load_balancer_config.get("tags", {})
    tag_pairs = [
        {'Key': CLOUDTIK_TAG_WORKSPACE_NAME, 'Value': workspace_name}
    ]
    for k, v in tags.items():
        tag_pairs.append({
            "Key": k,
            "Value": v,
        })

    load_balancer_name = get_load_balancer_config_name(load_balancer_config)
    load_balancer_type = get_load_balancer_config_type(load_balancer_config)
    load_balancer_scheme = get_load_balancer_config_scheme(load_balancer_config)

    static_public_ips = get_load_balancer_public_ips(load_balancer_config)
    if (load_balancer_type == LOAD_BALANCER_TYPE_NETWORK
            and load_balancer_scheme == LOAD_BALANCER_SCHEME_INTERNET_FACING
            and static_public_ips):
        # handle elastic ip (application load balancer cannot assign static IP)
        valid_ip_count = min(len(static_public_ips), len(subnet_ids))
        subnet_mappings = [
            {
                'SubnetId': subnet_ids[i],
                'AllocationId': static_public_ips[i]["id"],
            }
            for i in range(0, valid_ip_count)
        ]

        response = elb_client.create_load_balancer(
            Name=load_balancer_name,
            Type=load_balancer_type,
            Tags=tag_pairs,
            Scheme=load_balancer_scheme,
            SubnetMappings=subnet_mappings,
            SecurityGroups=[security_group_id]
        )
    else:
        response = elb_client.create_load_balancer(
            Name=load_balancer_name,
            Type=load_balancer_type,
            Tags=tag_pairs,
            Scheme=load_balancer_scheme,
            Subnets=subnet_ids,
            SecurityGroups=[security_group_id]
        )
    load_balancer = _get_response_object(response, "LoadBalancers")
    if not load_balancer:
        raise RuntimeError(
            "Failed to create load balancer: {}.".format(load_balancer_name))
    load_balancer_id = _get_load_balancer_id(load_balancer)
    wait_load_balancer_exists(elb_client, load_balancer_id)

    load_balancer_context = _get_load_balancer_context(
        context, load_balancer_name)

    # create or update all the target groups of the load balancer
    _create_or_update_load_balancer_target_groups(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context, vpc_id)

    # create listeners
    _create_load_balancer_listeners(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context)


def wait_load_balancer_exists(elb_client, load_balancer_id):
    waiter = elb_client.get_waiter('load_balancer_exists')
    waiter.wait(
        LoadBalancerArns=[load_balancer_id],
        WaiterConfig={
            'Delay': 1,
            'MaxAttempts': 120
        }
    )


def _update_load_balancer(
        elb_client, provider_config,
        workspace_name, load_balancer_config,
        vpc_id, context):
    # The load balancer exists
    # we track the last settings we updated in context
    load_balancer_name = get_load_balancer_config_name(load_balancer_config)
    load_balancer = _get_load_balancer_by_name(elb_client, load_balancer_name)
    if not load_balancer:
        raise RuntimeError(
            "Load balancer with name {} doesn't exist.".format(
                load_balancer_name))

    load_balancer_context = _get_load_balancer_context(
        context, load_balancer_name)

    # create or update the target groups
    _create_or_update_load_balancer_target_groups(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context, vpc_id)

    _update_load_balancer_listeners(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context)

    # delete the target groups that is not needed
    _delete_load_balancer_target_groups_unused(
        elb_client, load_balancer, load_balancer_context)


def _delete_load_balancer(
        elb_client, load_balancer: Dict[str, Any], context):
    load_balancer_name = load_balancer["name"]
    load_balancer = _get_load_balancer_by_name(elb_client, load_balancer_name)
    if not load_balancer:
        return
    load_balancer_id = _get_load_balancer_id(load_balancer)
    _clear_load_balancer_context(context, load_balancer_name)

    # when the load balancer is deleted, all listener and its rules are deleted
    _delete_load_balancer_by_id(elb_client, load_balancer_id)

    # delete load balancer targets
    _delete_load_balancer_target_groups(elb_client, load_balancer_name)


def _delete_load_balancer_by_id(elb_client, load_balancer_id):
    elb_client.delete_load_balancer(
        LoadBalancerArn=load_balancer_id
    )
    wait_load_balancer_deleted(elb_client, load_balancer_id)


def wait_load_balancer_deleted(elb_client, load_balancer_id):
    waiter = elb_client.get_waiter('load_balancers_deleted')
    waiter.wait(
        LoadBalancerArns=[load_balancer_id],
        WaiterConfig={
            'Delay': 3,
            'MaxAttempts': 120
        }
    )


def _get_load_balancer_services(load_balancer_config):
    service_groups = get_load_balancer_service_groups(load_balancer_config)
    load_balancer_services = {}
    for service_group in service_groups:
        services = get_service_group_services(service_group)
        load_balancer_services.update(
            {service["name"]: service for service in services})
    return load_balancer_services


def _create_or_update_load_balancer_target_groups(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context, vpc_id):
    load_balancer_name = _get_load_balancer_name(load_balancer)
    load_balancer_services = _get_load_balancer_services(load_balancer_config)
    existing_target_groups = _get_target_groups(elb_client, load_balancer_name)
    (target_groups_to_create,
     target_groups_to_update) = _get_target_groups_for_action(
        load_balancer_services, existing_target_groups)
    target_groups_hash_context = _get_target_groups_hash_context(load_balancer_context)

    for service in target_groups_to_create:
        _create_target_group_for_service(
            elb_client, load_balancer_name,
            service, vpc_id)
        _update_target_group_hash(target_groups_hash_context, service)

    for target_group_to_update in target_groups_to_update:
        service, target_group = target_group_to_update
        if _is_target_group_updated(target_groups_hash_context, service):
            _update_target_group_for_service(
                elb_client, target_group, service)
            _update_target_group_hash(target_groups_hash_context, service)


def _get_target_groups_for_action(load_balancer_services, existing_target_groups):
    target_groups_create = []
    target_groups_update = []

    # convert to dict for fast search
    existing_target_groups_by_key = {
        _get_target_group_service_name(target_group): target_group
        for target_group in existing_target_groups
    }
    for service_name, service in load_balancer_services.items():
        if service_name not in existing_target_groups_by_key:
            target_groups_create.append(service)
        else:
            target_group = existing_target_groups_by_key[service_name]
            target_groups_update.append((service, target_group))
    return target_groups_create, target_groups_update


def _get_unused_target_groups(target_groups, target_groups_used):
    target_groups_by_id = {
        _get_target_group_id(target_group): target_group
        for target_group in target_groups}
    target_groups_used_by_id = {
        _get_target_group_id(target_group): target_group
        for target_group in target_groups_used}
    return {
        target_group_id: target_group
        for target_group_id, target_group in target_groups_by_id.items()
        if target_group_id not in target_groups_used_by_id
    }


def _delete_load_balancer_target_groups_unused(
        elb_client, load_balancer, load_balancer_context):
    load_balancer_id = _get_load_balancer_id(load_balancer)
    load_balancer_name = _get_load_balancer_name(load_balancer)
    target_groups = _get_target_groups(elb_client, load_balancer_name)
    target_groups_used = _get_used_target_groups(elb_client, load_balancer_id)
    target_groups_unused = _get_unused_target_groups(
        target_groups, target_groups_used)
    target_groups_hash_context = _get_target_groups_hash_context(load_balancer_context)

    for target_group_id, target_group in target_groups_unused.items():
        _delete_target_group(elb_client, target_group_id)
        service_name = _get_target_group_service_name(target_group)
        _clear_target_group_hash(target_groups_hash_context, service_name)


def _delete_load_balancer_target_groups(
        elb_client, load_balancer_name):
    target_groups = _get_target_groups(elb_client, load_balancer_name)
    for target_group in target_groups:
        target_group_id = _get_target_group_id(target_group)
        _delete_target_group(elb_client, target_group_id)


def _get_listeners_hash_context(load_balancer_context):
    return _get_resources_context(load_balancer_context, LISTENERS_HASH_CONTEXT)


def _get_listeners_context(load_balancer_context):
    return _get_resources_context(load_balancer_context, LISTENERS_CONTEXT)


def _get_listener_key(listener):
    return listener["protocol"], listener["port"]


def _get_load_balancer_listener_key(load_balancer_listener):
    return load_balancer_listener["Protocol"], load_balancer_listener["Port"]


def _get_listener_context(listeners_context, listener_key):
    return get_config_for_update(
        listeners_context, listener_key)


def _update_listener_hash(listener_hashes_context, listener):
    listener_key = _get_listener_key(listener)
    _update_resource_hash(
        listener_hashes_context, listener_key, listener)


def _is_listener_updated(listener_hashes_context, listener):
    listener_key = _get_listener_key(listener)
    return _is_resource_updated(
        listener_hashes_context, listener_key, listener)


def _clear_listener_hash(listener_hashes_context, listener_key):
    _clear_resource_hash(listener_hashes_context, listener_key)


def _get_load_balancer_listeners(elb_client, load_balancer_id):
    return elb_client.describe_listeners(
        LoadBalancerArn=load_balancer_id).get("Listeners", [])


def _get_listener_id(load_balancer_listener):
    return load_balancer_listener["ListenerArn"]


def _get_listeners_config(load_balancer_config):
    service_groups = get_load_balancer_service_groups(load_balancer_config)
    listeners_config = []
    for service_group in service_groups:
        # The listeners of the service group cannot overlap
        listeners = get_service_group_listeners(service_group)
        for listener in listeners:
            listener_config = _get_service_group_listener_config(
                service_group, listener)
            listeners_config.append(listener_config)
    return listeners_config


def _get_service_group_listener_config(service_group, listener):
    # this will remove unrelated attributes for listener
    services = get_service_group_services(service_group)
    services_config = []
    for service in services:
        service_config = {
            "name": service["name"],
            "route_path": service["route_path"],
        }
        if "default" in service:
            service_config["default"] = service["default"]
        services_config.append(service_config)
    listener_config = {
        "protocol": listener["protocol"],
        "port": listener["port"],
        "services": services_config,
    }
    return listener_config


def _create_load_balancer_listeners(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context):
    load_balancer_id = _get_load_balancer_id(load_balancer)
    load_balancer_name = _get_load_balancer_name(load_balancer)
    load_balancer_type = get_load_balancer_config_type(load_balancer_config)
    listeners = _get_listeners_config(load_balancer_config)
    listeners_hash_context = _get_listeners_hash_context(load_balancer_context)
    listeners_context = _get_listeners_context(load_balancer_context)

    load_balancer_listeners = []
    for listener in listeners:
        load_balancer_listener = _create_load_balancer_listener(
            elb_client, load_balancer_name,
            load_balancer_id, load_balancer_type,
            listener, listeners_context)
        load_balancer_listeners.append(load_balancer_listener)
        _update_listener_hash(listeners_hash_context, listener)

    return load_balancer_listeners


def _get_listener_first_service(listener):
    # This is for the case that each listener has a single service
    services = listener.get("services")
    if not services:
        raise RuntimeError(
            "No service defined for listener.")
    return services[0]


def _get_listener_default_action(target_group):
    if target_group:
        target_group_id = _get_target_group_id(target_group)
        default_action = {
            'Type': 'forward',
            'TargetGroupArn': target_group_id,
        }
    else:
        # this happens only for application load balancer
        # show fix response of service unavailable
        default_action = {
            'Type': 'fixed-response',
            'FixedResponseConfig': {
                'MessageBody': "Service Unavailable",
                'StatusCode': "503",
                'ContentType': "text/plain"
            }
        }
    return default_action


def _create_load_balancer_listener(
        elb_client, load_balancer_name,
        load_balancer_id, load_balancer_type,
        listener, listeners_context):
    protocol = listener["protocol"]
    port = listener["port"]

    # first we need to create the default target group
    target_group = _get_listener_default_target_group(
        elb_client, load_balancer_name, load_balancer_type,
        listener)
    default_action = _get_listener_default_action(target_group)

    response = elb_client.create_listener(
        LoadBalancerArn=load_balancer_id,
        Protocol=protocol,
        Port=port,
        DefaultActions=[
            default_action
        ],
    )
    load_balancer_listener = _get_response_object(response, "Listeners")
    if not load_balancer_listener:
        raise RuntimeError(
            "Failed to create load balancer listener.")

    listener_key = _get_listener_key(listener)
    listener_context = _get_listener_context(
        listeners_context, listener_key)
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        # we need to create rules for services
        _create_listener_rules(
            elb_client, load_balancer_name,
            listener, load_balancer_listener,
            listener_context)

    return load_balancer_listener


def _modify_listener_default_action(
        elb_client, load_balancer_listener,
        target_group):
    default_action = _get_listener_default_action(target_group)
    listener_id = _get_listener_id(load_balancer_listener)
    response = elb_client.modify_listener(
        ListenerArn=listener_id,
        DefaultActions=[
            default_action
        ],
    )
    return _get_response_object(response, "Listeners")


def _get_listener_default_target_group(
        elb_client, load_balancer_name, load_balancer_type,
        listener):
    services = _get_listener_services(listener)
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        service = _get_application_default_service(
            services)
        # may not have a default service up
    else:
        # there is only one service as default
        service = services[0]
    if not service:
        return None
    target_group = _get_target_group_for_service(
        elb_client, load_balancer_name, service)
    return target_group


def _update_load_balancer_listeners(
        elb_client, load_balancer, load_balancer_config,
        load_balancer_context):
    load_balancer_id = _get_load_balancer_id(load_balancer)
    load_balancer_name = _get_load_balancer_name(load_balancer)
    load_balancer_type = get_load_balancer_config_type(load_balancer_config)
    listeners = _get_listeners_config(load_balancer_config)
    existing_listeners = _get_load_balancer_listeners(
        elb_client, load_balancer_id)

    (listeners_to_create,
     listeners_to_update,
     listeners_to_delete) = _get_listeners_for_action(
        listeners, existing_listeners)
    listeners_hash_context = _get_listeners_hash_context(load_balancer_context)
    listeners_context = _get_listeners_context(load_balancer_context)

    for listener in listeners_to_create:
        _create_load_balancer_listener(
            elb_client, load_balancer_name,
            load_balancer_id, load_balancer_type,
            listener, listeners_context)
        _update_listener_hash(listeners_hash_context, listener)

    for listener_to_update in listeners_to_update:
        listener, load_balancer_listener = listener_to_update
        if _is_listener_updated(listeners_hash_context, listener):
            _update_load_balancer_listener(
                elb_client, load_balancer_name, load_balancer_type,
                listener, load_balancer_listener,
                listeners_context)
            _update_listener_hash(listeners_hash_context, listener)

    for load_balancer_listener in listeners_to_delete:
        _delete_load_balancer_listener(
            elb_client, load_balancer_listener)
        listener_key = _get_load_balancer_listener_key(load_balancer_listener)
        _clear_listener_hash(listeners_hash_context, listener_key)


def _get_listeners_for_action(listeners, existing_listeners):
    listeners_create = []
    listeners_update = []
    listeners_to_delete = []
    # decide the listener by protocol and port
    # convert to dict for fast search
    listeners_by_key = {
        _get_listener_key(listener): listener
        for listener in listeners
    }
    existing_listeners_by_key = {
        _get_load_balancer_listener_key(existing_listener): existing_listener
        for existing_listener in existing_listeners
    }
    for listener_key, listener in listeners_by_key.items():
        if listener_key not in existing_listeners_by_key:
            listeners_create.append(listener)
        else:
            load_balancer_listener = existing_listeners_by_key[listener_key]
            listeners_update.append((listener, load_balancer_listener))

    for listener_key, existing_listeners in existing_listeners_by_key.items():
        if listener_key not in listeners_by_key:
            listeners_to_delete.append(existing_listeners)
    return listeners_create, listeners_update, listeners_to_delete


def _get_listener_target_groups(
        elb_client, load_balancer_type, load_balancer_listener):
    target_group_ids = set()
    default_target_group_id = get_default_action_target_group(load_balancer_listener)
    if default_target_group_id:
        target_group_ids.add(default_target_group_id)

    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        # get the target groups from the rules
        listener_rules = _get_listener_rules(
            elb_client, load_balancer_listener)
        for listener_rule in listener_rules:
            target_group_id = _get_target_group_of_listener_rule(listener_rule)
            if target_group_id:
                target_group_ids.add(target_group_id)

    return target_group_ids


def get_default_action_target_group(load_balancer_listener):
    default_actions = load_balancer_listener.get("DefaultActions")
    if not default_actions:
        return None
    return default_actions[0].get("TargetGroupArn")


def _delete_load_balancer_listener(
        elb_client, load_balancer_listener):
    listener_id = _get_listener_id(load_balancer_listener)
    elb_client.delete_listener(ListenerArn=listener_id)


def _delete_target_group(elb_client, target_group_id):
    elb_client.delete_target_group(
        TargetGroupArn=target_group_id)


def _update_load_balancer_listener(
        elb_client, load_balancer_name, load_balancer_type,
        listener, load_balancer_listener,
        listeners_context):
    listener_key = _get_listener_key(listener)
    listener_context = _get_listener_context(
        listeners_context, listener_key)

    _update_default_listener_rule(
        elb_client, load_balancer_name, load_balancer_type,
        listener, load_balancer_listener)

    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        _update_listener_rules(
            elb_client, load_balancer_name,
            listener, load_balancer_listener,
            listener_context)


def _update_default_listener_rule(
        elb_client, load_balancer_name, load_balancer_type,
        listener, load_balancer_listener):
    # take care of default action rule
    if load_balancer_type == LOAD_BALANCER_TYPE_APPLICATION:
        # It is possible hat the default service shows up or disappear
        _update_application_listener_default_rule(
            elb_client, load_balancer_name,
            listener, load_balancer_listener)
    else:
        default_target_group_id = get_default_action_target_group(
            load_balancer_listener)
        service = _get_listener_first_service(listener)

        # service may change, modify the listener
        _update_listener_default_action(
            elb_client, load_balancer_name, load_balancer_listener,
            default_target_group_id, service)


def _update_listener_default_action(
        elb_client, load_balancer_name, load_balancer_listener,
        default_target_group_id, service):
    # create target group for the service
    target_group = None
    if service:
        target_group = _get_target_group_for_service(
            elb_client, load_balancer_name, service)

    # check whether service changed
    if default_target_group_id and target_group:
        target_group_id = _get_target_group_id(target_group)
        if default_target_group_id == target_group_id:
            return

    # modify listener default rule to route to the target group
    _modify_listener_default_action(
        elb_client, load_balancer_listener, target_group)

    # deletion of the original default target group will be done later


#####################################
# Target group functions
#####################################


def _get_target_groups_hash_context(load_balancer_context):
    return _get_resources_context(
        load_balancer_context, TARGET_GROUPS_HASH_CONTEXT)


def _update_target_group_hash(target_groups_hash_context, service):
    service_name = service["name"]
    _update_resource_hash(
        target_groups_hash_context, service_name, service)


def _is_target_group_updated(target_groups_hash_context, service):
    service_name = service["name"]
    return _is_resource_updated(
        target_groups_hash_context, service_name, service)


def _clear_target_group_hash(target_groups_hash_context, service_name):
    _clear_resource_hash(target_groups_hash_context, service_name)


def _get_used_target_groups(
        elb_client, load_balancer_id):
    response = elb_client.describe_target_groups(
        LoadBalancerArn=load_balancer_id)
    return _get_response_list(response, "TargetGroups")


def _get_target_group_id(target_group):
    return target_group["TargetGroupArn"]


def _get_target_groups(
        elb_client, load_balancer_name):
    response = elb_client.describe_target_groups()
    target_groups = _get_response_list(response, "TargetGroups")
    if not target_groups:
        return []
    _get_resource_tags(elb_client, target_groups, "TargetGroupArn")
    # filter by load balancer name
    return [
        target_group for target_group in target_groups
        if _is_tagged_by(
            target_group.get("Tags"), CLOUDTIK_TAG_LOAD_BALANCER_NAME,
            load_balancer_name)]


def _create_target_group_for_service(
        elb_client, load_balancer_name,
        service, vpc_id):
    target_group_name = _get_target_group_name(
        load_balancer_name, service)
    target_group = _create_target_group(
        elb_client, load_balancer_name,
        target_group_name, service, vpc_id)
    if not target_group:
        raise RuntimeError(
            "Failed to create target group.")

    target_group_id = _get_target_group_id(target_group)
    _update_target_group_targets(
        elb_client, target_group_id, service)
    return target_group


def _get_target_group_name(
        load_balancer_name, service):
    # TODO: the target name length constrains:
    # This name must be unique per region per account, can have a maximum of 32 characters,
    # must contain only alphanumeric characters or hyphens, and must not begin or end with a hyphen.
    service_name = service["name"]
    return "{}-{}".format(load_balancer_name, service_name)


def _get_target_group_service_name(target_group):
    return _get_tagged_service_name(target_group)


def _get_target_group_for_service(
        elb_client, load_balancer_name, service):
    target_group_name = _get_target_group_name(
        load_balancer_name, service)
    return _get_target_group_by_name(elb_client, target_group_name)


def _create_target_group(
        elb_client, load_balancer_name, target_group_name,  service, vpc_id):
    service_name = service["name"]
    protocol = service["protocol"]
    port = service["port"]
    response = elb_client.create_target_group(
        Name=target_group_name,
        Port=port,
        Protocol=protocol,
        VpcId=vpc_id,
        TargetType="ip",
        Tags=[
            {
                'Key': CLOUDTIK_TAG_LOAD_BALANCER_NAME,
                'Value': load_balancer_name
            },
            {
                'Key': CLOUDTIK_TAG_SERVICE_NAME,
                'Value': service_name
            },
        ],
    )
    return _get_response_object(response, "TargetGroups")


def _get_target_group_by_name(
        elb_client, target_group_name):
    if not target_group_name:
        return None
    try:
        response = elb_client.describe_target_groups(
            Names=[
                target_group_name,
            ])
        return _get_response_object(response, "TargetGroups")
    except botocore.exceptions.ClientError as e:
        error_code = get_boto_error_code(e)
        if error_code == "TargetGroupNotFoundException":
            return None
        raise e


def _get_target_group_by_id(
        elb_client, target_group_id):
    if not target_group_id:
        return None
    response = elb_client.describe_target_groups(
        TargetGroupArns=[
            target_group_id,
        ])
    return _get_response_object(response, "TargetGroups")


def _update_target_group_for_service(
        elb_client, target_group, service):
    # update the targets
    target_group_id = _get_target_group_id(target_group)
    _update_target_group_targets(
        elb_client, target_group_id, service)


def _list_target_group_targets(elb_client, target_group_id):
    targets_health = elb_client.describe_target_health(
        TargetGroupArn=target_group_id,
    ).get("TargetHealthDescriptions", [])

    return [target_health["Target"] for target_health in targets_health
            if "Target" in target_health]


def _update_target_group_targets(
        elb_client, target_group_id, service):
    # decide targets to register and deregister
    targets = get_service_targets(service)
    existing_targets = _list_target_group_targets(elb_client, target_group_id)
    (targets_register,
     targets_to_deregister) = _get_targets_for_action(targets, existing_targets)

    if targets_register:
        _register_targets(elb_client, target_group_id, targets_register)
    if targets_to_deregister:
        _deregister_targets(elb_client, target_group_id, targets_to_deregister)


def _get_targets_for_action(targets, existing_targets):
    targets_register = []
    targets_to_deregister = []
    # decide the target by address and port
    # convert to dict for fast search
    targets_by_key = {
        (target["address"], target["port"]): target
        for target in targets
    }

    # If the target type is ip, the "Id" specify an IP address
    existing_targets_by_key = {
        (existing_target["Id"], existing_target["Port"]): existing_target
        for existing_target in existing_targets
    }
    for target_key, target in targets_by_key.items():
        if target_key not in existing_targets_by_key:
            targets_register.append(target)
    for target_key, existing_targets in existing_targets_by_key.items():
        if target_key not in targets_by_key:
            targets_to_deregister.append(existing_targets)
    return targets_register, targets_to_deregister


def _register_targets(
        elb_client, target_group_id, targets):
    target_group_targets = [
        {
            'Id': target["address"],
            'Port': target["port"],
        } for target in targets
    ]
    elb_client.register_targets(
        TargetGroupArn=target_group_id,
        Targets=target_group_targets,
    )


def _deregister_targets(
        elb_client, target_group_id, targets):
    elb_client.deregister_targets(
        TargetGroupArn=target_group_id,
        Targets=targets,
    )

########################################################
# These functions are for application load balancer only
########################################################


def _is_default_service(service):
    return service.get("default", False)


def _get_application_default_service(services):
    if not services:
        return None
    for service in services:
        if _is_default_service(service):
            return service
    return None


def _get_rules_hash_context(listener_context):
    return _get_resources_context(
        listener_context, LISTENER_RULES_HASH_CONTEXT)


def _update_rule_hash(rules_hash_context, service):
    service_name = service["name"]
    _update_resource_hash(
        rules_hash_context, service_name, service)


def _is_rule_updated(rules_hash_context, service):
    service_name = service["name"]
    return _is_resource_updated(
        rules_hash_context, service_name, service)


def _clear_rule_hash(rules_hash_context, service_name):
    _clear_resource_hash(rules_hash_context, service_name)


def _get_listener_services(listener):
    return listener.get("services", [])


def _get_rule_id(rule):
    return rule["RuleArn"]


def _get_services_for_rules(listener):
    services = _get_listener_services(listener)
    # exclude the default service for rules
    return [
        service for service in services
        if not _is_default_service(service)]


def _create_listener_rules(
        elb_client, load_balancer_name,
        listener, load_balancer_listener,
        listener_context):
    services = _get_services_for_rules(listener)
    rules_hash_context = _get_rules_hash_context(
        listener_context)

    listener_rules = []
    for service in services:
        listener_rule = _create_listener_rule(
            elb_client, load_balancer_name,
            load_balancer_listener, service)
        listener_rules.append(listener_rule)
        _update_rule_hash(rules_hash_context, service)

    return listener_rules


def _update_application_listener_default_rule(
        elb_client, load_balancer_name,
        listener, load_balancer_listener):
    default_target_group_id = get_default_action_target_group(
        load_balancer_listener)
    services = _get_listener_services(listener)
    service = _get_application_default_service(
        services)
    if not default_target_group_id and not service:
        # nothing changed
        return

    # default service appears, disappears or changed, modify the listener
    _update_listener_default_action(
        elb_client, load_balancer_name, load_balancer_listener,
        default_target_group_id, service)


def _update_listener_rules(
        elb_client, load_balancer_name,
        listener, load_balancer_listener,
        listener_context):
    services = _get_services_for_rules(listener)
    existing_listener_rules = _get_listener_rules(
        elb_client, load_balancer_listener)

    (rules_to_create,
     rules_to_update,
     rules_to_delete) = _get_listener_rules_for_action(
        services, existing_listener_rules)

    rules_hash_context = _get_rules_hash_context(listener_context)

    for rule_to_create in rules_to_create:
        # rule_to_create is a service
        _create_listener_rule(
            elb_client, load_balancer_name,
            load_balancer_listener, rule_to_create)
        _update_rule_hash(rules_hash_context, rule_to_create)

    for rule_to_update in rules_to_update:
        # rule_to_update is a tuple of service, listener_rule
        service, listener_rule = rule_to_update
        if _is_rule_updated(rules_hash_context, service):
            _update_listener_rule(
                elb_client, load_balancer_name,
                service, listener_rule)
            _update_rule_hash(rules_hash_context, service)

    for rule_to_delete in rules_to_delete:
        # rule_to_delete is listener_rule
        _delete_listener_rule(
            elb_client, rule_to_delete)
        service_name = _get_listener_rule_service_name(rule_to_delete)
        _clear_rule_hash(rules_hash_context, service_name)


def _create_listener_rule(
        elb_client, load_balancer_name,
        load_balancer_listener, service):
    target_group = _get_target_group_for_service(
        elb_client, load_balancer_name, service)
    if not target_group:
        raise RuntimeError(
            "Target group for service not found: {}.".format(
                service["name"]))
    # create a rule to route to the target group
    return _create_rule(
        elb_client, load_balancer_listener, service, target_group)


def _update_listener_rule(
        elb_client, load_balancer_name, service, listener_rule):
    # The route path many changed
    target_group = _get_target_group_for_service(
        elb_client, load_balancer_name, service)
    if not target_group:
        raise RuntimeError(
            "Target group for service not found: {}.".format(
                service["name"]))

    return _modify_rule(elb_client, listener_rule, service, target_group)


def _get_target_group_of_listener_rule(listener_rule):
    # currently we support one target group for one rule
    actions = listener_rule.get("Actions")
    if not actions:
        return None
    return actions[0].get("TargetGroupArn")


def _delete_listener_rule(elb_client, listener_rule):
    # delete a listener rule
    # the corresponding target group will be deleted as final step if it is not used
    _delete_rule(elb_client, listener_rule)


def _delete_rule(elb_client, listener_rule):
    rule_id = _get_rule_id(listener_rule)
    elb_client.delete_rule(
        RuleArn=rule_id
    )


def _get_listener_rules(elb_client, load_balancer_listener):
    listener_id = _get_listener_id(load_balancer_listener)
    all_listener_rules = elb_client.describe_rules(
        ListenerArn=listener_id).get("Rules", [])
    # it will return all the rules include the default rule
    # exclude the default rules
    listener_rules = [
        listener_rule for listener_rule in all_listener_rules
        if not listener_rule.get("IsDefault", False)
    ]
    _get_resource_tags(elb_client, listener_rules, "RuleArn")
    return listener_rules


def _get_listener_rule_service_name(listener_rule):
    return _get_tagged_service_name(listener_rule)


def _get_listener_rules_for_action(services, existing_listener_rules):
    rules_create = []
    rules_update = []
    rules_to_delete = []

    # convert to dict for fast search
    services_by_key = {
        service["name"]: service
        for service in services
    }
    existing_rules_by_key = {
        _get_listener_rule_service_name(listener_rule): listener_rule
        for listener_rule in existing_listener_rules
    }
    for service_name, service in services_by_key.items():
        if service_name not in existing_rules_by_key:
            rules_create.append(service)
        else:
            listener_rule = existing_rules_by_key[service_name]
            rules_update.append((service, listener_rule))

    for service_name, listener_rule in existing_rules_by_key.items():
        if service_name not in services_by_key:
            rules_to_delete.append(listener_rule)
    return rules_create, rules_update, rules_to_delete


def _create_rule(
        elb_client, load_balancer_listener, service, target_group):
    listener_id = _get_listener_id(load_balancer_listener)
    target_group_id = _get_target_group_id(target_group)
    service_name = service["name"]
    route_path = service["route_path"]
    # Note: Path-based routing rules look for an exact match.
    # If your application requires requests to be routed further down these paths,
    # for example, /svcA/doc, then include a wildcard when you write the condition
    # for the path-based routing rule.
    # So we create two matching string one for exact match to /service-path. And one
    # for match to /service-path/*.
    if route_path.endswith('/'):
        # if the route path ends with /, we don't need two uris to match
        # /abc/ will use /abc/* to match (it will not match /abc)
        # / will use /* to match
        patterns = [route_path + '*']
    else:
        patterns = [route_path, route_path + '/*']

    response = elb_client.create_rule(
        ListenerArn=listener_id,
        Actions=[
            {
                'TargetGroupArn': target_group_id,
                'Type': 'forward',
            },
        ],
        Conditions=[
            {
                'Field': 'path-pattern',
                'PathPatternConfig': {
                    'Values': patterns
                }
            },
        ],
        Tags=[
            {
                'Key': CLOUDTIK_TAG_SERVICE_NAME,
                'Value': service_name
            },
        ],
        Priority=10,
    )
    return _get_response_object(response, "Rules")


def _modify_rule(elb_client, listener_rule, service, target_group):
    rule_id = _get_rule_id(listener_rule)
    target_group_id = _get_target_group_id(target_group)
    route_path = service["route_path"]
    response = elb_client.modify_rule(
        RuleArn=rule_id,
        Actions=[
            {
                'TargetGroupArn': target_group_id,
                'Type': 'forward',
            },
        ],
        Conditions=[
            {
                'Field': 'path-pattern',
                'PathPatternConfig': {
                    'Values': [
                        route_path, route_path + '/*',
                    ]
                }
            },
        ],
    )
    return _get_response_object(response, "Rules")

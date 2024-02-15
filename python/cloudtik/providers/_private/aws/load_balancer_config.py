from typing import Dict, Any

from cloudtik.core._private.util.core_utils import batch_list, get_json_object_hash
from cloudtik.core._private.utils import get_provider_config
from cloudtik.core.tags import CLOUDTIK_TAG_WORKSPACE_NAME
from cloudtik.providers._private.aws.config import _is_workspace_tagged, _get_response_object, \
    get_workspace_private_subnets
from cloudtik.providers._private.aws.utils import tags_list_to_dict, _make_resource


def _bootstrap_load_balancer_config(
        config: Dict[str, Any], provider_config: Dict[str, Any]):
    cluster_provider_config = get_provider_config(config)
    # copy the related information from cluster provider config to provider config
    provider_config["type"] = cluster_provider_config["type"]
    provider_config["region"] = cluster_provider_config["region"]
    return provider_config


def _list_load_balancers(elb_client):
    return elb_client.describe_load_balancers().get("LoadBalancers", [])


def _get_load_balancer_tags(elb_client, load_balancers):
    for load_balancer_batch in batch_list(load_balancers, 20):
        # can specify up to 20 resources in a single call
        _get_load_balancer_tags_batch(elb_client, load_balancer_batch)


def _get_load_balancer_tags_batch(elb_client, load_balancers):
    resource_ids = [load_balancer["LoadBalancerArn"] for load_balancer in load_balancers]
    response = elb_client.describe_tags(ResourceArns=resource_ids)
    load_balancer_tags = {
        tag_desc["ResourceArn"]: tag_desc.get(
            "Tags") for tag_desc in response.get("TagDescriptions", [])}

    for load_balancer in load_balancers:
        load_balancer_id = load_balancer["LoadBalancerArn"]
        tags = load_balancer_tags.get(load_balancer_id)
        if tags:
            load_balancer["Tags"] = tags


def _list_workspace_load_balancers(elb_client, workspace_name):
    load_balancers = _list_load_balancers(elb_client)
    _get_load_balancer_tags(elb_client, load_balancers)
    return [
        load_balancer for load_balancer in load_balancers
        if _is_workspace_tagged(load_balancer.get("Tags"), workspace_name)]


def _get_workspace_load_balancer_info(elb_client, workspace_name):
    load_balancers = _list_workspace_load_balancers(
        elb_client, workspace_name)
    load_balancer_map = {}
    for load_balancer in load_balancers:
        load_balancer_name = load_balancer["LoadBalancerName"]
        load_balancer_info = _get_load_balancer_info_of(load_balancer)
        load_balancer_map[load_balancer_name] = load_balancer_info
    return load_balancer_map


def _get_load_balancer(elb_client, load_balancer_name):
    response = elb_client.describe_load_balancers(
        Names=[load_balancer_name])
    return _get_response_object(response, "LoadBalancers")


def _get_load_balancer_info_of(load_balancer):
    load_balancer_id = load_balancer["LoadBalancerArn"]
    load_balancer_name = load_balancer["LoadBalancerName"]
    load_balancer_info = {
        "id": load_balancer_id,
        "name": load_balancer_name
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


def _get_load_balancer_info(elb_client, load_balancer_name):
    load_balancer = _get_load_balancer(elb_client, load_balancer_name)
    if not load_balancer:
        return None
    _get_load_balancer_tags(elb_client, [load_balancer])
    load_balancer_id = load_balancer["LoadBalancerArn"]
    load_balancer_listeners = _get_load_balancer_listeners(
        elb_client, load_balancer_id)

    load_balancer_info = _get_load_balancer_info_of(load_balancer)
    if load_balancer_listeners:
        listeners_info = _get_load_balancer_listener_info(
            load_balancer_listeners)
        load_balancer_info["listeners"] = listeners_info

    return load_balancer_info


def _delete_load_balancer_by_name(elb_client, load_balancer_name):
    load_balancer = _get_load_balancer(elb_client, load_balancer_name)
    if not load_balancer:
        return False
    load_balancer_id = load_balancer["LoadBalancerArn"]
    _delete_load_balancer(elb_client, load_balancer_id)
    return True


def _delete_load_balancer(elb_client, load_balancer_id):
    elb_client.delete_load_balancer(
        LoadBalancerArn=load_balancer_id
    )
    wait_load_balancer_deleted(elb_client, load_balancer_id)


def wait_load_balancer_deleted(elb_client, load_balancer_id):
    waiter = elb_client.get_waiter('load_balancers_deleted')
    waiter.wait(
        LoadBalancerArns=[load_balancer_id],
        WaiterConfig={
            'Delay': 1,
            'MaxAttempts': 120
        }
    )


def _get_load_balancer_subnet_ids(
        provider_config, workspace_name, vpc_id):
    ec2 = _make_resource("ec2", provider_config)
    private_subnets = get_workspace_private_subnets(
        workspace_name, ec2, vpc_id)
    subnet_ids = [private_subnet.id for private_subnet in private_subnets]
    return subnet_ids


def _create_load_balancer(
        elb_client, provider_config,
        workspace_name, load_balancer_config,
        vpc_id, context):
    subnet_ids = _get_load_balancer_subnet_ids(
        provider_config, workspace_name, vpc_id)

    tags = load_balancer_config.get("tags", {})
    tag_pairs = [
        {'Key': CLOUDTIK_TAG_WORKSPACE_NAME, 'Value': workspace_name}
    ]
    for k, v in tags.items():
        tag_pairs.append({
            "Key": k,
            "Value": v,
        })

    # TODO: handle Scheme: internal or internet facing and elastic ip
    load_balancer_name = load_balancer_config["name"]
    schema = "internet-facing"
    response = elb_client.create_load_balancer(
        Name=load_balancer_name,
        Type='network',
        Tags=tag_pairs,
        Scheme=schema,
        Subnets=subnet_ids,
    )
    load_balancer = _get_response_object(response, "LoadBalancers")
    if not load_balancer:
        raise RuntimeError(
            "Failed to create load balancer.")
    load_balancer_id = load_balancer["LoadBalancerArn"]
    wait_load_balancer_exists(elb_client, load_balancer_id)

    # create listeners
    _create_load_balancer_listeners(
        elb_client, load_balancer_id, load_balancer_config,
        vpc_id, context)


def wait_load_balancer_exists(elb_client, load_balancer_id):
    waiter = elb_client.get_waiter('load_balancer_exists')
    waiter.wait(
        LoadBalancerArns=[load_balancer_id],
        WaiterConfig={
            'Delay': 1,
            'MaxAttempts': 120
        }
    )


def _get_listener_key(listener):
    return listener["protocol"], listener["port"]


def _get_load_balancer_listener_key(load_balancer_listener):
    return load_balancer_listener["Protocol"], load_balancer_listener["Port"]


def _update_listener_last_hash(context, listener):
    listener_key = _get_listener_key(listener)
    listener_hash = get_json_object_hash(listener)
    context[listener_key] = listener_hash


def _is_listener_updated(context, listener):
    listener_key = _get_listener_key(listener)
    old_listener_hash = context.get(listener_key)
    if not old_listener_hash:
        return True
    listener_hash = get_json_object_hash(listener)
    if listener_hash != old_listener_hash:
        return True
    return False


def _clear_listener_last_hash(context, listener_key):
    context.pop(listener_key, None)


def _get_load_balancer_listeners(elb_client, load_balancer_id):
    return elb_client.describe_listeners(
        LoadBalancerArn=load_balancer_id).get("Listeners", [])


def _create_load_balancer_listeners(
        elb_client, load_balancer_id, load_balancer_config,
        vpc_id, context):
    load_balancer_name = load_balancer_config["name"]
    listeners = load_balancer_config.get("listeners", [])

    for listener in listeners:
        _create_load_balancer_listener(
            elb_client, load_balancer_name, load_balancer_id,
            listener, vpc_id)
        _update_listener_last_hash(context, listener)


def _create_load_balancer_listener(
        elb_client, load_balancer_name, load_balancer_id,
        listener, vpc_id):

    # first we need to create a target group
    target_group = _create_load_balancer_target_group(
        elb_client, load_balancer_name, listener, vpc_id)
    if not target_group:
        raise RuntimeError(
            "Failed to create target group.")

    target_group_id = target_group["TargetGroupArn"]
    _update_target_group_targets(
        elb_client, target_group_id, listener)

    protocol = listener["protocol"]
    port = listener["port"]
    response = elb_client.create_listener(
        LoadBalancerArn=load_balancer_id,
        Protocol=protocol,
        Port=port,
        DefaultActions=[
            {
                'TargetGroupArn': target_group_id,
                'Type': 'forward',
            },
        ],
    )
    load_balancer_listener = _get_response_object(response, "Listeners")

    return load_balancer_listener


def _get_load_balancer_target_group_name(
        load_balancer_name, protocol, port):
    return "{}-{}-{}".format(load_balancer_name, protocol, port)


def _create_load_balancer_target_group(
        elb_client, load_balancer_name, listener, vpc_id):
    protocol = listener["protocol"]
    port = listener["port"]
    target_group_name = _get_load_balancer_target_group_name(
        load_balancer_name, protocol, port)
    response = elb_client.create_target_group(
        Name=target_group_name,
        Port=port,
        Protocol=protocol,
        VpcId=vpc_id,
        TargetType="ip"
    )
    return _get_response_object(response, "TargetGroups")


def _get_load_balancer_target_group(
        elb_client, load_balancer_name, load_balancer_id, listener):
    protocol = listener["protocol"]
    port = listener["port"]
    target_group_name = _get_load_balancer_target_group_name(
        load_balancer_name, protocol, port)
    response = elb_client.describe_target_groups(
        LoadBalancerArn=load_balancer_id,
        Names=[
            target_group_name,
        ])
    return _get_response_object(response, "TargetGroups")


def _update_load_balancer(
        elb_client, provider_config,
        workspace_name, load_balancer_config,
        vpc_id, context):
    # The load balancer exists
    # we track the last settings we updated in context
    load_balancer_name = load_balancer_config["name"]
    load_balancer = _get_load_balancer(elb_client, load_balancer_name)
    if not load_balancer:
        raise RuntimeError(
            "Load balancer with name {} doesn't exist.".format(load_balancer_name))

    load_balancer_id = load_balancer["LoadBalancerArn"]
    _update_load_balancer_listeners(
        elb_client, load_balancer_id, load_balancer_config,
        vpc_id, context)


def _update_load_balancer_listeners(
        elb_client, load_balancer_id, load_balancer_config,
        vpc_id, context):
    load_balancer_name = load_balancer_config["name"]
    listeners = load_balancer_config.get("listeners", [])
    existing_listeners = _get_load_balancer_listeners(
        elb_client, load_balancer_id)

    (listeners_to_create,
     listeners_to_update,
     listeners_to_delete) = _get_listeners_for_action(
        listeners, existing_listeners)

    for listener in listeners_to_create:
        _create_load_balancer_listener(
            elb_client, load_balancer_name, load_balancer_id,
            listener, vpc_id)
        _update_listener_last_hash(context, listener)

    for listener in listeners_to_update:
        if _is_listener_updated(context, listener):
            _update_load_balancer_listener(
                elb_client, load_balancer_name, load_balancer_id,
                listener)
            _update_listener_last_hash(context, listener)

    for load_balancer_listener in listeners_to_delete:
        _delete_load_balancer_listener(
            elb_client, load_balancer_listener)
        listener_key = _get_load_balancer_listener_key(load_balancer_listener)
        _clear_listener_last_hash(context, listener_key)


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
            listeners_update.append(listener)

    for listener_key, existing_listeners in existing_listeners_by_key.items():
        if listener_key not in listeners_by_key:
            listeners_to_delete.append(existing_listeners)
    return listeners_create, listeners_update, listeners_to_delete


def _get_load_balancer_listener_target_group(load_balancer_listener):
    default_actions = load_balancer_listener.get("DefaultActions")
    if not default_actions:
        return None
    return default_actions[0].get("TargetGroupArn")


def _delete_load_balancer_listener(
        elb_client, load_balancer_listener):
    listener_id = load_balancer_listener["ListenerArn"]
    elb_client.delete_listener(ListenerArn=listener_id)

    target_group_id = _get_load_balancer_listener_target_group(
        load_balancer_listener)
    if target_group_id:
        elb_client.delete_target_group(
            TargetGroupArn=target_group_id)


def _update_load_balancer_listener(
        elb_client, load_balancer_name, load_balancer_id,
        listener):
    target_group = _get_load_balancer_target_group(
        elb_client, load_balancer_name, load_balancer_id, listener)
    if not target_group:
        raise RuntimeError(
            "Target group for listener doesn't exist.")

    # update the targets
    target_group_id = target_group["TargetGroupArn"]
    _update_target_group_targets(
        elb_client, target_group_id, listener)


def _list_target_group_targets(elb_client, target_group_id):
    targets_health = elb_client.describe_target_health(
        TargetGroupArn=target_group_id,
    ).get("TargetHealthDescriptions", [])

    return [target_health["Target"] for target_health in targets_health
            if "Target" in target_health]


def _update_target_group_targets(
        elb_client, target_group_id, listener):
    # decide targets to register and deregister
    targets = listener.get("targets", [])
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
    # decide the target by ip and port
    # convert to dict for fast search
    targets_by_key = {
        (target["ip"], target["port"]): target
        for target in targets
    }
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
            'Id': target["ip"],
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

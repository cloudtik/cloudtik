import copy
import os
import logging
from typing import Any, Dict, Optional

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend

from googleapiclient import errors

from google.oauth2 import service_account

from cloudtik.core._private.util.core_utils import get_config_for_update, get_node_ip_address, open_with_mode
from cloudtik.core._private.util.database_utils import DATABASE_ENGINE_POSTGRES, DATABASE_ENGINE_MYSQL
from cloudtik.core.workspace_provider import Existence, CLOUDTIK_MANAGED_CLOUD_STORAGE, \
    CLOUDTIK_MANAGED_CLOUD_STORAGE_URI, CLOUDTIK_MANAGED_CLOUD_DATABASE, CLOUDTIK_MANAGED_CLOUD_DATABASE_ENDPOINT, \
    CLOUDTIK_MANAGED_CLOUD_STORAGE_NAME, CLOUDTIK_MANAGED_CLOUD_DATABASE_ENGINE, \
    CLOUDTIK_MANAGED_CLOUD_DATABASE_ADMIN_USER, CLOUDTIK_MANAGED_CLOUD_DATABASE_PORT, \
    CLOUDTIK_MANAGED_CLOUD_DATABASE_NAME

from cloudtik.core.tags import CLOUDTIK_TAG_NODE_KIND, NODE_KIND_HEAD, CLOUDTIK_TAG_CLUSTER_NAME, \
    CLOUDTIK_TAG_WORKSPACE_NAME, CLOUDTIK_TAG_NODE_SEQ_ID
from cloudtik.core._private.cli_logger import cli_logger, cf
from cloudtik.core._private.utils import check_cidr_conflict, unescape_private_key, is_use_internal_ip, \
    is_managed_cloud_storage, is_use_managed_cloud_storage, is_worker_role_for_cloud_storage, \
    _is_use_managed_cloud_storage, is_use_peering_vpc, is_use_working_vpc, _is_use_working_vpc, \
    is_peering_firewall_allow_working_subnet, is_peering_firewall_allow_ssh_only, is_gpu_runtime, \
    is_managed_cloud_database, is_use_managed_cloud_database, is_permanent_data_volumes, _is_permanent_data_volumes, \
    _get_managed_cloud_storage_name, _get_managed_cloud_database_name, enable_stable_node_seq_id, get_provider_config, \
    get_workspace_name, get_available_node_types
from cloudtik.providers._private.gcp.node import GCPCompute
from cloudtik.providers._private.gcp.utils import _get_node_info, construct_clients_from_provider_config, \
    wait_for_compute_global_operation, wait_for_compute_region_operation, _create_storage, \
    wait_for_crm_operation, HAS_TPU_PROVIDER_FIELD, _is_head_node_a_tpu, _has_tpus_in_node_configs, \
    export_gcp_cloud_storage_config, get_service_account_email, construct_storage_client, construct_storage, \
    get_gcp_cloud_storage_config, get_gcp_cloud_storage_config_for_update, GCP_GCS_BUCKET, get_gcp_cloud_storage_uri, \
    GCP_DATABASE_ENDPOINT, get_gcp_database_config_for_update, construct_sql_admin, get_gcp_database_config, \
    wait_for_sql_admin_operation, export_gcp_cloud_database_config, construct_compute_client, \
    construct_service_networking, \
    wait_for_service_networking_operation, get_gcp_database_engine, get_gcp_database_default_admin_user, \
    get_gcp_database_default_port, wait_for_compute_zone_operation, get_network_url
from cloudtik.providers._private.utils import StorageTestingError

logger = logging.getLogger(__name__)

VERSION = "v1"

GCP_RESOURCE_NAME_PREFIX = "cloudtik"

GCP_HEAD_SERVICE_ACCOUNT_ID = GCP_RESOURCE_NAME_PREFIX + "-{}"
GCP_HEAD_SERVICE_ACCOUNT_DISPLAY_NAME = "CloudTik Head Service Account - {}"

GCP_WORKER_SERVICE_ACCOUNT_ID = GCP_RESOURCE_NAME_PREFIX + "-w-{}"
GCP_WORKER_SERVICE_ACCOUNT_DISPLAY_NAME = "CloudTik Worker Service Account - {}"

GCP_WORKSPACE_VPC_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-vpc"
GCP_WORKSPACE_SUBNET_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-{}-subnet"

GCP_WORKSPACE_ROUTER_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-private-router"
GCP_WORKSPACE_NAT_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-nat"

GCP_WORKSPACE_DEFAULT_INTERNAL_FIREWALL_NAME = (
        GCP_RESOURCE_NAME_PREFIX + "-{}-default-allow-internal-firewall")
GCP_WORKSPACE_LOAD_BALANCER_HEALTH_CHECK_FIREWALL_NAME = (
        GCP_RESOURCE_NAME_PREFIX + "-{}-load-balancer-health-check-firewall")
GCP_WORKSPACE_CUSTOM_FIREWALL_NAME = (
        GCP_RESOURCE_NAME_PREFIX + "-{}-custom-{}-firewall")

GCP_WORKSPACE_VPC_PEERING_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-a-peer"
GCP_WORKING_VPC_PEERING_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-b-peer"

GCP_WORKSPACE_DATABASE_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-db"
GCP_WORKSPACE_DATABASE_GLOBAL_ADDRESS_NAME = GCP_RESOURCE_NAME_PREFIX + "-{}-addr"

GCP_WORKSPACE_PUBLIC_SUBNET = "public"
GCP_WORKSPACE_PRIVATE_SUBNET = "private"

# We currently create only regional proxy-only subnet
GCP_WORKSPACE_GLOBAL_PROXY_SUBNET = "global-proxy"
GCP_WORKSPACE_REGIONAL_PROXY_SUBNET = "regional-proxy"

GCP_SERVICE_NETWORKING_NAME = "servicenetworking.googleapis.com"

# Notes for roles:
# roles/compute.networkAdmin:
# Permissions to create, modify, and delete networking resources, except for
# firewall rules and SSL certificates. The network admin role allows read-only
# access to firewall rules, SSL certificates, and instances.

# roles/compute.securityAdmin:
# Permissions to create, modify, and delete firewall rules and SSL certificates.

# Those roles will always be added.
HEAD_SERVICE_ACCOUNT_ROLES = [
    "roles/compute.admin",
    "roles/storage.admin",
    # TODO: The network admin role for load balancers in more fine grained way
    "roles/compute.networkAdmin",
    "roles/iam.serviceAccountUser",
]

# Those roles will always be added.
WORKER_SERVICE_ACCOUNT_ROLES = [
    "roles/compute.viewer",
    "roles/storage.admin",
    # TODO: The network admin role for load balancers in more fine grained way
    "roles/compute.networkAdmin",
    "roles/iam.serviceAccountUser",
]

# Those roles will only be added if there are TPU nodes defined in config.
TPU_SERVICE_ACCOUNT_ROLES = ["roles/tpu.admin"]

# NOTE: iam.serviceAccountUser allows the Head Node to create worker nodes
# with ServiceAccounts.

GCP_WORKSPACE_NUM_CREATION_STEPS = 8
GCP_WORKSPACE_NUM_DELETION_STEPS = 7
GCP_WORKSPACE_NUM_UPDATE_STEPS = 1
GCP_WORKSPACE_TARGET_RESOURCES = 9

"""
Key Concepts to note for GCP:

A VPC network is a global resource, but individual subnets are regional resources.

GCP VPC networks do not have any IP address ranges associated with them.
When you create a subnet, you must define a primary IP address range as
long as the address range is a valid Private IPv4 address ranges, for example:
10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16

Subnets regionally segment the network IP space into prefixes (subnets)
and control which prefix an instance's internal IP address is allocated from.

A virtual machine (VM) instance is located within a zone and can access
global resources or resources within the same zone.

The key allow a network interface to be allocated with an IP address and gain
access to internet is accessConfigs in the networkInterfaces of VM instances:
"accessConfigs": [{
    "name": "External NAT",
    "type": "ONE_TO_ONE_NAT",
}]

To allow private subnet have access to internet, a router with NAT for the subnet
needs to be created.

The interface with access config with Public IP address and External NAT doesn't
need extra NAT router.

What if the network interface has both external IP and NAT router configured:
Public NAT gateway provides NAT for the VM's network interface's primary internal
IP address, provided that the network interface doesn't have an external IP address
assigned to it. If the network interface has an external IP address assigned to it,
Google Cloud automatically performs one-to-one NAT for packets whose sources match
the interface's primary internal IP address because the network interface meets the
Google Cloud internet access requirements. The existence of an external IP address
on an interface always takes precedence and always performs one-to-one NAT, without
using Public NAT. (For this, we may don't need public subnet for GCP, the only thing
needed is to assign external IP address to head network interface.)

Notes to security:

GCP firewall rules are global resource. The firewall rules apply to a given project and network.
VPC firewall rules let you allow or deny connections to or from virtual machine (VM) instances
in your VPC network.

The current firewall rules:
1. Allows communication between all existing subnets of workspace VPC (including proxy-only subnet).
(Newly created subnets not included. Because VPC network doesn't have a fixed CIDR)
2. Allow rule for specific sources for SSH on 22 port. (for public SSH to head)
3. Allow SSH on 22 port within the VPC.
4. If using working node to peering, allow Working node VPC to SSH on 22 port.
5. From load balancer health checks from 130.211.0.0/22,35.191.0.0/16

"""


######################
# Workspace functions
######################


def get_workspace_subnet_name_of_type(workspace_name, subnet_type):
    return GCP_WORKSPACE_SUBNET_NAME.format(workspace_name, subnet_type)


def get_workspace_subnet_name(workspace_name, is_private=True):
    subnet_type = 'private' if is_private else 'public'
    return get_workspace_subnet_name_of_type(workspace_name, subnet_type)


def get_workspace_public_subnet_name(workspace_name):
    # We may need to remove the public subnet concept
    return get_workspace_subnet_name(workspace_name, is_private=False)


def get_workspace_proxy_subnet_name(workspace_name, is_regional=True):
    subnet_type = (
        GCP_WORKSPACE_GLOBAL_PROXY_SUBNET
        if is_regional else GCP_WORKSPACE_REGIONAL_PROXY_SUBNET)
    return get_workspace_subnet_name_of_type(
        workspace_name, subnet_type)


def get_workspace_router_name(workspace_name):
    return GCP_WORKSPACE_ROUTER_NAME.format(workspace_name)


def get_workspace_head_nodes(provider_config, workspace_name):
    _, _, compute, tpu = \
        construct_clients_from_provider_config(provider_config)
    return _get_workspace_head_nodes(
        provider_config, workspace_name, compute=compute)


def _get_workspace_head_nodes(provider_config, workspace_name, compute):
    use_working_vpc = _is_use_working_vpc(provider_config)
    project_id = provider_config.get("project_id")
    availability_zone = provider_config.get("availability_zone")
    vpc_id = _get_gcp_vpc_id(
        provider_config, workspace_name, compute, use_working_vpc)
    if vpc_id is None:
        raise RuntimeError(
            "Failed to get the VPC. The workspace {} doesn't exist or is in the wrong state.".format(
                workspace_name
            ))
    vpc_self_link = compute.networks().get(
        project=project_id, network=vpc_id).execute()["selfLink"]

    filter_expr = '(labels.{key} = {value}) AND (status = RUNNING)'.format(
        key=CLOUDTIK_TAG_NODE_KIND, value=NODE_KIND_HEAD)

    response = compute.instances().list(
        project=project_id,
        zone=availability_zone,
        filter=filter_expr,
    ).execute()

    all_heads = response.get("items", [])
    workspace_heads = []
    for head in all_heads:
        in_workspace = False
        for networkInterface in head.get("networkInterfaces", []):
            if networkInterface.get("network") == vpc_self_link:
                in_workspace = True
        if in_workspace:
            workspace_heads.append(head)

    return workspace_heads


def create_gcp_workspace(config):
    config = copy.deepcopy(config)

    # Steps of configuring the workspace
    config = _create_workspace(config)

    return config


def _create_workspace(config):
    crm, iam, compute, tpu = construct_clients_from_provider_config(
        config["provider"])
    workspace_name = get_workspace_name(config)
    managed_cloud_storage = is_managed_cloud_storage(config)
    managed_cloud_database = is_managed_cloud_database(config)
    use_peering_vpc = is_use_peering_vpc(config)

    current_step = 1
    total_steps = GCP_WORKSPACE_NUM_CREATION_STEPS
    if managed_cloud_storage:
        total_steps += 1
    if managed_cloud_database:
        total_steps += 1
    if use_peering_vpc:
        total_steps += 1

    try:
        with cli_logger.group(
                "Creating workspace: {}", workspace_name):
            with cli_logger.group(
                    "Configuring project",
                    _numbered=("[]", current_step, total_steps)):
                current_step += 1
                config = _configure_project(config, crm)

            current_step = _create_network_resources(
                config, current_step, total_steps)

            with cli_logger.group(
                    "Creating service accounts",
                    _numbered=("[]", current_step, total_steps)):
                current_step += 1
                config = _create_workspace_service_accounts(config, crm, iam)

            if managed_cloud_storage:
                with cli_logger.group(
                        "Creating GCS bucket",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    config = _create_workspace_cloud_storage(config)

            if managed_cloud_database:
                with cli_logger.group(
                        "Creating managed database",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    _create_workspace_cloud_database(config)
    except Exception as e:
        cli_logger.error(
            "Failed to create workspace with the name {}. "
            "You need to delete and try create again. {}",
            workspace_name, str(e))
        raise e

    cli_logger.success(
        "Successfully created workspace: {}.",
        cf.bold(workspace_name))

    return config


def _configure_project(config, crm):
    """Setup a Google Cloud Platform Project.

    Google Compute Platform organizes all the resources, such as storage
    buckets, users, and instances under projects. This is different from
    aws ec2 where everything is global.
    """
    config = copy.deepcopy(config)

    project_id = config["provider"].get("project_id")
    assert config["provider"]["project_id"] is not None, (
        "'project_id' must be set in the 'provider' section of the"
        " config. Notice that the project id must be globally unique.")
    project = _get_project(project_id, crm)

    if project is None:
        #  Project not found, try creating it
        _create_project(project_id, crm)
        project = _get_project(project_id, crm)
    else:
        cli_logger.print(
            "Using the existing project: {}.".format(project_id))

    assert project is not None, "Failed to create project"
    assert project["lifecycleState"] == "ACTIVE", (
        "Project status needs to be ACTIVE, got {}".format(
            project["lifecycleState"]))

    config["provider"]["project_id"] = project["projectId"]

    return config


def get_workspace_vpc_id(config, compute):
    return _get_workspace_vpc_id(
        config["provider"], config["workspace_name"], compute)


def _get_workspace_vpc_name(workspace_name):
    return GCP_WORKSPACE_VPC_NAME.format(workspace_name)


def _get_workspace_vpc_id(provider_config, workspace_name, compute):
    project_id = provider_config.get("project_id")
    vpc_name = _get_workspace_vpc_name(workspace_name)
    cli_logger.verbose(
        "Getting the VPC Id for workspace: {}...".format(vpc_name))

    vpc_ids = [
        vpc["id"] for vpc in compute.networks().list(
            project=project_id).execute().get("items", "")
        if vpc["name"] == vpc_name]
    if len(vpc_ids) == 0:
        cli_logger.verbose_error(
            "The VPC for workspace is not found: {}.".format(vpc_name))
        return None
    else:
        cli_logger.verbose(
            "Successfully get the VPC Id of {} for workspace.".format(vpc_name))
        return vpc_ids[0]


def _delete_vpc(config, compute):
    use_working_vpc = is_use_working_vpc(config)
    if use_working_vpc:
        cli_logger.print(
            "Will not delete the current working VPC.")
        return

    vpc_id = get_workspace_vpc_id(config, compute)
    project_id = config["provider"].get("project_id")
    vpc_name = _get_workspace_vpc_name(config["workspace_name"])

    if vpc_id is None:
        cli_logger.print(
            "The VPC: {} doesn't exist.".format(vpc_name))
        return

    """ Delete the VPC """
    cli_logger.print(
        "Deleting the VPC: {}...".format(vpc_name))

    try:
        operation = compute.networks().delete(
            project=project_id, network=vpc_id).execute()
        wait_for_compute_global_operation(project_id, operation, compute)
        cli_logger.print(
            "Successfully deleted the VPC: {}.".format(vpc_name))
    except Exception as e:
        cli_logger.error(
            "Failed to delete the VPC: {}. {}", vpc_name, str(e))
        raise e


def create_vpc(config, compute):
    project_id = config["provider"].get("project_id")
    vpc_name = _get_workspace_vpc_name(config["workspace_name"])
    network_body = {
        "autoCreateSubnetworks": False,
        "description": "Auto created network by cloudtik",
        "name": vpc_name,
        "routingConfig": {
            "routingMode": "REGIONAL"
        },
        "mtu": 1460
    }

    cli_logger.print(
        "Creating workspace VPC: {}...", vpc_name)
    # create vpc
    try:
        operation = compute.networks().insert(project=project_id, body=network_body).execute()
        wait_for_compute_global_operation(project_id, operation, compute)
        cli_logger.print(
            "Successfully created workspace VPC: {}", vpc_name)
    except Exception as e:
        cli_logger.error(
            "Failed to create workspace VPC. {}", str(e))
        raise e


def get_vpc_name_by_id(config, compute, vpc_id):
    provider_config = get_provider_config(config)
    project_id = provider_config.get("project_id")
    return compute.networks().get(
        project=project_id, network=vpc_id).execute()["name"]


def get_working_node_vpc_id(config, compute):
    return _get_working_node_vpc_id(config["provider"], compute)


def get_working_node_vpc_name(config, compute):
    return _get_working_node_vpc_name(config["provider"], compute)


def _find_working_node_network_interface(provider_config, compute):
    ip_address = get_node_ip_address(address="8.8.8.8:53")
    project_id = provider_config.get("project_id")
    zone = provider_config.get("availability_zone")
    instances = compute.instances().list(
        project=project_id, zone=zone).execute()["items"]
    for instance in instances:
        for networkInterface in instance.get("networkInterfaces"):
            if networkInterface.get("networkIP") == ip_address:
                return networkInterface
    return None


def _find_working_node_vpc(provider_config, compute):
    network_interface = _find_working_node_network_interface(
        provider_config, compute)
    if network_interface is None:
        cli_logger.verbose_error(
            "Failed to get the VPC of the working node. "
            "Please check whether the working node is a GCP instance.")
        return None

    network = network_interface.get("network").split("/")[-1]
    cli_logger.verbose(
        "Successfully get the VPC for working node.")
    return network


def _split_subnetwork_info(project_id, subnetwork_url):
    info = subnetwork_url.split(
        "projects/" + project_id + "/regions/")[-1].split("/")
    subnetwork_region = info[0]
    subnet_name = info[-1]
    return subnetwork_region, subnet_name


def _find_working_node_subnetwork(provider_config, compute):
    network_interface = _find_working_node_network_interface(
        provider_config, compute)
    if network_interface is None:
        return None

    subnetwork = network_interface.get("subnetwork")
    cli_logger.verbose(
        "Successfully get the VPC for working node.")
    return subnetwork


def _get_working_node_vpc(provider_config, compute):
    network = _find_working_node_vpc(provider_config, compute)
    if network is None:
        return None

    project_id = provider_config.get("project_id")
    return compute.networks().get(
        project=project_id, network=network).execute()


def _get_working_node_vpc_id(provider_config, compute):
    vpc = _get_working_node_vpc(provider_config, compute)
    if vpc is None:
        return None
    return vpc["id"]


def _get_working_node_vpc_name(provider_config, compute):
    vpc = _get_working_node_vpc(provider_config, compute)
    if vpc is None:
        return None
    return vpc["name"]


def _configure_gcp_subnets_cidr(config, compute, vpc_id, num_cidr):
    project_id = config["provider"].get("project_id")
    region = config["provider"].get("region")
    vpc_self_link = compute.networks().get(
        project=project_id, network=vpc_id).execute()["selfLink"]
    subnets = compute.subnetworks().list(
        project=project_id, region=region,
        filter='((network = \"{}\"))'.format(vpc_self_link)).execute().get("items", [])
    cidr_list = []

    if len(subnets) == 0:
        for i in range(0, num_cidr):
            cidr_list.append("10.0." + str(i) + ".0/24")
    else:
        cidr_blocks = [subnet["ipCidrRange"] for subnet in subnets]
        ip = cidr_blocks[0].split("/")[0].split(".")
        for i in range(0, 256):
            tmp_cidr_block = ip[0] + "." + ip[1] + "." + str(i) + ".0/24"
            if check_cidr_conflict(tmp_cidr_block, cidr_blocks):
                cidr_list.append(tmp_cidr_block)
                cli_logger.print("Choose CIDR: {}".format(tmp_cidr_block))

            if len(cidr_list) == num_cidr:
                break

    return cidr_list


def _delete_subnet(config, compute, is_private=True):
    if is_private:
        subnet_type = GCP_WORKSPACE_PRIVATE_SUBNET
    else:
        subnet_type = GCP_WORKSPACE_PUBLIC_SUBNET

    _delete_subnet_of_type(config, compute, subnet_type)


def _delete_subnet_of_type(config, compute, subnet_type):
    project_id = config["provider"].get("project_id")
    region = config["provider"].get("region")
    workspace_name = get_workspace_name(config)
    subnet_name = get_workspace_subnet_name_of_type(
        workspace_name, subnet_type)

    if get_subnet(config, subnet_name, compute) is None:
        cli_logger.print(
            "The {} subnet {} doesn't exist in workspace. Skip deletion."
            .format(subnet_type, subnet_name))
        return

    # """ Delete custom subnet """
    cli_logger.print(
        "Deleting {} subnet: {}...".format(subnet_type, subnet_name))
    try:
        operation = compute.subnetworks().delete(
            project=project_id, region=region,
            subnetwork=subnet_name).execute()
        wait_for_compute_region_operation(project_id, region, operation, compute)
        cli_logger.print(
            "Successfully deleted {} subnet: {}.",
            subnet_type, subnet_name)
    except Exception as e:
        cli_logger.error(
            "Failed to delete the {} subnet: {}. {}",
            subnet_type, subnet_name, str(e))
        raise e


def _delete_proxy_subnets(config, compute):
    _delete_subnet_of_type(
        config, compute, GCP_WORKSPACE_REGIONAL_PROXY_SUBNET)


def _create_and_configure_subnets(config, compute, vpc_id):
    workspace_name = get_workspace_name(config)
    project_id = config["provider"]["project_id"]
    region = config["provider"]["region"]
    num_cidr = 2
    cidr_list = _configure_gcp_subnets_cidr(config, compute, vpc_id, num_cidr)
    if len(cidr_list) != num_cidr:
        raise RuntimeError(
            "Failed to get {} free CIDR ranges for VPC: {}.".format(
                num_cidr, vpc_id))

    subnets_type = [GCP_WORKSPACE_PUBLIC_SUBNET, GCP_WORKSPACE_PRIVATE_SUBNET]
    for i in range(2):
        subnet_name = get_workspace_subnet_name_of_type(
            workspace_name, subnets_type[i])
        cli_logger.print(
            "Creating subnet for the vpc: {} with CIDR: {}...",
            vpc_id, cidr_list[i])
        network_body = {
            "description": "Auto created {} subnet for cloudtik".format(subnets_type[i]),
            "enableFlowLogs": False,
            "ipCidrRange": cidr_list[i],
            "name": subnet_name,
            "network": get_network_url(project_id, vpc_id),
            "stackType": "IPV4_ONLY",
            "privateIpGoogleAccess": False if subnets_type[i] == GCP_WORKSPACE_PUBLIC_SUBNET else True,
            "region": region
        }
        try:
            operation = compute.subnetworks().insert(
                project=project_id, region=region, body=network_body).execute()
            wait_for_compute_region_operation(project_id, region, operation, compute)
            cli_logger.print(
                "Successfully created subnet: {}.".format(subnet_name))
        except Exception as e:
            cli_logger.error(
                "Failed to create subnet. {}",  str(e))
            raise e


def _create_or_update_proxy_subnets(config, compute, vpc_id):
    # create only regional proxy-only subnet
    _create_or_update_proxy_subnet(
        config, compute, vpc_id)


def _create_or_update_proxy_subnet(config, compute, vpc_id, is_regional=True):
    workspace_name = get_workspace_name(config)
    project_id = config["provider"]["project_id"]
    region = config["provider"]["region"]
    subnet_type = get_workspace_proxy_subnet_name(workspace_name, is_regional)
    subnet_name = get_workspace_subnet_name_of_type(
        workspace_name, subnet_type)

    subnet = get_subnet(config, subnet_name, compute)
    if subnet:
        cli_logger.print(
            "The {} subnet {} already exists for workspace. Skip creation.",
            subnet_type, subnet_name)
        return

    cidr_list = _configure_gcp_subnets_cidr(config, compute, vpc_id, 1)
    if not cidr_list:
        raise RuntimeError(
            "Failed to get free CIDR range for VPC: {}.".format(vpc_id))

    cidr_range = cidr_list[0]
    purpose = "GLOBAL_MANAGED_PROXY" if not is_regional else "REGIONAL_MANAGED_PROXY"

    cli_logger.print(
        "Creating subnet for the vpc: {} with CIDR: {}...",
        vpc_id, cidr_range)

    # The enableFlowLogs field isn't supported if the subnet purpose field is set
    # to GLOBAL_MANAGED_PROXY or REGIONAL_MANAGED_PROXY.
    network_body = {
        "description": "Auto created {} subnet for cloudtik".format(subnet_type),
        "purpose": purpose,
        "role": "ACTIVE",
        "ipCidrRange": cidr_range,
        "name": subnet_name,
        "network": get_network_url(project_id, vpc_id),
        "region": region
    }
    try:
        operation = compute.subnetworks().insert(
            project=project_id, region=region, body=network_body).execute()
        wait_for_compute_region_operation(project_id, region, operation, compute)
        cli_logger.print(
            "Successfully created subnet: {}.".format(subnet_name))
    except Exception as e:
        cli_logger.error(
            "Failed to create subnet. {}",  str(e))
        raise e


def _create_router(config, compute, vpc_id):
    project_id = config["provider"]["project_id"]
    region = config["provider"]["region"]
    workspace_name = get_workspace_name(config)
    router_name = get_workspace_router_name(workspace_name)
    vpc_name = _get_workspace_vpc_name(workspace_name)
    cli_logger.print(
        "Creating router for the private subnet: {}...".format(router_name))
    router_body = {
        "bgp": {
            "advertiseMode": "CUSTOM"
        },
        "description": "Auto created for the workspace: {}".format(vpc_name),
        "name": router_name,
        "network": get_network_url(project_id, vpc_id),
        "region": "projects/{}/regions/{}".format(project_id, region)
    }
    try:
        operation = compute.routers().insert(
            project=project_id, region=region, body=router_body).execute()
        wait_for_compute_region_operation(project_id, region, operation, compute)
        cli_logger.print(
            "Successfully created router for the private subnet.")
    except Exception as e:
        cli_logger.error(
            "Failed to create router for the private subnet. {}", str(e))
        raise e


def _create_nat_for_router(config, compute):
    project_id = config["provider"]["project_id"]
    region = config["provider"]["region"]
    workspace_name = get_workspace_name(config)
    nat_name = GCP_WORKSPACE_NAT_NAME.format(workspace_name)

    cli_logger.print(
        "Creating NAT for private subnet router: {}... ".format(nat_name))

    router = get_workspace_router_name(workspace_name)
    subnet_name = get_workspace_subnet_name(workspace_name)
    private_subnet = get_subnet(config, subnet_name, compute)
    private_subnet_self_link = private_subnet.get("selfLink")
    router_body = {
        "nats": [
            {
                "natIpAllocateOption": "AUTO_ONLY",
                "name": nat_name,
                "subnetworks": [
                    {
                        "sourceIpRangesToNat": [
                            "ALL_IP_RANGES"
                        ],
                        "name": private_subnet_self_link
                    }
                ],
                "sourceSubnetworkIpRangesToNat": "LIST_OF_SUBNETWORKS"
            }
        ]
    }

    try:
        operation = compute.routers().patch(
            project=project_id, region=region, router=router, body=router_body).execute()
        wait_for_compute_region_operation(project_id, region, operation, compute)
        cli_logger.print(
            "Successfully created NAT for the private subnet router: {}.",
            nat_name)
    except Exception as e:
        cli_logger.error(
            "Failed to create NAT for the private subnet router. {}", str(e))
        raise e


def _delete_router(config, compute):
    project_id = config["provider"]["project_id"]
    region = config["provider"]["region"]
    workspace_name = get_workspace_name(config)
    router_name = get_workspace_router_name(workspace_name)

    if get_router(config, router_name, compute) is None:
        cli_logger.print(
            "The router doesn't exist: {}. Skip deletion.",
            router_name)
        return

    # """ Delete custom subnet """
    cli_logger.print(
        "Deleting the router: {}...".format(router_name))
    try:
        operation = compute.routers().delete(
            project=project_id, region=region, router=router_name).execute()
        wait_for_compute_region_operation(project_id, region, operation, compute)
        cli_logger.print(
            "Successfully deleted the router: {}.".format(router_name))
    except Exception as e:
        cli_logger.error(
            "Failed to delete the router: {}. {}", router_name, str(e))
        raise e


def check_firewall_exist(config, compute, firewall_name):
    if get_firewall(config, compute, firewall_name) is None:
        return False
    else:
        return True


def get_firewall(config, compute, firewall_name):
    project_id = config["provider"]["project_id"]
    firewall = None
    cli_logger.verbose(
        "Getting the existing firewall: {}...".format(firewall_name))
    try:
        firewall = compute.firewalls().get(
            project=project_id, firewall=firewall_name).execute()
        cli_logger.verbose(
            "Successfully get the firewall: {}.".format(firewall_name))
    except Exception:
        cli_logger.verbose_error(
            "Failed to get the firewall: {}.".format(firewall_name))
    return firewall


def create_firewall(compute, project_id, firewall_body):
    cli_logger.print(
        "Creating firewall: {}... ".format(firewall_body.get("name")))
    try:
        operation = compute.firewalls().insert(
            project=project_id, body=firewall_body).execute()
        wait_for_compute_global_operation(project_id, operation, compute)
        cli_logger.print(
            "Successfully created firewall: {}.".format(firewall_body.get("name")))
    except Exception as e:
        cli_logger.error(
            "Failed to create firewall. {}", str(e))
        raise e


def update_firewall(compute, project_id, firewall_body):
    cli_logger.print(
        "Updating firewall: {}... ".format(firewall_body.get("name")))
    try:
        operation = compute.firewalls().update(
            project=project_id,
            firewall=firewall_body.get("name"),
            body=firewall_body).execute()
        wait_for_compute_global_operation(project_id, operation, compute)
        cli_logger.print(
            "Successfully updated firewall: {}.".format(firewall_body.get("name")))
    except Exception as e:
        cli_logger.error(
            "Failed to update firewall. {}", str(e))
        raise e


def create_or_update_firewall(config, compute, firewall_body):
    firewall_name = firewall_body.get("name")
    project_id = config["provider"]["project_id"]

    if not check_firewall_exist(config, compute, firewall_name):
        create_firewall(compute, project_id, firewall_body)
    else:
        cli_logger.print(
            "The firewall {} already exists. Will update the rules... ",
            firewall_name)
        update_firewall(compute, project_id, firewall_body)


def _get_subnetwork_ip_cidr_range(project_id, compute, subnetwork):
    subnetwork_region, subnet_name = _split_subnetwork_info(project_id, subnetwork)
    return compute.subnetworks().get(
        project=project_id,
        region=subnetwork_region,
        subnetwork=subnet_name).execute().get("ipCidrRange")


def get_subnetworks_ip_cidr_range(config, compute, vpc_id):
    provider_config = get_provider_config(config)
    project_id = provider_config["project_id"]
    subnetworks = compute.networks().get(
        project=project_id,
        network=vpc_id).execute().get("subnetworks")
    subnetwork_cidrs = []
    for subnetwork in subnetworks:
        # proxy CIDR range already allowed
        subnetwork_cidrs.append(
            _get_subnetwork_ip_cidr_range(project_id, compute, subnetwork))
    return subnetwork_cidrs


def get_working_node_ip_cidr_range(config, compute):
    provider_config = get_provider_config(config)
    project_id = provider_config["project_id"]
    subnetwork_cidrs = []
    subnetwork = _find_working_node_subnetwork(provider_config, compute)
    if subnetwork is not None:
        subnetwork_cidrs.append(
            _get_subnetwork_ip_cidr_range(project_id, compute, subnetwork))
    return subnetwork_cidrs


def _create_default_allow_internal_firewall(config, compute, vpc_id):
    project_id = config["provider"]["project_id"]
    workspace_name = get_workspace_name(config)
    subnetwork_cidrs = get_subnetworks_ip_cidr_range(config, compute, vpc_id)
    firewall_name = GCP_WORKSPACE_DEFAULT_INTERNAL_FIREWALL_NAME.format(
        workspace_name)
    firewall_body = {
        "name": firewall_name,
        "network": get_network_url(project_id, vpc_id),
        "allowed": [
            {
                "IPProtocol": "tcp",
                "ports": ["0-65535"]
            },
            {
                "IPProtocol": "udp",
                "ports": ["0-65535"]
            },
            {
                "IPProtocol": "icmp"
            }
        ],
        "sourceRanges": subnetwork_cidrs
    }

    create_or_update_firewall(config, compute, firewall_body)


def _create_or_update_load_balancer_health_check_firewall(
        config, compute, vpc_id):
    project_id = config["provider"]["project_id"]
    workspace_name = get_workspace_name(config)
    firewall_name = GCP_WORKSPACE_LOAD_BALANCER_HEALTH_CHECK_FIREWALL_NAME.format(
        workspace_name)
    firewall_body = {
        "name": firewall_name,
        "network": get_network_url(project_id, vpc_id),
        "allowed": [
            {
                "IPProtocol": "tcp",
                "ports": ["0-65535"]
            },
            {
                "IPProtocol": "udp",
                "ports": ["0-65535"]
            }
        ],
        "sourceRanges": ["35.191.0.0/16", "130.211.0.0/22"]
    }
    create_or_update_firewall(config, compute, firewall_body)


def _get_allow_working_node_firewall_rules(config, compute):
    firewall_rules = []
    subnetwork_cidrs = get_working_node_ip_cidr_range(config, compute)
    if len(subnetwork_cidrs) == 0:
        return firewall_rules

    firewall_rule = {
        "allowed": [
            {
                "IPProtocol": "tcp",
                "ports": [
                    "22" if is_peering_firewall_allow_ssh_only(config) else "0-65535"
                ]
            }
        ],
        "sourceRanges": subnetwork_cidrs
    }

    firewall_rules.append(firewall_rule)
    return firewall_rules


def _create_or_update_custom_firewalls(config, compute, vpc_id):
    firewall_rules = config["provider"].get(
        "firewalls", {}).get("firewall_rules", [])

    if is_use_peering_vpc(config) and is_peering_firewall_allow_working_subnet(config):
        firewall_rules += _get_allow_working_node_firewall_rules(config, compute)

    project_id = config["provider"]["project_id"]
    workspace_name = get_workspace_name(config)
    for i in range(len(firewall_rules)):
        firewall_body = {
            "name": GCP_WORKSPACE_CUSTOM_FIREWALL_NAME.format(workspace_name, i),
            "network": get_network_url(project_id, vpc_id),
            "allowed": firewall_rules[i]["allowed"],
            "sourceRanges": firewall_rules[i]["sourceRanges"]
        }
        create_or_update_firewall(config, compute, firewall_body)


def _create_or_update_firewalls(config, compute, vpc_id):
    current_step = 1
    total_steps = 3

    with cli_logger.group(
            "Creating or updating internal firewall",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_default_allow_internal_firewall(config, compute, vpc_id)

    with cli_logger.group(
            "Creating or updating load balancer health check firewall",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_or_update_load_balancer_health_check_firewall(
            config, compute, vpc_id)

    with cli_logger.group(
            "Creating or updating custom firewalls",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_or_update_custom_firewalls(config, compute, vpc_id)


def check_workspace_firewalls(config, compute):
    workspace_name = get_workspace_name(config)
    firewall_names = [
        GCP_WORKSPACE_DEFAULT_INTERNAL_FIREWALL_NAME.format(
            workspace_name),
        GCP_WORKSPACE_LOAD_BALANCER_HEALTH_CHECK_FIREWALL_NAME.format(
            workspace_name),
    ]

    for firewall_name in firewall_names:
        if not check_firewall_exist(config, compute, firewall_name):
            return False

    return True


def delete_firewall(compute, project_id, firewall_name):
    cli_logger.print(
        "Deleting the firewall {}... ".format(firewall_name))
    try:
        operation = compute.firewalls().delete(
            project=project_id, firewall=firewall_name).execute()
        wait_for_compute_global_operation(project_id, operation, compute)
        cli_logger.print(
            "Successfully delete the firewall {}.".format(firewall_name))
    except Exception as e:
        cli_logger.error(
            "Failed to delete the firewall {}. {}", firewall_name, str(e))
        raise e


def _delete_firewalls(config, compute):
    project_id = config["provider"]["project_id"]
    workspace_name = get_workspace_name(config)
    workspace_firewalls = [
        firewall.get("name")
        for firewall in compute.firewalls().list(
            project=project_id).execute().get("items")
        if "{}-{}".format(
            GCP_RESOURCE_NAME_PREFIX, workspace_name) in firewall.get("name")]

    total_steps = len(workspace_firewalls)
    if total_steps == 0:
        cli_logger.print(
            "No firewall exists for workspace. Skip deletion.")
        return

    for i, workspace_firewall in enumerate(workspace_firewalls):
        with cli_logger.group(
                "Deleting firewall",
                _numbered=("()", i + 1, total_steps)):
            delete_firewall(compute, project_id, workspace_firewall)


def get_gcp_vpc_id(config, compute, use_working_vpc):
    return _get_gcp_vpc_id(
        config["provider"], config.get("workspace_name"),
        compute, use_working_vpc)


def _get_gcp_vpc_id(
        provider_config, workspace_name, compute, use_working_vpc):
    if use_working_vpc:
        vpc_id = _get_working_node_vpc_id(provider_config, compute)
    else:
        vpc_id = _get_workspace_vpc_id(
            provider_config, workspace_name, compute)
    return vpc_id


def get_gcp_vpc_name(provider_config, workspace_name):
    compute = construct_compute_client(provider_config)
    use_working_vpc = _is_use_working_vpc(provider_config)
    return _get_gcp_vpc_name(
        provider_config, workspace_name, compute, use_working_vpc)


def _get_gcp_vpc_name(
        provider_config, workspace_name, compute, use_working_vpc):
    if use_working_vpc:
        vpc_name = _get_working_node_vpc_name(provider_config, compute)
    else:
        vpc_name = _get_workspace_vpc_name(workspace_name)
    return vpc_name


def update_gcp_workspace(
        config,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False):
    workspace_name = get_workspace_name(config)
    managed_cloud_storage = is_managed_cloud_storage(config)
    managed_cloud_database = is_managed_cloud_database(config)

    current_step = 1
    total_steps = GCP_WORKSPACE_NUM_UPDATE_STEPS
    if managed_cloud_storage or delete_managed_storage:
        total_steps += 1
    if managed_cloud_database or delete_managed_database:
        total_steps += 1

    try:
        with cli_logger.group(
                "Updating workspace: {}", workspace_name):
            with cli_logger.group(
                    "Updating network resources",
                    _numbered=("[]", current_step, total_steps)):
                current_step += 1
                update_network_resources(config)

            if managed_cloud_storage:
                with cli_logger.group(
                        "Creating managed cloud storage...",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    _create_workspace_cloud_storage(config)
            else:
                if delete_managed_storage:
                    with cli_logger.group(
                            "Deleting managed cloud storage",
                            _numbered=("[]", current_step, total_steps)):
                        current_step += 1
                        _delete_workspace_cloud_storage(config)

            if managed_cloud_database:
                with cli_logger.group(
                        "Creating managed database",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    _create_workspace_cloud_database(config)
            else:
                if delete_managed_database:
                    with cli_logger.group(
                            "Deleting managed database",
                            _numbered=("[]", current_step, total_steps)):
                        current_step += 1
                        _delete_workspace_cloud_database(
                            config, delete_for_update=True)
    except Exception as e:
        cli_logger.error(
            "Failed to update workspace with the name {}. "
            "You need to delete and try create again. {}",
            workspace_name, str(e))
        raise e

    cli_logger.success(
        "Successfully updated workspace: {}.",
        cf.bold(workspace_name))


def update_network_resources(config):
    workspace_name = get_workspace_name(config)
    crm, iam, compute, tpu = \
        construct_clients_from_provider_config(config["provider"])
    use_working_vpc = is_use_working_vpc(config)
    vpc_id = get_gcp_vpc_id(config, compute, use_working_vpc)
    if vpc_id is None:
        raise RuntimeError(
            "The workspace: {} doesn't exist.".format(workspace_name))

    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Updating proxy subnets",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_or_update_proxy_subnets(config, compute, vpc_id)

    with cli_logger.group(
            "Updating workspace firewalls",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        update_workspace_firewalls(config, compute, vpc_id)


def update_workspace_firewalls(config, compute, vpc_id):
    workspace_name = get_workspace_name(config)

    try:
        cli_logger.print(
            "Updating the firewalls of workspace...")
        _create_or_update_firewalls(config, compute, vpc_id)
    except Exception as e:
        cli_logger.error(
            "Failed to update the firewalls of workspace {}. {}",
            workspace_name, str(e))
        raise e

    cli_logger.print(
        "Successfully updated the firewalls of workspace: {}.",
        cf.bold(workspace_name))


def delete_gcp_workspace(
        config,
        delete_managed_storage: bool = False,
        delete_managed_database: bool = False):
    crm, iam, compute, tpu = construct_clients_from_provider_config(
        config["provider"])

    workspace_name = get_workspace_name(config)
    managed_cloud_storage = is_managed_cloud_storage(config)
    managed_cloud_database = is_managed_cloud_database(config)
    use_working_vpc = is_use_working_vpc(config)
    use_peering_vpc = is_use_peering_vpc(config)
    vpc_id = get_gcp_vpc_id(config, compute, use_working_vpc)

    current_step = 1
    total_steps = GCP_WORKSPACE_NUM_DELETION_STEPS
    if vpc_id is None:
        total_steps = 1
    else:
        if use_peering_vpc:
            total_steps += 1
    if managed_cloud_storage and delete_managed_storage:
        total_steps += 1
    if managed_cloud_database and delete_managed_database:
        total_steps += 1

    try:
        with cli_logger.group(
                "Deleting workspace: {}", workspace_name):
            # Delete in a reverse way of creating
            if managed_cloud_storage and delete_managed_storage:
                with cli_logger.group(
                        "Deleting GCS bucket",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    _delete_workspace_cloud_storage(config)

            if managed_cloud_database and delete_managed_database:
                with cli_logger.group(
                        "Deleting managed database",
                        _numbered=("[]", current_step, total_steps)):
                    current_step += 1
                    _delete_workspace_cloud_database(config)

            with cli_logger.group(
                    "Deleting service accounts",
                    _numbered=("[]", current_step, total_steps)):
                current_step += 1
                _delete_workspace_service_accounts(config, iam)

            if vpc_id:
                _delete_network_resources(config, compute, current_step, total_steps)

    except Exception as e:
        cli_logger.error(
            "Failed to delete workspace {}. {}",
            workspace_name, str(e))
        raise e

    cli_logger.success(
            "Successfully deleted workspace: {}.",
            cf.bold(workspace_name))


def _delete_workspace_service_accounts(config, iam):
    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Deleting service account for head",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _delete_head_service_account(config, iam)

    with cli_logger.group(
            "Deleting service account for worker",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _delete_worker_service_account(config, iam)


def _delete_head_service_account(config, iam):
    workspace_name = get_workspace_name(config)
    head_service_account_id = GCP_HEAD_SERVICE_ACCOUNT_ID.format(workspace_name)
    _delete_service_account(config["provider"], head_service_account_id, iam)


def _delete_worker_service_account(config, iam):
    workspace_name = get_workspace_name(config)
    worker_service_account_id = GCP_WORKER_SERVICE_ACCOUNT_ID.format(workspace_name)
    _delete_service_account(config["provider"], worker_service_account_id, iam)


def _delete_service_account(cloud_provider, service_account_id, iam):
    project_id = cloud_provider["project_id"]
    email = get_service_account_email(
        account_id=service_account_id,
        project_id=project_id)
    service_account = _get_service_account(cloud_provider, email, iam)
    if service_account is None:
        cli_logger.print(
            "No service account with id {} found.".format(service_account_id))
        return

    try:
        cli_logger.print(
            "Deleting service account: {}...".format(service_account_id))
        full_name = get_service_account_resource_name(
            project_id=project_id, account=email)
        iam.projects().serviceAccounts().delete(name=full_name).execute()
        cli_logger.print(
            "Successfully deleted the service account.")
    except Exception as e:
        cli_logger.error(
            "Failed to delete the service account. {}", str(e))
        raise e


def _delete_workspace_cloud_storage(config):
    _delete_managed_cloud_storage(config["provider"], config["workspace_name"])


def _delete_managed_cloud_storage(
        cloud_provider, workspace_name,
        object_storage_name=None):
    region = cloud_provider["region"]
    if not object_storage_name:
        object_storage_name = get_default_workspace_object_storage_name(
            workspace_name, region)

    bucket = get_managed_gcs_bucket(
        cloud_provider, workspace_name,
        object_storage_name=object_storage_name)
    if bucket is None:
        cli_logger.print(
            "No GCS bucket with the name {} found. Skip Deletion.",
            object_storage_name)
        return

    try:
        cli_logger.print(
            "Deleting GCS bucket: {}...".format(bucket.name))
        bucket.delete(force=True)
        cli_logger.print(
            "Successfully deleted GCS bucket.")
    except Exception as e:
        cli_logger.error(
            "Failed to delete GCS bucket. {}", str(e))
        raise e


def _delete_workspace_cloud_database(
        config, delete_for_update: bool = False):
    provider_config = get_provider_config(config)
    workspace_name = get_workspace_name(config)
    vpc_name = get_gcp_vpc_name(
        provider_config, workspace_name)

    _delete_managed_cloud_database(
        provider_config, workspace_name, vpc_name,
        delete_for_update)


def _delete_managed_cloud_database(
        provider_config, workspace_name, vpc_name,
        delete_for_update: bool = False):
    current_step = 1
    total_steps = 3

    with cli_logger.group(
            "Deleting managed database instances",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _delete_managed_database_instances(provider_config, workspace_name)

    private_connection_deleted = False
    with cli_logger.group(
            "Deleting private connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        try:
            _delete_private_connection(
                provider_config, workspace_name,
                vpc_name)
            private_connection_deleted = True
        except Exception as e:
            # skip the error for update delete
            if delete_for_update:
                cli_logger.warning(
                    "Failed to delete the global address. It might be in use. Please retry later.")
            else:
                raise e

    with cli_logger.group(
            "Deleting global address",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        if not private_connection_deleted:
            cli_logger.warning(
                "Skip deletion of the global address: in use.")
        else:
            try:
                _delete_global_address(provider_config, workspace_name)
            except Exception as e:
                # skip the error for update delete
                if delete_for_update:
                    cli_logger.warning(
                        "Failed to delete the global address. It might be in use. Please retry later.")
                else:
                    raise e


def _delete_global_address(provider_config, workspace_name):
    global_address = get_global_address(provider_config, workspace_name)
    if global_address is None:
        cli_logger.print(
            "No global address was found for workspace. Skip deletion.")
        return

    compute = construct_compute_client(provider_config)
    project_id = provider_config["project_id"]
    global_address_name = GCP_WORKSPACE_DATABASE_GLOBAL_ADDRESS_NAME.format(workspace_name)
    try:
        cli_logger.print(
            "Deleting global address: {}...".format(global_address_name))
        operation = compute.globalAddresses().delete(
            project=project_id, address=global_address_name).execute()
        result = wait_for_compute_global_operation(project_id, operation, compute)
        if "status" in result and result["status"] == "DONE":
            cli_logger.print(
                "Successfully deleted global address: {}.",
                global_address_name)
        else:
            raise RuntimeError(
                "Timeout for deleting global address.")
    except Exception as e:
        cli_logger.error(
            "Failed to delete global address. {}", str(e))
        raise e


def _delete_private_connection(
        provider_config, workspace_name, vpc_name):
    private_connection = get_private_connection(
        provider_config, workspace_name, vpc_name)
    if private_connection is None:
        cli_logger.print(
            "No private connection was found for network. Skip deletion.")
        return

    service_networking = construct_service_networking(provider_config)

    network = private_connection["network"]
    vpc_peering_name = private_connection["peering"]
    name = "services/{}/connections/{}".format(
        GCP_SERVICE_NETWORKING_NAME, vpc_peering_name
    )
    delete_body = {
        "consumerNetwork": network
    }

    cli_logger.print(
        "Deleting private connection for network: {}...".format(vpc_name))
    try:
        operation = service_networking.services().connections().deleteConnection(
            name=name, body=delete_body).execute()
        result = wait_for_service_networking_operation(
            operation, service_networking)
        if "done" in result and result["done"]:
            cli_logger.print(
                "Successfully deleted private connection for network: {}.",
                vpc_name)
        else:
            raise RuntimeError(
                "Timeout for deleting private connection for network.")
    except Exception as e:
        cli_logger.error(
            "Failed to delete private connection. {}", str(e))
        raise e


def _delete_managed_database_instance(
        provider_config, workspace_name, db_instance_name=None):
    if not db_instance_name:
        # if not specified, workspace default database
        db_instance_name = get_default_workspace_database_name(workspace_name)
    db_instance = get_managed_database_instance(
        provider_config, workspace_name,
        db_instance_name=db_instance_name)
    if db_instance is None:
        cli_logger.print(
            "No managed database instance {} was found in workspace. Skip deletion.",
            db_instance_name)
        return

    _delete_database_instance(provider_config, db_instance)


def _delete_database_instance(provider_config, db_instance):
    sql_admin = construct_sql_admin(provider_config)
    project_id = provider_config["project_id"]
    try:
        db_instance_name = db_instance["name"]
        cli_logger.print(
            "Deleting database instance: {}...".format(db_instance_name))
        operation = sql_admin.instances().delete(
            project=project_id, instance=db_instance_name).execute()
        result = wait_for_sql_admin_operation(project_id, operation, sql_admin)
        if result["status"] == "DONE":
            cli_logger.print(
                "Successfully deleted database instance: {}.", db_instance_name)
        else:
            raise RuntimeError(
                "Timeout for deleting database instance.")

    except Exception as e:
        cli_logger.error(
            "Failed to delete database instance. {}", str(e))
        raise e


def _delete_managed_database_instances(provider_config, workspace_name):
    database_instances = get_managed_database_instances(
        provider_config, workspace_name)
    if database_instances is None:
        cli_logger.print(
            "No managed database instances found in workspace {}. Skip deletion.",
            workspace_name)
        return

    total = len(database_instances)
    for i, database_instance in enumerate(database_instances):
        with cli_logger.group(
                "Deleting database instance: {}",
                database_instance["name"],
                _numbered=("()", i + 1, total)):
            _delete_database_instance(
                provider_config, database_instance)


def _delete_network_resources(config, compute, current_step, total_steps):
    """
         Do the work - order of operation:
         Delete VPC peering connection if needed
         Delete proxy subnets
         Delete public subnet
         Delete router for private subnet
         Delete private subnets
         Delete firewalls
         Delete vpc
    """
    use_peering_vpc = is_use_peering_vpc(config)

    # delete vpc peering connection
    if use_peering_vpc:
        with cli_logger.group(
                "Deleting VPC peering connection",
                _numbered=("[]", current_step, total_steps)):
            current_step += 1
            _delete_vpc_peering_connections(config, compute)

    with cli_logger.group(
            "Deleting proxy subnets",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_proxy_subnets(config, compute)

    # delete public subnets
    with cli_logger.group(
            "Deleting public subnet",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_subnet(config, compute, is_private=False)

    # delete router for private subnets
    with cli_logger.group(
            "Deleting router",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_router(config, compute)

    # delete private subnets
    with cli_logger.group(
            "Deleting private subnet",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_subnet(config, compute, is_private=True)

    # delete firewalls
    with cli_logger.group(
            "Deleting firewall rules",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_firewalls(config, compute)

    # delete vpc
    with cli_logger.group(
            "Deleting VPC",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _delete_vpc(config, compute)


def _create_vpc(config, compute):
    workspace_name = get_workspace_name(config)
    use_working_vpc = is_use_working_vpc(config)
    vpc_id = None
    if use_working_vpc:
        # No need to create new vpc
        vpc_id = get_working_node_vpc_id(config, compute)
        if vpc_id is None:
            cli_logger.abort(
                "Failed to get the VPC for the current machine. "
                "Please make sure your current machine is an GCP virtual machine "
                "to use use_internal_ips=True with use_working_vpc=True.")
    else:
        # Need to create a new vpc
        if get_workspace_vpc_id(config, compute) is None:
            create_vpc(config, compute)
            vpc_id = get_workspace_vpc_id(config, compute)
        else:
            cli_logger.abort(
                "There is a existing VPC with the same name: {}, "
                "if you want to create a new workspace with the same name, "
                "you need to execute workspace delete first.".format(workspace_name))
    return vpc_id


def _create_head_service_account(config, crm, iam):
    workspace_name = get_workspace_name(config)
    service_account_id = GCP_HEAD_SERVICE_ACCOUNT_ID.format(workspace_name)
    cli_logger.print(
        "Creating head service account: {}...".format(service_account_id))

    try:
        service_account_config = {
            "displayName": GCP_HEAD_SERVICE_ACCOUNT_DISPLAY_NAME.format(workspace_name),
        }

        service_account = _create_service_account(
            config["provider"], service_account_id, service_account_config,
            iam)

        assert service_account is not None, "Failed to create head service account."

        if config["provider"].get(HAS_TPU_PROVIDER_FIELD, False):
            roles = HEAD_SERVICE_ACCOUNT_ROLES + TPU_SERVICE_ACCOUNT_ROLES
        else:
            roles = HEAD_SERVICE_ACCOUNT_ROLES

        _add_iam_role_binding_for_service_account(
            service_account, roles, crm)
        cli_logger.print(
            "Successfully created head service account and configured with roles.")
    except Exception as e:
        cli_logger.error(
            "Failed to create head service account. {}", str(e))
        raise e


def _create_worker_service_account(config, crm, iam):
    workspace_name = get_workspace_name(config)
    service_account_id = GCP_WORKER_SERVICE_ACCOUNT_ID.format(workspace_name)
    cli_logger.print(
        "Creating worker service account: {}...".format(service_account_id))

    try:
        service_account_config = {
            "displayName": GCP_WORKER_SERVICE_ACCOUNT_DISPLAY_NAME.format(
                workspace_name),
        }
        service_account = _create_service_account(
            config["provider"], service_account_id, service_account_config,
            iam)

        assert service_account is not None, "Failed to create worker service account."

        _add_iam_role_binding_for_service_account(
            service_account, WORKER_SERVICE_ACCOUNT_ROLES, crm)
        cli_logger.print(
            "Successfully created worker service account and configured with roles.")
    except Exception as e:
        cli_logger.error(
            "Failed to create worker service account. {}", str(e))
        raise e


def _create_workspace_service_accounts(config, crm, iam):
    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Creating service account for head",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_head_service_account(config, crm, iam)

    with cli_logger.group(
            "Creating service account for worker",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_worker_service_account(config, crm, iam)

    return config


def get_default_workspace_object_storage_name(
        workspace_name, region):
    bucket_name = "{prefix}-{workspace_name}-{region}-{suffix}".format(
        prefix=GCP_RESOURCE_NAME_PREFIX,
        workspace_name=workspace_name,
        region=region,
        suffix="default"
    )
    return bucket_name


def _create_workspace_cloud_storage(config):
    _create_managed_cloud_storage(config["provider"], config["workspace_name"])
    return config


def _create_managed_cloud_storage(
        cloud_provider, workspace_name,
        object_storage_name=None):
    region = cloud_provider["region"]
    if not object_storage_name:
        object_storage_name = get_default_workspace_object_storage_name(
            workspace_name, region)

    # If the managed cloud storage for the workspace already exists
    # Skip the creation step
    bucket = get_managed_gcs_bucket(
        cloud_provider, workspace_name,
        object_storage_name=object_storage_name)
    if bucket is not None:
        cli_logger.print(
            "GCS bucket {} already exists. Skip creation.",
            object_storage_name)
        return

    storage_client = construct_storage_client(cloud_provider)

    cli_logger.print(
        "Creating GCS bucket for the workspace: {}".format(workspace_name))
    try:
        bucket = storage_client.create_bucket(
            bucket_or_name=object_storage_name, location=region)

        # Bet bucket labels
        labels = bucket.labels if bucket.labels is not None else {}
        labels[CLOUDTIK_TAG_WORKSPACE_NAME] = workspace_name
        bucket.labels = labels
        bucket.patch()

        cli_logger.print(
            "Successfully created GCS bucket: {}.",
            object_storage_name)
    except Exception as e:
        cli_logger.error(
            "Failed to create GCS bucket. {}", str(e))
        raise e


def _create_workspace_cloud_database(config):
    provider_config = get_provider_config(config)
    workspace_name = get_workspace_name(config)
    vpc_name = get_gcp_vpc_name(
        provider_config, workspace_name)
    _create_managed_cloud_database(
        provider_config, workspace_name,
        vpc_name)


def _create_managed_cloud_database(
        provider_config, workspace_name, vpc_name,
        db_instance_name=None):
    current_step = 1
    total_steps = 3

    with cli_logger.group(
            "Creating global address",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_global_address(
            provider_config, workspace_name, vpc_name)

    with cli_logger.group(
            "Creating private connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_private_connection(
            provider_config, workspace_name, vpc_name)

    with cli_logger.group(
            "Creating managed database instance",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_managed_database_instance(
            provider_config, workspace_name, vpc_name,
            db_instance_name=db_instance_name)


def _create_global_address(
        provider_config, workspace_name, vpc_name):
    global_address = get_global_address(provider_config, workspace_name)
    if global_address is not None:
        cli_logger.print(
            "Global global address for database already exists. Skip creation.")
        return

    compute = construct_compute_client(provider_config)

    global_address_name = GCP_WORKSPACE_DATABASE_GLOBAL_ADDRESS_NAME.format(
        workspace_name)
    project_id = provider_config.get("project_id")
    network = get_network_url(project_id, vpc_name)
    create_body = {
        "name": global_address_name,
        "purpose": "VPC_PEERING",
        "addressType": "INTERNAL",
        "prefixLength": 16,
        "network": network,
    }

    cli_logger.print(
        "Creating global address for the database: {}...".format(global_address_name))
    try:
        operation = compute.globalAddresses().insert(
            project=project_id, body=create_body).execute()
        result = wait_for_compute_global_operation(
            project_id, operation, compute)
        if "status" in result and result["status"] == "DONE":
            cli_logger.print(
                "Successfully created global address: {}.",
                global_address_name)
        else:
            raise RuntimeError("Error timeout.")
    except Exception as e:
        cli_logger.error(
            "Failed to create global address. {}", str(e))
        raise e


def _create_private_connection(provider_config, workspace_name, vpc_name):
    private_connection = get_private_connection(
        provider_config, workspace_name, vpc_name)
    if private_connection is not None:
        cli_logger.print(
            "Private connection for network already exists. Skip creation.")
        return

    service_networking = construct_service_networking(provider_config)

    project_id = provider_config.get("project_id")
    network = get_network_url(project_id, vpc_name)
    service_name = "services/{}".format(GCP_SERVICE_NETWORKING_NAME)
    global_address_name = GCP_WORKSPACE_DATABASE_GLOBAL_ADDRESS_NAME.format(workspace_name)
    create_body = {
        "network": network,
        "reservedPeeringRanges": [global_address_name]
    }

    cli_logger.print(
        "Creating private connection for network: {}...".format(vpc_name))
    try:
        operation = service_networking.services().connections().create(
            parent=service_name, body=create_body).execute()
        result = wait_for_service_networking_operation(operation, service_networking)
        if "done" in result and result["done"]:
            cli_logger.print(
                "Successfully created private connection for network: {}.",
                vpc_name)
        else:
            raise RuntimeError("Error timeout.")
    except Exception as e:
        cli_logger.error(
            "Failed to create private connection. {}", str(e))
        raise e


def _create_managed_database_instance_in_workspace(
        provider_config, workspace_name,
        db_instance_name=None):
    vpc_name = get_gcp_vpc_name(
        provider_config, workspace_name)
    _create_managed_database_instance(
        provider_config, workspace_name, vpc_name,
        db_instance_name=db_instance_name)


def _create_managed_database_instance(
        provider_config, workspace_name, vpc_name,
        db_instance_name=None):
    if not db_instance_name:
        # if not specified, workspace default database
        db_instance_name = get_default_workspace_database_name(workspace_name)
    # If the managed cloud database for the workspace already exists
    # Skip the creation step
    db_instance = get_managed_database_instance(
        provider_config, workspace_name,
        db_instance_name=db_instance_name)
    if db_instance is not None:
        cli_logger.print(
            "Managed database instance {} already exists in the workspace. Skip creation.",
            db_instance_name)
        return

    project_id = provider_config.get("project_id")
    network = get_network_url(project_id, vpc_name)

    sql_admin = construct_sql_admin(provider_config)
    region = provider_config["region"]
    project_id = provider_config.get("project_id")

    database_config = get_gcp_database_config(provider_config, {})
    engine = get_gcp_database_engine(database_config)
    database_version = "MYSQL_8_0" if engine == "mysql" else "POSTGRES_14"

    create_body = {
        "name": db_instance_name,
        "region": region,
        "databaseVersion": database_version,
        "rootPassword": database_config.get('password', "cloudtik"),
        "settings": {
            "tier": database_config.get("instance_type", "db-custom-4-15360"),
            "dataDiskType": database_config.get("storage_type", "PD_SSD"),
            "dataDiskSizeGb": str(database_config.get("storage_size", 50)),
            "ipConfiguration": {
                "ipv4Enabled": False,
                "privateNetwork": network,
                "enablePrivatePathForGoogleCloudServices": True
            },
            "userLabels": {
                CLOUDTIK_TAG_WORKSPACE_NAME: workspace_name
            }
        }
    }

    if database_config.get("high_availability", False):
        recovery = "binaryLogEnabled" if engine == "mysql" else "pointInTimeRecoveryEnabled"
        settings = create_body["settings"]
        settings["backupConfiguration"] = {
            "enabled": True,
            recovery: True
        },
        settings["availabilityType"] = "REGIONAL"

    cli_logger.print(
        "Creating database instance for the workspace: {}...".format(workspace_name))
    try:
        operation = sql_admin.instances().insert(
            project=project_id, body=create_body).execute()
        result = wait_for_sql_admin_operation(project_id, operation, sql_admin)
        if result["status"] == "DONE":
            cli_logger.print(
                "Successfully created database instance: {}.".format(db_instance_name))
        else:
            raise RuntimeError(
                "Timeout for creating database instance.")
    except Exception as e:
        cli_logger.error(
            "Failed to create database instance. {}", str(e))
        raise e


def _create_network_resources(config, current_step, total_steps):
    crm, iam, compute, tpu = construct_clients_from_provider_config(
        config["provider"])

    # create vpc
    with cli_logger.group(
            "Creating VPC",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        vpc_id = _create_vpc(config, compute)

    # create subnets
    with cli_logger.group(
            "Creating subnets",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_and_configure_subnets(config, compute, vpc_id)

    # create router
    with cli_logger.group(
            "Creating router",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_router(config, compute, vpc_id)

    # create NAT for router
    with cli_logger.group(
            "Creating NAT for router",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_nat_for_router(config, compute)

    with cli_logger.group(
            "Creating proxy subnets",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_or_update_proxy_subnets(config, compute, vpc_id)

    # create firewalls
    with cli_logger.group(
            "Creating firewall rules",
            _numbered=("[]", current_step, total_steps)):
        current_step += 1
        _create_or_update_firewalls(config, compute, vpc_id)

    if is_use_peering_vpc(config):
        with cli_logger.group(
                "Creating VPC peering connection",
                _numbered=("[]", current_step, total_steps)):
            current_step += 1
            _create_vpc_peering_connections(config, compute, vpc_id)

    return current_step


def check_gcp_workspace_existence(config):
    crm, iam, compute, tpu = \
        construct_clients_from_provider_config(config["provider"])
    workspace_name = get_workspace_name(config)
    managed_cloud_storage = is_managed_cloud_storage(config)
    managed_cloud_database = is_managed_cloud_database(config)
    use_working_vpc = is_use_working_vpc(config)
    use_peering_vpc = is_use_peering_vpc(config)

    existing_resources = 0
    target_resources = GCP_WORKSPACE_TARGET_RESOURCES
    if managed_cloud_storage:
        target_resources += 1
    if managed_cloud_database:
        target_resources += 1
    if use_peering_vpc:
        target_resources += 1

    """
         Do the work - order of operation
         Check project
         Check VPC
         Check private subnet
         Check public subnet
         Check proxy subnets
         Check router
         Check firewalls
         Check VPC peering if needed
         Check GCS bucket
         Check service accounts
         Check cloud database
    """
    project_existence = False
    cloud_storage_existence = False
    cloud_database_existence = False
    if get_workspace_project(config, crm) is not None:
        existing_resources += 1
        project_existence = True
        # All resources that depending on project
        vpc_id = get_gcp_vpc_id(config, compute, use_working_vpc)
        if vpc_id is not None:
            existing_resources += 1
            # Network resources that depending on VPC
            if get_subnet(config, get_workspace_subnet_name(
                    workspace_name), compute) is not None:
                existing_resources += 1
            if get_subnet(config, get_workspace_public_subnet_name(
                    workspace_name), compute) is not None:
                existing_resources += 1
            if get_subnet(config, get_workspace_proxy_subnet_name(
                    workspace_name), compute) is not None:
                existing_resources += 1
            if get_router(config, get_workspace_router_name(
                    workspace_name), compute) is not None:
                existing_resources += 1
            if check_workspace_firewalls(config, compute):
                existing_resources += 1
            if use_peering_vpc:
                peerings = get_workspace_vpc_peering_connections(
                    config, compute, vpc_id)
                if len(peerings) == 2:
                    existing_resources += 1

        if managed_cloud_storage:
            if get_workspace_gcs_bucket(config, workspace_name) is not None:
                existing_resources += 1
                cloud_storage_existence = True

        if managed_cloud_database:
            if get_workspace_database_instance(config) is not None:
                existing_resources += 1
                cloud_database_existence = True

        if _get_workspace_service_account(
                config, iam, GCP_HEAD_SERVICE_ACCOUNT_ID) is not None:
            existing_resources += 1
        if _get_workspace_service_account(
                config, iam, GCP_WORKER_SERVICE_ACCOUNT_ID) is not None:
            existing_resources += 1

    if existing_resources == 0 or (
            existing_resources == 1 and project_existence):
        return Existence.NOT_EXIST
    elif existing_resources == target_resources:
        return Existence.COMPLETED
    else:
        skipped_resources = 1
        if existing_resources == skipped_resources + 1 and cloud_storage_existence:
            return Existence.STORAGE_ONLY
        elif existing_resources == skipped_resources + 1 and cloud_database_existence:
            return Existence.DATABASE_ONLY
        elif existing_resources == skipped_resources + 2 and cloud_storage_existence \
                and cloud_database_existence:
            return Existence.STORAGE_AND_DATABASE_ONLY
        return Existence.IN_COMPLETED


def check_gcp_workspace_integrity(config):
    existence = check_gcp_workspace_existence(config)
    return True if existence == Existence.COMPLETED else False


def get_gcp_workspace_info(config):
    managed_cloud_storage = is_managed_cloud_storage(config)
    managed_cloud_database = is_managed_cloud_database(config)

    info = {}
    if managed_cloud_storage:
        get_gcp_managed_cloud_storage_info(
            config, config["provider"], info)

    if managed_cloud_database:
        get_gcp_managed_cloud_database_info(
            config, config["provider"], info)
    return info


def get_gcp_managed_cloud_storage_info(config, cloud_provider, info):
    workspace_name = get_workspace_name(config)
    cloud_storage_info = _get_managed_cloud_storage_info(
        cloud_provider, workspace_name)
    if cloud_storage_info:
        info[CLOUDTIK_MANAGED_CLOUD_STORAGE] = cloud_storage_info


def _get_managed_cloud_storage_info(
        cloud_provider, workspace_name,
        object_storage_name=None):
    bucket = get_managed_gcs_bucket(
        cloud_provider, workspace_name,
        object_storage_name=object_storage_name)
    return _get_object_storage_info(bucket)


def _get_object_storage_info(bucket):
    managed_bucket_name = None if bucket is None else bucket.name
    if managed_bucket_name is not None:
        gcp_cloud_storage = {GCP_GCS_BUCKET: managed_bucket_name}
        managed_cloud_storage = {
            CLOUDTIK_MANAGED_CLOUD_STORAGE_NAME: managed_bucket_name,
            CLOUDTIK_MANAGED_CLOUD_STORAGE_URI: get_gcp_cloud_storage_uri(
                gcp_cloud_storage)}
        return managed_cloud_storage
    return None


def get_gcp_managed_cloud_database_info(config, cloud_provider, info):
    workspace_name = get_workspace_name(config)
    cloud_database_info = _get_managed_cloud_database_info(
        cloud_provider, workspace_name)
    if cloud_database_info:
        info[CLOUDTIK_MANAGED_CLOUD_DATABASE] = cloud_database_info


def _get_managed_cloud_database_info(
        cloud_provider, workspace_name, db_instance_name=None):
    database_instance = get_managed_database_instance(
        cloud_provider, workspace_name,
        db_instance_name=db_instance_name)
    return _get_managed_database_instance_info(database_instance)


def _get_managed_database_instance_info(database_instance):
    if database_instance is not None:
        db_address = _get_managed_database_address(database_instance)
        engine = _get_managed_database_engine(database_instance)
        if engine is None:
            return None

        managed_cloud_database_info = {
            CLOUDTIK_MANAGED_CLOUD_DATABASE_NAME: database_instance["name"],
            CLOUDTIK_MANAGED_CLOUD_DATABASE_ENDPOINT: db_address,
            CLOUDTIK_MANAGED_CLOUD_DATABASE_PORT: get_gcp_database_default_port(engine),
            CLOUDTIK_MANAGED_CLOUD_DATABASE_ENGINE: engine,
            CLOUDTIK_MANAGED_CLOUD_DATABASE_ADMIN_USER: get_gcp_database_default_admin_user(engine),
        }
        return managed_cloud_database_info
    return None


def get_subnet(config, subnet_name, compute):
    cli_logger.verbose(
        "Getting the existing subnet: {}.".format(subnet_name))
    try:
        subnet = compute.subnetworks().get(
            project=config["provider"]["project_id"],
            region=config["provider"]["region"],
            subnetwork=subnet_name,
        ).execute()
        cli_logger.verbose(
            "Successfully get the subnet: {}.".format(subnet_name))
        return subnet
    except Exception:
        cli_logger.verbose_error(
            "Failed to get the subnet: {}.".format(subnet_name))
        return None


def get_router(config, router_name, compute):
    cli_logger.verbose(
        "Getting the existing router: {}.".format(router_name))
    try:
        router = compute.routers().get(
            project=config["provider"]["project_id"],
            region=config["provider"]["region"],
            router=router_name,
        ).execute()
        cli_logger.verbose(
            "Successfully get the router: {}.".format(router_name))
        return router
    except Exception:
        cli_logger.verbose_error(
            "Failed to get the router: {}.".format(router_name))
        return None


def _get_project(project_id, crm):
    try:
        project = crm.projects().get(projectId=project_id).execute()
    except errors.HttpError as e:
        if e.resp.status != 403:
            raise
        project = None

    return project


def get_workspace_project(config, crm):
    project_id = config["provider"]["project_id"]
    return _get_project(project_id, crm)


def get_workspace_gcs_bucket(config, workspace_name):
    return get_managed_gcs_bucket(config["provider"], workspace_name)


def get_managed_gcs_bucket(
        cloud_provider, workspace_name,
        object_storage_name=None):
    region = cloud_provider["region"]
    if not object_storage_name:
        object_storage_name = get_default_workspace_object_storage_name(
            workspace_name, region)

    gcs = construct_storage_client(cloud_provider)

    project_id = cloud_provider["project_id"]

    cli_logger.verbose(
        "Getting GCS bucket: {}.".format(object_storage_name))
    for bucket in gcs.list_buckets(project=project_id):
        if bucket.name == object_storage_name:
            cli_logger.verbose(
                "Successfully get the GCS bucket: {}.".format(object_storage_name))
            return bucket

    cli_logger.verbose_error(
        "Failed to get the GCS bucket: {}.", object_storage_name)
    return None


def _is_workspace_labeled(labels, workspace_name):
    if not labels:
        return False
    value = labels.get(CLOUDTIK_TAG_WORKSPACE_NAME)
    if value == workspace_name:
        return True
    return False


def get_managed_gcs_buckets(
        cloud_provider, workspace_name):
    gcs = construct_storage_client(cloud_provider)
    project_id = cloud_provider["project_id"]

    cli_logger.verbose(
        "Getting GCS buckets of workspace: {}.".format(workspace_name))
    workspace_buckets = []
    for bucket in gcs.list_buckets(project=project_id):
        labels = bucket.labels
        if _is_workspace_labeled(labels, workspace_name):
            workspace_buckets.append(bucket)

    cli_logger.verbose(
        "Successfully get {} GCS buckets.".format(
            len(workspace_buckets)))
    return workspace_buckets


def get_workspace_database_instance(config):
    return get_managed_database_instance(
        config["provider"], config["workspace_name"])


def get_default_workspace_database_name(workspace_name):
    return GCP_WORKSPACE_DATABASE_NAME.format(workspace_name)


def get_managed_database_instance(
        provider_config, workspace_name, db_instance_name=None):
    if not db_instance_name:
        # if not specified, workspace default database
        db_instance_name = get_default_workspace_database_name(workspace_name)
    sql_admin = construct_sql_admin(provider_config)
    project_id = provider_config["project_id"]

    cli_logger.verbose(
        "Getting the database instance: {}.".format(db_instance_name))
    try:
        db_instance = sql_admin.instances().get(
            project=project_id, instance=db_instance_name).execute()
        cli_logger.verbose(
            "Successfully get database instance: {}.".format(db_instance_name))
        return db_instance
    except Exception as e:
        cli_logger.verbose_error(
            "Failed to get database instance. {}", str(e))
        return None


def get_managed_database_instances(
        provider_config, workspace_name):
    sql_admin = construct_sql_admin(provider_config)
    project_id = provider_config["project_id"]

    cli_logger.verbose(
        "Getting the database instances...")
    try:
        filter_expr = "settings.userLabels.{}:{}".format(
            CLOUDTIK_TAG_WORKSPACE_NAME, workspace_name)
        response = sql_admin.instances().list(
            project=project_id, filter=filter_expr).execute()
        db_instances = response.get("items", [])
        cli_logger.verbose(
            "Successfully get {} database instances.", len(db_instances))
        return db_instances
    except Exception as e:
        cli_logger.verbose_error(
            "Failed to get database instances. {}", str(e))
        return None


def get_private_connection(provider_config, workspace_name, vpc_name):
    service_networking = construct_service_networking(provider_config)
    project_id = provider_config.get("project_id")
    network = get_network_url(project_id, vpc_name)
    service_name = "services/{}".format(GCP_SERVICE_NETWORKING_NAME)
    cli_logger.verbose(
        "Getting the private connection for network: {}.".format(vpc_name))
    try:
        list_response = service_networking.services().connections().list(
            parent=service_name, network=network).execute()
        cli_logger.verbose(
            "Successfully get private connection: {}.".format(vpc_name))
        if "connections" in list_response and len(list_response["connections"]) > 0:
            return list_response["connections"][0]
    except Exception as e:
        cli_logger.verbose_error(
            "Failed to get private connection. {}", str(e))
        return None

    cli_logger.verbose(
        "No private connection found.")
    return None


def get_global_address(provider_config, workspace_name):
    compute = construct_compute_client(provider_config)
    project_id = provider_config["project_id"]
    global_address_name = GCP_WORKSPACE_DATABASE_GLOBAL_ADDRESS_NAME.format(
        workspace_name)
    cli_logger.verbose(
        "Getting the global address: {}.".format(global_address_name))
    try:
        global_address = compute.globalAddresses().get(
            project=project_id, address=global_address_name).execute()
        cli_logger.verbose(
            "Successfully get global address: {}.".format(global_address_name))
        return global_address
    except Exception as e:
        cli_logger.verbose_error(
            "Failed to get global address. {}", str(e))
        return None


def _create_project(project_id, crm):
    cli_logger.print(
        "Creating project: {}...".format(project_id))
    operation = crm.projects().create(body={
        "projectId": project_id,
        "name": project_id
    }).execute()

    result = wait_for_crm_operation(operation, crm)
    if "done" in result and result["done"]:
        cli_logger.print(
            "Successfully created project: {}.".format(project_id))

    return result


def _get_service_account_by_id(cloud_provider, account_id, iam):
    email = get_service_account_email(
        account_id=account_id,
        project_id=cloud_provider["project_id"])
    return _get_service_account(cloud_provider, email, iam)


def _get_service_account(cloud_provider, account, iam):
    project_id = cloud_provider["project_id"]
    return _get_service_account_of_project(project_id, account, iam)


def _get_service_account_of_project(project_id, account, iam):
    full_name = get_service_account_resource_name(
        project_id=project_id, account=account)
    try:
        cli_logger.verbose(
            "Getting the service account: {}...".format(account))
        service_account = iam.projects().serviceAccounts().get(
            name=full_name).execute()
        cli_logger.verbose(
            "Successfully get the service account: {}.".format(account))
    except errors.HttpError as e:
        if e.resp.status != 404:
            raise
        cli_logger.verbose(
            "The service account doesn't exist: {}...".format(account))
        service_account = None

    return service_account


def _create_service_account(cloud_provider, account_id, account_config, iam):
    project_id = cloud_provider["project_id"]
    service_account = iam.projects().serviceAccounts().create(
        name="projects/{project_id}".format(project_id=project_id),
        body={
            "accountId": account_id,
            "serviceAccount": account_config,
        }).execute()

    return service_account


def _add_iam_role_binding_for_service_account(service_account, roles, crm):
    project_id = service_account["projectId"]
    service_account_email = service_account["email"]
    return _add_iam_role_binding(
        project_id, service_account_email, roles, crm)


def _add_iam_role_binding(project_id, service_account_email, roles, crm):
    """Add new IAM roles for the service account."""
    member_id = "serviceAccount:" + service_account_email
    policy = crm.projects().getIamPolicy(
        resource=project_id, body={}).execute()

    changed = _add_role_bindings_to_policy(roles, member_id, policy)
    if not changed:
        # In some managed environments, an admin needs to grant the
        # roles, so only call setIamPolicy if needed.
        return

    result = crm.projects().setIamPolicy(
        resource=project_id, body={
            "policy": policy,
        }).execute()

    return result


def _remove_iam_role_binding(project_id, service_account_email, roles, crm):
    """Remove new IAM roles for the service account."""
    member_id = "serviceAccount:" + service_account_email
    policy = crm.projects().getIamPolicy(
        resource=project_id, body={}).execute()

    changed = _remove_role_bindings_from_policy(roles, member_id, policy)
    if not changed:
        return

    result = crm.projects().setIamPolicy(
        resource=project_id, body={
            "policy": policy,
        }).execute()
    return result


def _has_iam_role_binding(project_id, service_account_email, roles, crm):
    role_bindings = _get_iam_role_binding(
        project_id, service_account_email, roles, crm)
    if len(role_bindings) != len(roles):
        return False
    return True


def _get_iam_role_binding(project_id, service_account_email, roles, crm):
    """Get IAM roles bindings for the service account."""
    member_id = "serviceAccount:" + service_account_email
    policy = crm.projects().getIamPolicy(
        resource=project_id, body={}).execute()
    return _get_role_bindings_of_policy(roles, member_id, policy)


def get_service_account_resource_name(project_id, account):
    # 'account' can be the account id or the email
    return "projects/{project_id}/serviceAccounts/{account}".format(
           project_id=project_id, account=account)


def _add_role_bindings_to_policy(roles, member_id, policy):
    changed = False
    if "bindings" not in policy:
        bindings = []
        for role in roles:
            bindings.append({
                "members": [member_id],
                "role": role,
            })
        policy["bindings"] = bindings
        changed = True

    for role in roles:
        role_exists = False
        for binding in policy["bindings"]:
            if binding["role"] == role:
                if "members" not in binding:
                    binding["members"] = [member_id]
                    changed = True
                elif member_id not in binding["members"]:
                    binding["members"].append(member_id)
                    changed = True
                role_exists = True

        if not role_exists:
            changed = True
            policy["bindings"].append({
                "members": [member_id],
                "role": role,
            })
    return changed


def _remove_role_bindings_from_policy(roles, member_id, policy):
    changed = False
    if "bindings" not in policy:
        return changed
    for role in roles:
        for binding in policy["bindings"]:
            if binding["role"] == role:
                if "members" in binding and member_id in binding["members"]:
                    binding["members"].remove(member_id)
                    changed = True
    return changed


def _get_role_bindings_of_policy(roles, member_id, policy):
    role_bindings = []
    if "bindings" not in policy:
        return role_bindings

    for role in roles:
        for binding in policy["bindings"]:
            if binding["role"] == role:
                if "members" in binding and member_id in binding["members"]:
                    role_bindings.append({"role": role, "member": member_id})

    return role_bindings


def _check_service_account_existence(project_id, service_account_email, iam):
    sa = _get_service_account_of_project(project_id, service_account_email, iam)
    if sa is None:
        raise RuntimeError(
            "No service account found in project {}: {}".format(
                project_id, service_account_email))


def _add_service_account_iam_role_binding(
        project_id, service_account_email, roles, member_id, iam):
    """Add new IAM roles for the service account."""
    _check_service_account_existence(project_id, service_account_email, iam)
    resource = get_service_account_resource_name(
        project_id, service_account_email)
    policy = iam.projects().serviceAccounts().getIamPolicy(
        resource=resource).execute()

    changed = _add_role_bindings_to_policy(roles, member_id, policy)
    if not changed:
        # In some managed environments, an admin needs to grant the
        # roles, so only call setIamPolicy if needed.
        return

    result = iam.projects().serviceAccounts().setIamPolicy(
        resource=resource, body={
            "policy": policy,
        }).execute()

    return result


def _remove_service_account_iam_role_binding(
        project_id, service_account_email, roles, member_id, iam):
    """Remove new IAM roles for the service account."""
    _check_service_account_existence(project_id, service_account_email, iam)
    resource = get_service_account_resource_name(project_id, service_account_email)
    policy = iam.projects().serviceAccounts().getIamPolicy(
        resource=resource).execute()

    changed = _remove_role_bindings_from_policy(roles, member_id, policy)
    if not changed:
        return

    result = iam.projects().serviceAccounts().setIamPolicy(
        resource=resource, body={
            "policy": policy,
        }).execute()
    return result


def _has_service_account_iam_role_binding(
        project_id, service_account_email, roles, member_id, iam):
    sa = _get_service_account_of_project(project_id, service_account_email, iam)
    if sa is None:
        return False
    role_bindings = _get_service_account_iam_role_binding(
        project_id, service_account_email, roles, member_id, iam)
    if len(role_bindings) != len(roles):
        return False
    return True


def _get_service_account_iam_role_binding(
        project_id, service_account_email, roles, member_id, iam):
    """Get IAM roles bindings for the service account."""
    _check_service_account_existence(project_id, service_account_email, iam)
    resource = get_service_account_resource_name(project_id, service_account_email)
    policy = iam.projects().serviceAccounts().getIamPolicy(
        resource=resource).execute()
    return _get_role_bindings_of_policy(roles, member_id, policy)


def _create_project_ssh_key_pair(project, public_key, ssh_user, compute):
    """Inserts an ssh-key into project commonInstanceMetadata"""

    key_parts = public_key.split(" ")

    # Sanity checks to make sure that the generated key matches expectation
    assert len(key_parts) == 2, key_parts
    assert key_parts[0] == "ssh-rsa", key_parts

    new_ssh_meta = "{ssh_user}:ssh-rsa {key_value} {ssh_user}".format(
        ssh_user=ssh_user, key_value=key_parts[1])

    common_instance_metadata = project["commonInstanceMetadata"]
    items = common_instance_metadata.get("items", [])

    ssh_keys_i = next(
        (i for i, item in enumerate(items) if item["key"] == "ssh-keys"), None)

    if ssh_keys_i is None:
        items.append({"key": "ssh-keys", "value": new_ssh_meta})
    else:
        ssh_keys = items[ssh_keys_i]
        ssh_keys["value"] += "\n" + new_ssh_meta
        items[ssh_keys_i] = ssh_keys

    common_instance_metadata["items"] = items

    operation = compute.projects().setCommonInstanceMetadata(
        project=project["name"], body=common_instance_metadata).execute()

    wait_for_compute_global_operation(
        project["name"], operation, compute)


def get_cluster_name_from_head(head_node) -> Optional[str]:
    for key, value in head_node.get("labels", {}).items():
        if key == CLOUDTIK_TAG_CLUSTER_NAME:
            return value
    return None


def list_gcp_clusters(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    _, _, compute, tpu = \
        construct_clients_from_provider_config(config["provider"])
    head_nodes = _get_workspace_head_nodes(
        config["provider"], config["workspace_name"], compute=compute)

    clusters = {}
    for head_node in head_nodes:
        cluster_name = get_cluster_name_from_head(head_node)
        if cluster_name:
            gcp_resource = GCPCompute(
                compute, config["provider"]["project_id"],
                config["provider"]["availability_zone"], cluster_name)
            gcp_node = gcp_resource.from_instance(head_node)
            clusters[cluster_name] = _get_node_info(gcp_node)
    return clusters


def list_gcp_storages(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider_config = get_provider_config(config)
    workspace_name = get_workspace_name(config)
    return _list_gcp_storages(provider_config, workspace_name)


def _list_gcp_storages(
        cloud_provider: Dict[str, Any], workspace_name
) -> Optional[Dict[str, Any]]:
    buckets = get_managed_gcs_buckets(cloud_provider, workspace_name)
    object_storages = {}
    if buckets is None:
        return object_storages
    for bucket in buckets:
        storage_name = bucket.name
        if storage_name:
            object_storages[storage_name] = _get_object_storage_info(bucket)
    return object_storages


def list_gcp_databases(config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    provider_config = get_provider_config(config)
    workspace_name = get_workspace_name(config)
    return _list_gcp_databases(provider_config, workspace_name)


def _list_gcp_databases(
        cloud_provider: Dict[str, Any], workspace_name
) -> Optional[Dict[str, Any]]:
    database_instances = get_managed_database_instances(
        cloud_provider, workspace_name)
    cloud_databases = {}
    if database_instances is None:
        return cloud_databases
    for database_instance in database_instances:
        database_name = database_instance["name"]
        if database_name:
            cloud_databases[database_name] = _get_managed_database_instance_info(
                database_instance)
    return cloud_databases


def _create_vpc_peering_connections(config, compute, vpc_id):
    working_vpc_id = get_working_node_vpc_id(config, compute)
    if working_vpc_id is None:
        cli_logger.abort(
            "Failed to get the VPC for the current machine. "
            "Please make sure your current machine is an AWS virtual machine "
            "to use use_internal_ips=True with use_working_vpc=True.")

    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Creating workspace VPC peering connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_workspace_vpc_peering_connection(config, compute, vpc_id, working_vpc_id)

    with cli_logger.group(
            "Creating working VPC peering connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _create_working_vpc_peering_connection(config, compute, vpc_id, working_vpc_id)


def _create_vpc_peering_connection(config, compute, vpc_id, peer_name, peering_vpc_name):
    provider_config = get_provider_config(config)
    project_id = provider_config.get("project_id")
    cli_logger.print(
        "Creating VPC peering connection: {}...".format(peer_name))
    peer_network = get_network_url(project_id, peering_vpc_name)
    try:
        # Creating the VPC peering
        networks_add_peering_request_body = {
            "networkPeering": {
                "name": peer_name,
                "network": peer_network,
                "exchangeSubnetRoutes": True,
            }
        }

        operation = compute.networks().addPeering(
            project=project_id, network=vpc_id,
            body=networks_add_peering_request_body).execute()
        wait_for_compute_global_operation(project_id, operation, compute)

        cli_logger.print(
            "Successfully created VPC peering connection: {}.".format(peer_name))
    except Exception as e:
        cli_logger.error(
            "Failed to create VPC peering connection. {}", str(e))
        raise e


def _delete_vpc_peering_connection(config, compute, vpc_id, peer_name):
    provider_config = get_provider_config(config)
    project_id = provider_config.get("project_id")
    cli_logger.print(
        "Deleting VPC peering connection: {}".format(peer_name))
    try:
        networks_remove_peering_request_body = {
            "name": peer_name
        }
        operation = compute.networks().removePeering(
            project=project_id, network=vpc_id,
            body=networks_remove_peering_request_body).execute()
        wait_for_compute_global_operation(project_id, operation, compute)

        cli_logger.print(
            "Successfully deleted VPC peering connection: {}.".format(peer_name))
    except Exception as e:
        cli_logger.error(
            "Failed to delete VPC peering connection. {}", str(e))
        raise e


def _get_vpc_peering_connection(config, compute, vpc_id, peer_name):
    provider_config = get_provider_config(config)
    project_id = provider_config.get("project_id")
    vpc_info = compute.networks().get(
        project=project_id, network=vpc_id).execute()
    peerings = vpc_info.get("peerings")
    if peerings is not None:
        for peering in peerings:
            if peering["name"] == peer_name:
                return peering
    return None


def _create_workspace_vpc_peering_connection(config, compute, vpc_id, working_vpc_id):
    workspace_name = get_workspace_name(config)
    peer_name = GCP_WORKSPACE_VPC_PEERING_NAME.format(workspace_name)
    working_vpc_name = get_vpc_name_by_id(config, compute, working_vpc_id)
    _create_vpc_peering_connection(
        config, compute, vpc_id,
        peer_name=peer_name,
        peering_vpc_name=working_vpc_name
    )


def _create_working_vpc_peering_connection(config, compute, vpc_id, working_vpc_id):
    workspace_name = get_workspace_name(config)
    peer_name = GCP_WORKING_VPC_PEERING_NAME.format(workspace_name)
    workspace_vpc_name = _get_workspace_vpc_name(workspace_name)
    _create_vpc_peering_connection(
        config, compute, working_vpc_id,
        peer_name=peer_name,
        peering_vpc_name=workspace_vpc_name
    )


def _delete_vpc_peering_connections(config, compute):
    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Deleting working VPC peering connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _delete_working_vpc_peering_connection(config, compute)

    with cli_logger.group(
            "Deleting workspace VPC peering connection",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        _delete_workspace_vpc_peering_connection(config, compute)


def _delete_workspace_vpc_peering_connection(config, compute):
    workspace_name = get_workspace_name(config)
    peer_name = GCP_WORKSPACE_VPC_PEERING_NAME.format(workspace_name)
    vpc_id = get_workspace_vpc_id(config, compute)

    peering = _get_vpc_peering_connection(config, compute, vpc_id, peer_name)
    if peering is None:
        cli_logger.print(
            "The workspace peering connection {} doesn't exist. Skip deletion.",
            peer_name)
        return

    _delete_vpc_peering_connection(
        config, compute, vpc_id, peer_name
    )


def _delete_working_vpc_peering_connection(config, compute):
    workspace_name = get_workspace_name(config)
    peer_name = GCP_WORKING_VPC_PEERING_NAME.format(workspace_name)
    working_vpc_id = get_working_node_vpc_id(config, compute)

    peering = _get_vpc_peering_connection(
        config, compute, working_vpc_id, peer_name)
    if peering is None:
        cli_logger.print(
            "The workspace peering connection {} doesn't exist. Skip deletion.",
            peer_name)
        return

    _delete_vpc_peering_connection(
        config, compute, working_vpc_id, peer_name
    )


def get_workspace_vpc_peering_connections(config, compute, vpc_id):
    workspace_name = get_workspace_name(config)
    workspace_peer_name = GCP_WORKSPACE_VPC_PEERING_NAME.format(workspace_name)
    vpc_peerings = {}
    workspace_peering = _get_vpc_peering_connection(
        config, compute, vpc_id, workspace_peer_name)
    if workspace_peering:
        vpc_peerings["a"] = workspace_peering

    working_vpc_id = get_working_node_vpc_id(config, compute)
    if working_vpc_id is not None:
        working_peer_name = GCP_WORKING_VPC_PEERING_NAME.format(workspace_name)
        working_peering = _get_vpc_peering_connection(
            config, compute, working_vpc_id, working_peer_name)
        if working_peering:
            vpc_peerings["b"] = working_peering

    return vpc_peerings


def _get_managed_database_address(database_instance):
    if "ipAddresses" not in database_instance:
        return None

    ip_addresses = database_instance["ipAddresses"]
    for ip_addr in ip_addresses:
        addr_type = ip_addr.get("type")
        if "PRIVATE" == addr_type or "PRIMARY" == addr_type:
            return ip_addr["ipAddress"]

    return None


def _get_managed_database_engine(database_instance):
    database_version = database_instance.get("databaseVersion")
    if not database_version:
        return None

    database_version = database_version.lower()
    if database_version.startswith(DATABASE_ENGINE_MYSQL):
        return DATABASE_ENGINE_MYSQL
    elif database_version.startswith(DATABASE_ENGINE_POSTGRES):
        return DATABASE_ENGINE_POSTGRES

    # Unknown
    return None


def _get_workspace_service_account(config, iam, service_account_id_template):
    workspace_name = get_workspace_name(config)
    service_account_id = service_account_id_template.format(workspace_name)
    email = get_service_account_email(
        account_id=service_account_id,
        project_id=config["provider"]["project_id"])
    service_account = _get_service_account(config["provider"], email, iam)
    return service_account


######################
# Clustering functions
######################


def key_pair_name(i, region, project_id, ssh_user):
    """Returns the ith default gcp_key_pair_name."""
    key_name = "{}_gcp_{}_{}_{}_{}".format(
        GCP_RESOURCE_NAME_PREFIX, region, project_id, ssh_user, i)
    return key_name


def key_pair_paths(key_name):
    """Returns public and private key paths for a given key_name."""
    public_key_path = os.path.expanduser("~/.ssh/{}.pub".format(key_name))
    private_key_path = os.path.expanduser("~/.ssh/{}.pem".format(key_name))
    return public_key_path, private_key_path


def generate_rsa_key_pair():
    """Create public and private ssh-keys."""

    key = rsa.generate_private_key(
        backend=default_backend(), public_exponent=65537, key_size=2048)

    public_key = key.public_key().public_bytes(
        serialization.Encoding.OpenSSH,
        serialization.PublicFormat.OpenSSH).decode("utf-8")

    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()).decode("utf-8")

    return public_key, pem


def post_prepare_gcp(config: Dict[str, Any]) -> Dict[str, Any]:
    config = copy.deepcopy(config)
    config = _configure_project_id(config)

    try:
        config = fill_available_node_types_resources(config)
    except Exception as exc:
        cli_logger.warning(
            "Failed to detect node resources. "
            "Make sure you have properly configured the GCP credentials: {}.",
            str(exc))
        raise
    config = _configure_permanent_data_volumes(config)
    return config


def _configure_project_id(config):
    project_id = config["provider"].get("project_id")
    if project_id is None and "workspace_name" in config:
        config["provider"]["project_id"] = config["workspace_name"]
    return config


def fill_available_node_types_resources(
        cluster_config: Dict[str, Any]) -> Dict[str, Any]:
    """Fills out missing "resources" field for available_node_types."""
    if "available_node_types" not in cluster_config:
        return cluster_config

    # Get instance information from cloud provider
    provider_config = cluster_config["provider"]
    _, _, compute, tpu = construct_clients_from_provider_config(
        provider_config)

    response = compute.machineTypes().list(
        project=provider_config["project_id"],
        zone=provider_config["availability_zone"],
    ).execute()

    instances_list = response.get("items", [])
    instances_dict = {
        instance["name"]: instance
        for instance in instances_list
    }

    # Update the instance information to node type
    available_node_types = cluster_config["available_node_types"]
    for node_type in available_node_types:
        instance_type = available_node_types[node_type]["node_config"][
            "machineType"]
        if instance_type in instances_dict:
            cpus = instances_dict[instance_type]["guestCpus"]
            detected_resources = {"CPU": cpus}

            memory_total = instances_dict[instance_type]["memoryMb"]
            memory_total_in_bytes = int(memory_total) * 1024 * 1024
            detected_resources["memory"] = memory_total_in_bytes

            gpus = instances_dict[instance_type].get("accelerators")
            if gpus:
                # Current we consider only one accelerator type
                gpu_name = gpus[0]["guestAcceleratorType"]
                detected_resources.update({
                    "GPU": gpus[0]["guestAcceleratorCount"],
                    f"accelerator_type:{gpu_name}": 1
                })

            detected_resources.update(
                available_node_types[node_type].get("resources", {}))
            if detected_resources != \
                    available_node_types[node_type].get("resources", {}):
                available_node_types[node_type][
                    "resources"] = detected_resources
                logger.debug(
                    "Updating the resources of {} to {}.".format(
                        node_type, detected_resources))
        else:
            raise ValueError(
                "Instance type " + instance_type +
                " is not available in GCP zone: " +
                provider_config["availability_zone"] + ".")
    return cluster_config


def _configure_permanent_data_volumes(config):
    if is_permanent_data_volumes(config):
        enable_stable_node_seq_id(config)
    return config


def _configure_disk_type_for_disk(provider_config, disk):
    # fix disk type for all disks
    initialize_params = disk.get("initializeParams")
    if initialize_params is None:
        return

    disk_type = initialize_params.get("diskType")
    if disk_type is None or "diskTypes" in disk_type:
        return

    default_region_disks = provider_config.get("default_region_disks")
    if default_region_disks or "replicaZones" in disk:
        region = provider_config["region"]
        # Fix to format: regions/region/diskTypes/diskType
        fix_disk_type = "regions/{}/diskTypes/{}".format(region, disk_type)
    else:
        zone = provider_config["availability_zone"]
        # Fix to format: zones/zone/diskTypes/diskType
        fix_disk_type = "zones/{}/diskTypes/{}".format(zone, disk_type)
    initialize_params["diskType"] = fix_disk_type


def _configure_disk_volume(
        provider_config, disk, boot, source_image):
    if boot:
        # Need to fix source image for only boot disk
        if "initializeParams" not in disk:
            disk["initializeParams"] = {"sourceImage": source_image}
        else:
            disk["initializeParams"]["sourceImage"] = source_image
    else:
        # for data disk, set flag whether we need to auto delete
        if _is_permanent_data_volumes(provider_config):
            disk["autoDelete"] = False
        else:
            disk["autoDelete"] = True

    _configure_disk_type_for_disk(provider_config, disk)


def _configure_disks_for_node(
                provider_config, cluster_name,
                base_config, labels):
    if _is_permanent_data_volumes(provider_config):
        # node name for disk is in the format of cloudtik-{cluster_name}-{seq_id}
        seq_id = labels.get(CLOUDTIK_TAG_NODE_SEQ_ID) if labels else None
        if not seq_id:
            raise RuntimeError(
                "No node sequence id assigned for using permanent data volumes.")
        node_name_for_disk = "{}-{}-node-{}".format(
            GCP_RESOURCE_NAME_PREFIX, cluster_name, seq_id)
        base_config = copy.deepcopy(base_config)
        base_config = _configure_disk_name_for_volumes(
            base_config, cluster_name, node_name_for_disk)

    return base_config


def _configure_disk_name_for_volumes(
        node_config, cluster_name, node_name):
    disks = node_config.get("disks", [])
    data_disk_id = 0
    for disk in disks:
        boot = disk.get("boot", False)
        if boot:
            continue
        data_disk_id += 1
        _configure_disk_for_volume(
            disk, data_disk_id, cluster_name, node_name)
    return node_config


def _configure_disk_for_volume(
        disk, data_disk_id, cluster_name, node_name):
    initialize_params = get_config_for_update(disk, "initializeParams")
    disk_name = "{}-disk-{}".format(node_name, data_disk_id)
    initialize_params["diskName"] = disk_name

    # add labels for cluster name
    labels = get_config_for_update(initialize_params, "labels")
    labels[CLOUDTIK_TAG_CLUSTER_NAME] = cluster_name


def _configure_disk_volumes_for_node(
        provider_config, node_config):
    source_image = node_config.get("sourceImage", None)
    disks = node_config.get("disks", [])
    for disk in disks:
        boot = disk.get("boot", False)
        _configure_disk_volume(
            provider_config, disk, boot, source_image)

    # Remove the sourceImage from node config
    node_config.pop("sourceImage", None)


def _configure_disk_volumes(config):
    provider_config = get_provider_config(config)
    for node_type in config["available_node_types"].values():
        node_config = node_type["node_config"]
        _configure_disk_volumes_for_node(
            provider_config, node_config)

    return config


def _configure_spot_for_node_type(node_type_config,
                                  prefer_spot_node):
    # To be improved if scheduling has other configurations
    # scheduling:
    #   - preemptible: true
    node_config = node_type_config["node_config"]
    if prefer_spot_node:
        # Add spot instruction
        node_config.pop("scheduling", None)
        node_config["scheduling"] = [{"preemptible": True}]
    else:
        # Remove spot instruction
        node_config.pop("scheduling", None)


def _configure_prefer_spot_node(config):
    prefer_spot_node = config["provider"].get("prefer_spot_node")

    # if no such key, we consider user don't want to override
    if prefer_spot_node is None:
        return config

    # User override, set or remove spot settings for worker node types
    node_types = get_available_node_types(config)
    for node_type_name in node_types:
        if node_type_name == config["head_node_type"]:
            continue

        # worker node type
        node_type_data = node_types[node_type_name]
        _configure_spot_for_node_type(
            node_type_data, prefer_spot_node)

    return config


def _configure_image(config):
    is_gpu = is_gpu_runtime(config)

    default_image = None
    for key, node_type in config["available_node_types"].items():
        node_config = node_type["node_config"]
        source_image = node_config.get("sourceImage", "")
        if source_image == "":
            # Only set to default image if not specified by the user
            default_image = _get_default_image(default_image, is_gpu)
            node_config["sourceImage"] = default_image

    return config


def _get_default_image(default_image, is_gpu):
    if default_image is not None:
        return default_image

    if is_gpu:
        default_image = "projects/deeplearning-platform-release/global/images/family/common-cu110-ubuntu-2004"
    else:
        default_image = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2004-lts"
    return default_image


def bootstrap_gcp(config):
    workspace_name = config.get("workspace_name")
    if not workspace_name:
        raise RuntimeError(
            "Workspace name is not specified in cluster configuration.")

    config = bootstrap_gcp_from_workspace(config)
    return config


def bootstrap_gcp_from_workspace(config):
    if not check_gcp_workspace_integrity(config):
        workspace_name = get_workspace_name(config)
        cli_logger.abort(
            "GCP workspace {} doesn't exist or is in wrong state.", workspace_name)

    config = copy.deepcopy(config)

    # Used internally to store head IAM role.
    config["head_node"] = {}

    # Check if we have any TPUs defined, and if so,
    # insert that information into the provider config
    if _has_tpus_in_node_configs(config):
        config["provider"][HAS_TPU_PROVIDER_FIELD] = True

        # We can't run autoscaling through a serviceAccount on TPUs (atm)
        if _is_head_node_a_tpu(config):
            raise RuntimeError("TPUs are not supported as head nodes.")

    crm, iam, compute, tpu = \
        construct_clients_from_provider_config(config["provider"])

    config = _configure_image(config)
    config = _configure_disk_volumes(config)
    config = _configure_iam_role_from_workspace(config, iam)
    config = _configure_cloud_storage_from_workspace(config)
    config = _configure_cloud_database_from_workspace(config)
    config = _configure_key_pair(config, compute)
    config = _configure_subnet_from_workspace(config, compute)
    config = _configure_prefer_spot_node(config)
    return config


def bootstrap_gcp_workspace(config):
    # create a copy of the input config to modify
    config = copy.deepcopy(config)
    _configure_allowed_ssh_sources(config)
    return config


def _configure_allowed_ssh_sources(config):
    provider_config = get_provider_config(config)
    if "allowed_ssh_sources" not in provider_config:
        return

    allowed_ssh_sources = provider_config["allowed_ssh_sources"]
    if len(allowed_ssh_sources) == 0:
        return

    if "firewalls" not in provider_config:
        provider_config["firewalls"] = {}
    fire_walls = provider_config["firewalls"]

    if "firewall_rules" not in fire_walls:
        fire_walls["firewall_rules"] = []
    firewall_rules = fire_walls["firewall_rules"]

    firewall_rule = {
        "allowed": [
            {
              "IPProtocol": "tcp",
              "ports": [
                "22"
              ]
            }
        ],
        "sourceRanges": [allowed_ssh_source for allowed_ssh_source in allowed_ssh_sources]
    }
    firewall_rules.append(firewall_rule)


def _configure_cloud_storage_from_workspace(config):
    use_managed_cloud_storage = is_use_managed_cloud_storage(config)
    if use_managed_cloud_storage:
        _configure_managed_cloud_storage_from_workspace(
            config, config["provider"])

    return config


def _configure_managed_cloud_storage_from_workspace(config, cloud_provider):
    workspace_name = get_workspace_name(config)
    managed_cloud_storage_name = _get_managed_cloud_storage_name(cloud_provider)
    gcs_bucket = get_managed_gcs_bucket(
        cloud_provider, workspace_name,
        object_storage_name=managed_cloud_storage_name)
    if gcs_bucket is None:
        cli_logger.abort(
            "No managed GCS bucket was found. If you want to use managed GCS bucket, "
            "you should set managed_cloud_storage equal to True when you creating workspace.")

    cloud_storage = get_gcp_cloud_storage_config_for_update(config["provider"])
    cloud_storage[GCP_GCS_BUCKET] = gcs_bucket.name


def _configure_cloud_database_from_workspace(config):
    use_managed_cloud_database = is_use_managed_cloud_database(config)
    if use_managed_cloud_database:
        _configure_managed_cloud_database_from_workspace(
            config, config["provider"])

    return config


def _configure_managed_cloud_database_from_workspace(config, cloud_provider):
    workspace_name = get_workspace_name(config)
    managed_cloud_database_name = _get_managed_cloud_database_name(cloud_provider)
    database_instance = get_managed_database_instance(
        cloud_provider, workspace_name,
        db_instance_name=managed_cloud_database_name)
    if database_instance is None:
        cli_logger.abort(
            "No managed database was found. If you want to use managed database, "
            "you should set managed_cloud_database equal to True when you creating workspace.")

    db_address = _get_managed_database_address(database_instance)
    if not db_address:
        raise RuntimeError(
            "No IP address for managed database instance.")

    database_config = get_gcp_database_config_for_update(config["provider"])
    database_config[GCP_DATABASE_ENDPOINT] = db_address


def _configure_iam_role_for_head(config, iam):
    head_service_account = _get_workspace_service_account(
        config, iam, GCP_HEAD_SERVICE_ACCOUNT_ID)
    if head_service_account is None:
        cli_logger.abort(
            "Head service account not found for workspace.")

    head_service_accounts = [{
        "email": head_service_account["email"],
        # NOTE: The amount of access is determined by the scope + IAM
        # role of the service account. Even if the cloud-platform scope
        # gives (scope) access to the whole cloud-platform, the service
        # account is limited by the IAM rights specified below.
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }]
    config["head_node"]["serviceAccounts"] = head_service_accounts


def _configure_iam_role_for_worker(config, iam):
    # worker service account
    worker_service_account = _get_workspace_service_account(
        config, iam, GCP_WORKER_SERVICE_ACCOUNT_ID)
    if worker_service_account is None:
        cli_logger.abort(
            "Worker service account not found for workspace.")

    worker_service_accounts = [{
        "email": worker_service_account["email"],
        "scopes": ["https://www.googleapis.com/auth/cloud-platform"]
    }]

    for key, node_type in config["available_node_types"].items():
        if key == config["head_node_type"]:
            continue
        node_config = node_type["node_config"]
        node_config["serviceAccounts"] = worker_service_accounts


def _configure_iam_role_from_workspace(config, iam):
    config = copy.deepcopy(config)
    _configure_iam_role_for_head(config, iam)

    worker_role_for_cloud_storage = is_worker_role_for_cloud_storage(config)
    if worker_role_for_cloud_storage:
        _configure_iam_role_for_worker(config, iam)

    return config


def _configure_key_pair(config, compute):
    """Configure SSH access, using an existing key pair if possible.

    Creates a project-wide ssh key that can be used to access all the instances
    unless explicitly prohibited by instance config.

    The ssh-keys created are of format:

      [USERNAME]:ssh-rsa [KEY_VALUE] [USERNAME]

    where:

      [USERNAME] is the user for the SSH key, specified in the config.
      [KEY_VALUE] is the public SSH key value.
    """
    config = copy.deepcopy(config)

    if "ssh_private_key" in config["auth"]:
        return config

    ssh_user = config["auth"]["ssh_user"]

    project = compute.projects().get(
        project=config["provider"]["project_id"]).execute()

    # Key pairs associated with project meta data. The key pairs are general,
    # and not just ssh keys.
    ssh_keys_str = next(
        (item for item in project["commonInstanceMetadata"].get("items", [])
         if item["key"] == "ssh-keys"), {}).get("value", "")

    ssh_keys = ssh_keys_str.split("\n") if ssh_keys_str else []

    # Try a few times to get or create a good key pair.
    key_found = False
    private_key_path = None
    for i in range(10):
        key_name = key_pair_name(i, config["provider"]["region"],
                                 config["provider"]["project_id"], ssh_user)
        public_key_path, private_key_path = key_pair_paths(key_name)

        for ssh_key in ssh_keys:
            key_parts = ssh_key.split(" ")
            if len(key_parts) != 3:
                continue

            if key_parts[2] == ssh_user and os.path.exists(private_key_path):
                # Found a key
                key_found = True
                break

        # Writing the new ssh key to the filesystem fails if the ~/.ssh
        # directory doesn't already exist.
        os.makedirs(os.path.expanduser("~/.ssh"), exist_ok=True)

        # Create a key since it doesn't exist locally or in GCP
        if not key_found and not os.path.exists(private_key_path):
            cli_logger.print("Creating new key pair: {}".format(key_name))
            public_key, private_key = generate_rsa_key_pair()

            _create_project_ssh_key_pair(project, public_key, ssh_user,
                                         compute)

            # Create the directory if it doesn't exists
            private_key_dir = os.path.dirname(private_key_path)
            os.makedirs(private_key_dir, exist_ok=True)

            # We need to make sure to _create_ the file with the right
            # permissions. In order to do that we need to change the default
            # os.open behavior to include the mode we want.
            with open_with_mode(private_key_path, "w", os_mode=0o600) as f:
                f.write(private_key)

            with open(public_key_path, "w") as f:
                f.write(public_key)

            key_found = True
            break

        if key_found:
            break

    assert key_found, "SSH keypair for user {} not found for {}".format(
        ssh_user, private_key_path)
    assert os.path.exists(private_key_path), (
        "Private key file {} not found for user {}"
        "".format(private_key_path, ssh_user))

    cli_logger.print("Private key not specified in config, using: {}",
                     cf.bold(private_key_path))

    config["auth"]["ssh_private_key"] = private_key_path

    return config


def _configure_subnet_from_workspace(config, compute):
    workspace_name = get_workspace_name(config)
    use_internal_ips = is_use_internal_ip(config)

    """Pick a reasonable subnet if not specified by the config."""
    config = copy.deepcopy(config)

    # Rationale: avoid subnet lookup if the network is already
    # completely manually configured

    # networkInterfaces is compute, networkConfig is TPU
    public_subnet = get_subnet(
        config, get_workspace_public_subnet_name(workspace_name), compute)
    private_subnet = get_subnet(
        config, get_workspace_subnet_name(workspace_name), compute)

    public_interfaces = [{
        "subnetwork": public_subnet["selfLink"],
        "accessConfigs": [{
            "name": "External NAT",
            "type": "ONE_TO_ONE_NAT",
        }],
    }]

    private_interfaces = [{
        "subnetwork": private_subnet["selfLink"],
    }]

    for key, node_type in config["available_node_types"].items():
        node_config = node_type["node_config"]
        if key == config["head_node_type"]:
            if use_internal_ips:
                # compute
                node_config["networkInterfaces"] = copy.deepcopy(private_interfaces)
                # TPU
                node_config["networkConfig"] = copy.deepcopy(private_interfaces)[0]
            else:
                # compute
                node_config["networkInterfaces"] = copy.deepcopy(public_interfaces)
                # TPU
                node_config["networkConfig"] = copy.deepcopy(public_interfaces)[0]
                node_config["networkConfig"].pop("accessConfigs")
        else:
            # compute
            node_config["networkInterfaces"] = copy.deepcopy(private_interfaces)
            # TPU
            node_config["networkConfig"] = copy.deepcopy(private_interfaces)[0]

    return config


def verify_gcs_storage(provider_config: Dict[str, Any]):
    gcs_storage = get_gcp_cloud_storage_config(provider_config)
    if gcs_storage is None:
        return

    try:
        use_managed_cloud_storage = _is_use_managed_cloud_storage(
            provider_config)
        if use_managed_cloud_storage:
            storage_gcs = construct_storage(provider_config)
        else:
            private_key_id = gcs_storage.get("gcs.service.account.private.key.id")
            if private_key_id is None:
                # The bucket may be able to accessible from roles
                # Verify through the client credential
                storage_gcs = construct_storage(provider_config)
            else:
                private_key = gcs_storage.get("gcs.service.account.private.key")
                private_key = unescape_private_key(private_key)

                credentials_field = {
                    "project_id": provider_config.get("project_id"),
                    "private_key_id": private_key_id,
                    "private_key": private_key,
                    "client_email": gcs_storage.get("gcs.service.account.client.email"),
                    "token_uri": "https://oauth2.googleapis.com/token"
                }

                credentials = service_account.Credentials.from_service_account_info(
                    credentials_field)
                storage_gcs = _create_storage(credentials)

        storage_gcs.buckets().get(
            bucket=gcs_storage[GCP_GCS_BUCKET]).execute()
    except Exception as e:
        raise StorageTestingError(
            "Error happens when verifying GCS storage configurations. "
            "If you want to go without passing the verification, "
            "set 'verify_cloud_storage' to False under provider config. "
            "Error: {}.".format(str(e))) from None


def with_gcp_environment_variables(
        provider_config, node_type_config: Dict[str, Any], node_id: str):
    config_dict = {}
    export_gcp_cloud_storage_config(provider_config, config_dict)
    export_gcp_cloud_database_config(provider_config, config_dict)

    if "GCP_PROJECT_ID" not in config_dict:
        project_id = provider_config.get("project_id")
        if project_id:
            config_dict["GCP_PROJECT_ID"] = project_id

    return config_dict


def delete_cluster_disks(provider_config, cluster_name):
    compute = construct_compute_client(provider_config)
    current_step = 1
    total_steps = 2

    with cli_logger.group(
            "Deleting zone disks",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        delete_cluster_zone_disks(
            provider_config, cluster_name, compute)

    with cli_logger.group(
            "Deleting region disks",
            _numbered=("()", current_step, total_steps)):
        current_step += 1
        delete_cluster_region_disks(
            provider_config, cluster_name, compute)


def delete_cluster_zone_disks(
        provider_config, cluster_name, compute):
    project_id = provider_config["project_id"]
    availability_zone = provider_config.get("availability_zone")

    cli_logger.print(
        "Getting zone disks for cluster: {}", cluster_name)

    filter_expr = '(labels.{key} = {value})'.format(
        key=CLOUDTIK_TAG_CLUSTER_NAME, value=cluster_name)

    disks = []
    paged_disks = _get_cluster_zone_disks(
        compute, project_id, availability_zone,
        filter_expr)

    disks.extend(paged_disks.get("items", []))
    next_page_token = paged_disks.get("nextPageToken", None)

    while next_page_token is not None:
        paged_disks = _get_cluster_zone_disks(
            compute, project_id, availability_zone,
            filter_expr, next_page_token=next_page_token)

        disks.extend(paged_disks.get("items", []))
        next_page_token = paged_disks.get("nextPageToken", None)

    num_disks = len(disks)
    cli_logger.print(
        "Got {} zone disks to delete.", num_disks)

    if num_disks:
        # delete the disks
        cli_logger.print(
            "Deleting {} zone disks...", num_disks)

        num_deleted_disks = 0
        for i, disk in enumerate(disks):
            disk_name = disk["name"]
            with cli_logger.group(
                    "Deleting zone disk: {}",
                    disk_name,
                    _numbered=("()", i + 1, num_disks)):
                try:
                    operation = compute.disks().delete(
                        project=project_id,
                        zone=availability_zone,
                        disk=disk_name,
                    ).execute()
                    wait_for_compute_zone_operation(
                        project_id, availability_zone, operation, compute)
                    num_deleted_disks += 1
                    cli_logger.print(
                        "Successfully deleted.")
                except Exception as e:
                    cli_logger.error(
                        "Failed to delete zone disk: {}", str(e))

        cli_logger.print(
            "Successfully deleted {} zone disks.", num_deleted_disks)


def _get_cluster_zone_disks(
        compute, project_id, availability_zone,
        filter_expr, next_page_token=None):
    response = compute.disks().list(
        project=project_id,
        zone=availability_zone,
        filter=filter_expr,
        pageToken=next_page_token
    ).execute()
    return response


def delete_cluster_region_disks(
        provider_config, cluster_name, compute):
    project_id = provider_config["project_id"]
    region = provider_config["region"]

    cli_logger.print(
        "Getting region disks for cluster: {}", cluster_name)

    filter_expr = '(labels.{key} = {value})'.format(
        key=CLOUDTIK_TAG_CLUSTER_NAME, value=cluster_name)

    disks = []
    paged_disks = _get_cluster_region_disks(
        compute, project_id, region,
        filter_expr)

    disks.extend(paged_disks.get("items", []))
    next_page_token = paged_disks.get("nextPageToken", None)

    while next_page_token is not None:
        paged_disks = _get_cluster_region_disks(
            compute, project_id, region,
            filter_expr, next_page_token=next_page_token)

        disks.extend(paged_disks.get("items", []))
        next_page_token = paged_disks.get("nextPageToken", None)

    num_disks = len(disks)
    cli_logger.print(
        "Got {} region disks to delete.", num_disks)

    if num_disks:
        # delete the disks
        cli_logger.print(
            "Deleting {} region disks...", num_disks)

        num_deleted_disks = 0
        for i, disk in enumerate(disks):
            disk_name = disk["name"]
            with cli_logger.group(
                    "Deleting region disk: {}",
                    disk_name,
                    _numbered=("()", i + 1, num_disks)):
                try:
                    operation = compute.regionDisks().delete(
                        project=project_id,
                        region=region,
                        disk=disk_name,
                    ).execute()
                    wait_for_compute_region_operation(
                        project_id, region, operation, compute)
                    num_deleted_disks += 1
                    cli_logger.print(
                        "Successfully deleted.")
                except Exception as e:
                    cli_logger.error(
                        "Failed to delete region disk: {}", str(e))

        cli_logger.print(
            "Successfully deleted {} region disks.", num_deleted_disks)


def _get_cluster_region_disks(
        compute, project_id, region,
        filter_expr, next_page_token=None):
    response = compute.regionDisks().list(
        project=project_id,
        region=region,
        filter=filter_expr,
        pageToken=next_page_token
    ).execute()
    return response

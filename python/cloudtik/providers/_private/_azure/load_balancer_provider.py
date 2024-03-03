import logging
import re
from typing import Any, Dict

from azure.mgmt.network import NetworkManagementClient

from cloudtik.core.load_balancer_provider import LoadBalancerProvider
from cloudtik.providers._private._azure.config import get_virtual_network_name
from cloudtik.providers._private._azure.load_balancer_config import _list_load_balancers, \
    _delete_load_balancer, _create_load_balancer, _update_load_balancer, _get_load_balancer, \
    _bootstrap_load_balancer_config
from cloudtik.providers._private._azure.utils import get_credential

logger = logging.getLogger(__name__)


# Azure load balancer name
# Alphanumerics, underscores, periods, and hyphens.
# Start with alphanumeric. End alphanumeric or underscore.
AZURE_LOAD_BALANCER_NAME_MIN_LEN = 1
AZURE_LOAD_BALANCER_NAME_MAX_LEN = 80


def check_load_balancer_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class AzureLoadBalancerProvider(LoadBalancerProvider):
    """Provider for creating or deleting cloud load balancer services for Azure."""

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        super().__init__(provider_config, workspace_name)
        self.resource_group_name = self.provider_config["resource_group"]

        subscription_id = provider_config["subscription_id"]
        self.credential = get_credential(provider_config)
        self.network_client = NetworkManagementClient(
            self.credential,
            subscription_id)
        self.virtual_network_name = get_virtual_network_name(
            provider_config, workspace_name)

        self.context = {}

    def support_multi_service_group(self):
        """Returns whether the load balancer provider support multi service groups
        for a single load balancer"""
        return True

    def list(self):
        """List the load balancer in the workspace"""
        return _list_load_balancers(
            self.network_client, self.resource_group_name)

    def get(self, load_balancer_name: str):
        """Get the load balancer information given the load balancer name"""
        return _get_load_balancer(
            self.network_client, self.resource_group_name,
            load_balancer_name)

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config."""
        _create_load_balancer(
            self.network_client, self.provider_config, self.workspace_name,
            self.virtual_network_name, load_balancer_config, self.context)

    def update(
            self, load_balancer: Dict[str, Any],
            load_balancer_config: Dict[str, Any]):
        """Update a load balancer in the workspace based on the config.
        """
        _update_load_balancer(
            self.network_client, self.provider_config, self.workspace_name,
            self.virtual_network_name, load_balancer_config, self.context)

    def delete(self, load_balancer: Dict[str, Any]):
        """Delete a load balancer in the workspace.
        """
        _delete_load_balancer(
            self.network_client, self.resource_group_name,
            load_balancer, self.context)

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        pass

    @staticmethod
    def bootstrap_config(
            config: Dict[str, Any], provider_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return _bootstrap_load_balancer_config(config, provider_config)

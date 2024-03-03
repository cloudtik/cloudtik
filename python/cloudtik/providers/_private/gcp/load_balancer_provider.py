import logging
import re
from typing import Any, Dict

from cloudtik.core.load_balancer_provider import LoadBalancerProvider
from cloudtik.providers._private.gcp.config import get_gcp_vpc_name
from cloudtik.providers._private.gcp.load_balancer_config import _list_load_balancers, \
    _delete_load_balancer, _create_load_balancer, _update_load_balancer, _get_load_balancer, \
    _bootstrap_load_balancer_config
from cloudtik.providers._private.gcp.utils import construct_compute_client

logger = logging.getLogger(__name__)


# GCP load balancer name
# The name must be 1-63 characters long, and comply with RFC1035.
# Specifically, the name must be 1-63 characters long and match
# the regular expression `[a-z]([-a-z0-9]*[a-z0-9])?` which means
# the first character must be a lowercase letter, and all following
# characters must be a dash, lowercase letter, or digit, except the
# last character, which cannot be a dash.
GCP_LOAD_BALANCER_NAME_MIN_LEN = 1
GCP_LOAD_BALANCER_NAME_MAX_LEN = 63


def check_load_balancer_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class GCPLoadBalancerProvider(LoadBalancerProvider):
    """Provider for creating or deleting cloud load balancer services for GCP."""

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        super().__init__(provider_config, workspace_name)
        self.compute = construct_compute_client(provider_config)
        self.vpc_name = get_gcp_vpc_name(
            provider_config, workspace_name)

        self.context = {}

    def support_multi_service_group(self):
        """Returns whether the load balancer provider support multi service groups
        for a single load balancer"""
        return False

    def list(self):
        """List the load balancer in the workspace"""
        return _list_load_balancers(
            self.compute, self.provider_config, self.workspace_name)

    def get(self, load_balancer_name: str):
        """Get the load balancer information given the load balancer name"""
        return _get_load_balancer(
            self.compute, self.provider_config, self.workspace_name,
            load_balancer_name)

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config."""
        _create_load_balancer(
            self.compute, self.provider_config, self.workspace_name, self.vpc_name,
            load_balancer_config, self.context)

    def update(
            self, load_balancer: Dict[str, Any],
            load_balancer_config: Dict[str, Any]):
        """Update a load balancer in the workspace based on the config.
        """
        _update_load_balancer(
            self.compute, self.provider_config, self.workspace_name, self.vpc_name,
            load_balancer, load_balancer_config, self.context)

    def delete(self, load_balancer: Dict[str, Any]):
        """Delete a load balancer in the workspace.
        """
        _delete_load_balancer(
            self.compute, self.provider_config, self.workspace_name,
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

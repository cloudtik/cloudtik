import logging
import re
from typing import Any, Dict

from cloudtik.core.load_balancer_provider import LoadBalancerProvider
from cloudtik.providers._private.aws.config import \
    get_workspace_vpc_id
from cloudtik.providers._private.aws.load_balancer_config import _get_workspace_load_balancer_info, \
    _delete_load_balancer_by_name, _create_load_balancer, _update_load_balancer, _get_load_balancer_info, \
    _bootstrap_load_balancer_config
from cloudtik.providers._private.aws.utils import _make_client

logger = logging.getLogger(__name__)


AWS_LOAD_BALANCER_NAME_MIN_LEN = 1
AWS_LOAD_BALANCER_NAME_MAX_LEN = 32


def check_load_balancer_name_format(workspace_name):
    return bool(re.match(r"^[a-z0-9-]*$", workspace_name))


class AWSLoadBalancerProvider(LoadBalancerProvider):
    """Provider for creating or deleting cloud load balancer services for AWS."""

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        super().__init__(provider_config, workspace_name)
        self.elb_client = _make_client("elbv2", provider_config)
        self.vpc_id = get_workspace_vpc_id(provider_config, workspace_name)
        self.context = {}

    def is_multi_listener(self):
        """Returns whether the load balancer provider support multi listener
        for a single load balancer"""
        return True

    def list(self):
        """List the load balancer in the workspace"""
        return _get_workspace_load_balancer_info(
            self.elb_client, self.workspace_name)

    def get(self, load_balancer_name: str):
        """Check whether a load balancer exists"""
        return _get_load_balancer_info(
            self.elb_client, load_balancer_name)

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config."""
        _create_load_balancer(
            self.elb_client, self.provider_config,
            self.workspace_name, load_balancer_config,
            self.vpc_id, self.context)

    def update(self, load_balancer_config: Dict[str, Any]):
        """Update a load balancer in the workspace based on the config.
        """
        _update_load_balancer(
            self.elb_client, self.provider_config,
            self.workspace_name, load_balancer_config,
            self.vpc_id, self.context)

    def delete(self, load_balancer_name: str):
        """Delete a load balancer in the workspace based on the config.
        """
        _delete_load_balancer_by_name(
            self.elb_client, load_balancer_name,
            self.context)

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
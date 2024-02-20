import logging
from typing import Any, Dict

from cloudtik.core._private.annotations import DeveloperAPI

logger = logging.getLogger(__name__)

# Load balancer type
LOAD_BALANCER_TYPE_NETWORK = "network"
LOAD_BALANCER_TYPE_APPLICATION = "application"

# Network load balancer
LOAD_BALANCER_PROTOCOL_TCP = "TCP"
LOAD_BALANCER_PROTOCOL_TLS = "TLS"
LOAD_BALANCER_PROTOCOL_UDP = "UDP"

# Application load balancer
LOAD_BALANCER_PROTOCOL_HTTP = "HTTP"
LOAD_BALANCER_PROTOCOL_HTTPS = "HTTPS"

# The schema of the load balancer
LOAD_BALANCER_SCHEMA_INTERNET_FACING = "internet-facing"
LOAD_BALANCER_SCHEMA_INTERNAL = "internal"


@DeveloperAPI
class LoadBalancerProvider:
    """Interface for creating, updating and deleting load balancer services for a Cloud.

    **Important**: This is an INTERNAL API that is only exposed for the purpose
    of implementing custom load balancer providers. It is not allowed to call into
    LoadBalancerProvider methods from any package outside.
    """

    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        self.provider_config = provider_config
        self.workspace_name = workspace_name

    def is_multi_listener(self):
        """Returns whether the load balancer provider support multi listener
        for a single load balancer"""
        return True

    def list(self):
        """List the load balancer in the workspace
        The return is a map from load balancer name to its properties.
        {
            "load-balancer-1": {
                "name": "load-balancer-1",
                "type": "network",
                "schema": "internet-facing",
                tags: {...}
            }
        }
        """
        pass

    def get(self, load_balancer_name: str):
        """Return more detailed information for a load balancer including listeners
        {
            "name": "load-balancer-1",
            "type": "network",
            "schema": "internet-facing",
            "listeners": [
                {
                    "protocol": "TCP",
                    "port": 80
                }
            ]
            tags: {...}
        }
        """
        return None

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config.

        The load_balancer_config contains a common concept view of what is needed

        {
            "name": "load-balancer-1",
            "type": "network",
            "schema": "internet-facing",
            "listeners": [
                {
                    "protocol": "TCP",
                    "port": 80,
                    "services": [
                        {
                            "name": "abc"
                            "protocol": "TCP",
                            "port": 8080,
                            "targets": [
                                {
                                    "ip": "172.18.0.1",
                                    "port": 1234,
                                },
                                {
                                    "ip": "172.18.0.2",
                                    "port": 1234,
                                },
                            ]
                        }
                    ]
                }
            ],
            tags: {...}
        }

        """
        pass

    def update(self, load_balancer_config: Dict[str, Any]):
        """Update the load balancer in the workspace based on the config.
        """
        pass

    def delete(self, load_balancer_name: str):
        """Delete a load balancer in the workspace based on the config.
        """
        pass

    def validate_config(self, provider_config: Dict[str, Any]):
        """Check the configuration validation.
        This happens before bootstrap_config
        """
        pass

    @staticmethod
    def bootstrap_config(
            config: Dict[str, Any], provider_config: Dict[str, Any]) -> Dict[str, Any]:
        """Bootstraps the config by adding env defaults if needed."""
        return provider_config

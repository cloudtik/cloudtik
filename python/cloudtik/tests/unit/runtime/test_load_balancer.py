import copy
import unittest
from typing import Dict, Any

import pytest

from cloudtik.core.load_balancer_provider import LoadBalancerProvider, LOAD_BALANCER_PROTOCOL_TCP, \
    LOAD_BALANCER_PROTOCOL_HTTP, LOAD_BALANCER_TYPE_NETWORK, LOAD_BALANCER_TYPE_APPLICATION
from cloudtik.runtime.loadbalancer.provider_api import LoadBalancerManager, LOAD_BALANCER_APPLICATION_DEFAULT, \
    LOAD_BALANCER_NETWORK_DEFAULT
from cloudtik.runtime.loadbalancer.scripting import _get_backend_services_from_config


class MockLoadBalancerProvider(LoadBalancerProvider):
    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str,
            multi_service_group: bool = True) -> None:
        super().__init__(provider_config, workspace_name)
        self.multi_service_group = multi_service_group
        self.load_balancer_create = {}
        self.load_balancer_update = {}
        self.load_balancer_delete = set()

    def support_multi_service_group(self):
        """Returns whether the load balancer provider support multi service groups
        for a single load balancer"""
        return self.multi_service_group

    def list(self):
        """List the load balancer in the workspace"""
        return {}

    def get(self, load_balancer_name: str):
        """Check whether a load balancer exists"""
        return None

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config."""
        self.load_balancer_create[
            load_balancer_config["name"]] = load_balancer_config

    def update(self, load_balancer_config: Dict[str, Any]):
        """Update a load balancer in the workspace based on the config.
        """
        self.load_balancer_update[
            load_balancer_config["name"]] = load_balancer_config

    def delete(self, load_balancer: Dict[str, Any]):
        """Delete a load balancer in the workspace based on the config.
        """
        self.load_balancer_delete.add(load_balancer["name"])


class MockLoadBalancerManager(LoadBalancerManager):
    def __init__(
            self, provider_config, workspace_name,
            multi_service_group=True):
        self.provider_config = provider_config
        self.workspace_name = workspace_name
        self.load_balancer_provider = MockLoadBalancerProvider(
            provider_config, workspace_name, multi_service_group)
        self.load_balancer_last_hash = {}
        self.default_network_load_balancer = self._get_default_network_load_balancer_name()
        self.default_application_load_balancer = self._get_default_application_load_balancer_name()


SERVERS = [
            "192.168.0.1:1234",
            "192.168.0.2:1234",
            "192.168.0.3:1234",
        ]

BACKEND_CONFIG = {
    "services": {
        "a-1": {
            "protocol": LOAD_BALANCER_PROTOCOL_TCP,
            "port": 1000,
            "load_balancer_port": 100,
            "load_balancer_name": "lb-a",
            "servers": SERVERS
        },
        "a-2": {
            "protocol": LOAD_BALANCER_PROTOCOL_TCP,
            "port": 1000,
            "load_balancer_port": 110,
            "load_balancer_name": "lb-a",
            "servers": SERVERS
        },
        "b-1": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 80,
            "load_balancer_name": "lb-b",
            "servers": SERVERS
        },
        "b-2": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8090,
            "load_balancer_port": 80,
            "load_balancer_name": "lb-b",
            "servers": SERVERS
        },
        "b-3": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 81,
            "load_balancer_name": "lb-b",
            "servers": SERVERS
        },
        "b-4": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8090,
            "load_balancer_port": 81,
            "load_balancer_name": "lb-b",
            "servers": SERVERS
        },
        "c-1": {
            "protocol": LOAD_BALANCER_PROTOCOL_TCP,
            "port": 1000,
            "load_balancer_port": 100,
            "servers": SERVERS
        },
        "c-2": {
            "protocol": LOAD_BALANCER_PROTOCOL_TCP,
            "port": 1000,
            "load_balancer_port": 110,
            "servers": SERVERS
        },
        "d-1": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 80,
            "servers": SERVERS
        },
        "d-2": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 80,
            "servers": SERVERS
        },
        "d-3": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 81,
            "servers": SERVERS
        },
        "d-4": {
            "protocol": LOAD_BALANCER_PROTOCOL_HTTP,
            "port": 8080,
            "load_balancer_port": 81,
            "servers": SERVERS
        }
    }
}


class TestLoadBalancer(unittest.TestCase):
    def test_load_balancer_multi_service_group(self):
        provider_config = {}
        backend_services = _get_backend_services_from_config(BACKEND_CONFIG)
        workspace_name = "abc"

        load_balancer_manager = MockLoadBalancerManager(
            provider_config, workspace_name)
        load_balancer_manager.update(backend_services)
        load_balancer_create = load_balancer_manager.load_balancer_provider.load_balancer_create

        lb = load_balancer_create.get("lb-a")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_NETWORK
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 2
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 1

        lb = load_balancer_create.get("lb-b")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_APPLICATION
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 2
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 2

        lb = load_balancer_create.get(
            LOAD_BALANCER_NETWORK_DEFAULT.format(workspace_name))
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_NETWORK
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 2
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 1

        lb = load_balancer_create.get(
            LOAD_BALANCER_APPLICATION_DEFAULT.format(workspace_name))
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_APPLICATION
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 2
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 2

    def test_load_balancer_single_service_group(self):
        provider_config = {}
        backend_config = copy.deepcopy(BACKEND_CONFIG)
        services_config = backend_config["services"]
        services_config.pop("a-2")
        services_config.pop("b-3")
        services_config.pop("b-4")

        backend_services = _get_backend_services_from_config(backend_config)
        workspace_name = "abc"

        load_balancer_manager = MockLoadBalancerManager(
            provider_config, workspace_name, multi_service_group=False)
        load_balancer_manager.update(backend_services)
        load_balancer_create = load_balancer_manager.load_balancer_provider.load_balancer_create

        lb = load_balancer_create.get("lb-a")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_NETWORK
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 1

        lb = load_balancer_create.get("lb-b")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_APPLICATION
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 2

        lb = load_balancer_create.get("c-1")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_NETWORK
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 1

        lb = load_balancer_create.get("c-2")
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_NETWORK
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 1

        lb = load_balancer_create.get("{}-{}-{}".format(
            workspace_name, LOAD_BALANCER_PROTOCOL_HTTP, 80))
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_APPLICATION
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 2

        lb = load_balancer_create.get("{}-{}-{}".format(
            workspace_name, LOAD_BALANCER_PROTOCOL_HTTP, 81))
        assert lb is not None
        assert lb["type"] == LOAD_BALANCER_TYPE_APPLICATION
        lb_service_groups = lb["service_groups"]
        assert len(lb_service_groups) == 1
        for lb_service_group in lb_service_groups:
            assert len(lb_service_group["services"]) == 2


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

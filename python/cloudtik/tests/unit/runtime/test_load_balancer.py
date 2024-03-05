import copy
import unittest
from typing import Dict, Any

import pytest

from cloudtik.core.load_balancer_provider import LoadBalancerProvider, LOAD_BALANCER_PROTOCOL_TCP, \
    LOAD_BALANCER_PROTOCOL_HTTP, LOAD_BALANCER_TYPE_NETWORK, LOAD_BALANCER_TYPE_APPLICATION, \
    LOAD_BALANCER_SCHEME_INTERNAL
from cloudtik.providers._private._azure.load_balancer_provider import AzureLoadBalancerProvider
from cloudtik.providers._private.gcp.load_balancer_provider import GCPLoadBalancerProvider
from cloudtik.runtime.loadbalancer.provider_api import LoadBalancerManager, LOAD_BALANCER_APPLICATION_DEFAULT, \
    LOAD_BALANCER_NETWORK_DEFAULT
from cloudtik.runtime.loadbalancer.scripting import _get_backend_services_from_config


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
        """Get the load balancer information given the load balancer name"""
        return None

    def create(self, load_balancer_config: Dict[str, Any]):
        """Create the load balancer in the workspace based on the config."""
        self.load_balancer_create[
            load_balancer_config["name"]] = load_balancer_config

    def update(
            self, load_balancer: Dict[str, Any],
            load_balancer_config: Dict[str, Any]):
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
        self.multi_service_group = multi_service_group
        self.load_balancer_hash = {}
        self.default_network_load_balancer = self._get_default_network_load_balancer_name()
        self.default_application_load_balancer = self._get_default_application_load_balancer_name()
        self.error_abort = True
        self.default_load_balancer_scheme = LOAD_BALANCER_SCHEME_INTERNAL
        self.load_balancer_provider = self._get_load_balancer_provider()

    def _get_load_balancer_provider(self):
        return MockLoadBalancerProvider(
            self.provider_config, self.workspace_name, self.multi_service_group)


class MockAzureLoadBalancerManager(MockLoadBalancerManager):
    def __init__(
            self, provider_config, workspace_name,
            multi_service_group=True):
        super().__init__(
            provider_config, workspace_name, multi_service_group)

    def _get_load_balancer_provider(self):
        return MockAzureLoadBalancerProvider(
            self.provider_config, self.workspace_name)


class MockAzureLoadBalancerProvider(AzureLoadBalancerProvider):
    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        LoadBalancerProvider.__init__(self, provider_config, workspace_name)
        self.resource_group_name = self.provider_config["resource_group"]
        self.network_client = MockAzureNetworkManagementClient()
        self.virtual_network_name = workspace_name

        self.context = {}


class MockAzureNetworkManagementClient:
    def __init__(self):
        self.load_balancers = MockAzureCollection("load_balancers")
        self.application_gateways = MockAzureCollection("application_gateways")
        self.public_ip_addresses = MockAzureCollection("public_ip_addresses")


class MockAzureCollection:
    def __init__(self, name):
        self.name = name

    def list(self, *args, **kwargs):
        return []

    def get(self, *args, **kwargs):
        return MockAzureResource()

    def begin_create_or_update(self, *args, **kwargs):
        return MockAzureOperation(self.name, "create_or_update")

    def begin_delete(self, *args, **kwargs):
        return MockAzureOperation(self.name, "delete")


class MockAzureOperation:
    def __init__(self, collection_name, name):
        self.collection_name = collection_name
        self.name = name

    def result(self):
        return MockAzureResource()


class MockAzureResource:
    def __init__(self):
        self.name = "xyz"


class MockGCPLoadBalancerManager(MockLoadBalancerManager):
    def __init__(
            self, provider_config, workspace_name,
            multi_service_group=True):
        super().__init__(
            provider_config, workspace_name, multi_service_group)

    def _get_load_balancer_provider(self):
        return MockGCPLoadBalancerProvider(
            self.provider_config, self.workspace_name)


class MockGCPLoadBalancerProvider(GCPLoadBalancerProvider):
    def __init__(
            self,
            provider_config: Dict[str, Any],
            workspace_name: str) -> None:
        LoadBalancerProvider.__init__(self, provider_config, workspace_name)
        self.compute = MockGCPComputeClient()
        self.vpc_name = workspace_name

        self.context = {}


class MockGCPComputeClient:
    def __init__(self):
        pass

    def networkEndpointGroups(self):
        return MockGCPCollection("networkEndpointGroups")

    def backendServices(self):
        return MockGCPCollection("backendServices")

    def regionBackendServices(self):
        return MockGCPCollection("regionBackendServices")

    def healthChecks(self):
        return MockGCPCollection("healthChecks")

    def regionHealthChecks(self):
        return MockGCPCollection("regionHealthChecks")

    def targetTcpProxies(self):
        return MockGCPCollection("targetTcpProxies")

    def regionTargetTcpProxies(self):
        return MockGCPCollection("regionTargetTcpProxies")

    def targetHttpProxies(self):
        return MockGCPCollection("targetHttpProxies")

    def regionTargetHttpProxies(self):
        return MockGCPCollection("regionTargetHttpProxies")

    def targetHttpsProxies(self):
        return MockGCPCollection("targetHttpsProxies")

    def regionTargetHttpsProxies(self):
        return MockGCPCollection("regionTargetHttpsProxies")

    def forwardingRules(self):
        return MockGCPCollection("forwardingRules")

    def globalForwardingRules(self):
        return MockGCPCollection("globalForwardingRules")

    def urlMaps(self):
        return MockGCPCollection("urlMaps")

    def regionUrlMaps(self):
        return MockGCPCollection("regionUrlMaps")

    def globalOperations(self):
        return MockGCPCollection("globalOperations")

    def regionOperations(self):
        return MockGCPCollection("regionOperations")

    def zoneOperations(self):
        return MockGCPCollection("zoneOperations")


class MockGCPCollection:
    def __init__(self, name):
        self.name = name

    def list(self, *args, **kwargs):
        return MockGCPOperation(self.name, "list")

    def get(self, *args, **kwargs):
        return MockGCPOperation(self.name, "get")

    def insert(self, *args, **kwargs):
        return MockGCPOperation(self.name, "insert")

    def delete(self, *args, **kwargs):
        return MockGCPOperation(self.name, "delete")

    def listNetworkEndpoints(self, *args, **kwargs):
        return MockGCPOperation(self.name, "listNetworkEndpoints")

    def attachNetworkEndpoints(self, *args, **kwargs):
        return MockGCPOperation(self.name, "attachNetworkEndpoints")

    def detachNetworkEndpoints(self, *args, **kwargs):
        return MockGCPOperation(self.name, "detachNetworkEndpoints")

    def setBackendService(self, *args, **kwargs):
        return MockGCPOperation(self.name, "setBackendService")


class MockGCPOperation:
    def __init__(self, collection_name, name):
        self.collection_name = collection_name
        self.name = name

    def execute(self):
        if "list" in self.name:
            return {}
        elif self.name == "get":
            if self.collection_name in ["globalOperations", "regionOperations", "zoneOperations"]:
                return {
                    "status": "DONE"
                }
            else:
                return {}
        else:
            return {
                "name": "operation-{}-{}".format(self.collection_name, self.name),
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

    def test_load_balancer_azure(self):
        config = {
            "provider": {
                "type": "azure",
                "subscription_id": "my-subscription",
                "resource_group": "my-resource-group",
                "location": "us-west",
            }
        }
        provider_config = {}
        AzureLoadBalancerProvider.bootstrap_config(
            config, provider_config)

        backend_services = _get_backend_services_from_config(BACKEND_CONFIG)
        workspace_name = "abc"

        load_balancer_manager = MockAzureLoadBalancerManager(
            provider_config, workspace_name)
        load_balancer_manager.update(backend_services)

    def test_load_balancer_gcp(self):
        config = {
            "provider": {
                "type": "gcp",
                "project_id": "my-project-id",
                "availability_zone": "us-west-1",
                "region": "us-west",
            }
        }
        provider_config = {}
        GCPLoadBalancerProvider.bootstrap_config(
            config, provider_config)

        backend_services = _get_backend_services_from_config(BACKEND_CONFIG)
        workspace_name = "abc"

        load_balancer_manager = MockGCPLoadBalancerManager(
            provider_config, workspace_name)
        load_balancer_manager.update(backend_services)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

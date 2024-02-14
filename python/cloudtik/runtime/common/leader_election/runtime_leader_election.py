import urllib.parse

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_CONSUL
from cloudtik.core._private.utils import is_runtime_enabled
from cloudtik.runtime.common.leader_election.consul_leader_election import ConsulLeaderElection
from cloudtik.runtime.common.leader_election.etcd_leader_election import EtcdLeaderElection
from cloudtik.runtime.common.leader_election.leader_election_base import LeaderElection
from cloudtik.runtime.common.lock.runtime_lock import DISTRIBUTED_SCHEMA_CONSUL, DISTRIBUTED_SCHEMA_ETCD, \
    _get_distributed_system_url, ETCD_URI_KEY


# A factory method for creating a leader election based on runtime configuration
# If consul is configured, we create a consul leader election
# New implementations can be added
def get_runtime_leader_election(
        url, service_name: str,
        leader_ttl=None, leader_elect_delay=None) -> LeaderElection:
    addr = urllib.parse.urlparse(url)
    schema = addr.scheme
    if schema == DISTRIBUTED_SCHEMA_CONSUL:
        return ConsulLeaderElection(
            service_name,
            leader_ttl=leader_ttl,
            leader_elect_delay=leader_elect_delay)
    elif schema == DISTRIBUTED_SCHEMA_ETCD:
        endpoints = addr.netloc
        if not endpoints:
            raise ValueError(
                "Invalid distributed system URL. No endpoints specified.")
        return EtcdLeaderElection(
            endpoints,
            service_name,
            leader_ttl=leader_ttl,
            leader_elect_delay=leader_elect_delay)
    else:
        raise ValueError(
            "Unknown distributed system schema: {}".format(schema))


def get_runtime_leader_election_url(
        runtime_config, runtime_type):
    if is_runtime_enabled(
            runtime_config, BUILT_IN_RUNTIME_CONSUL):
        return _get_distributed_system_url(DISTRIBUTED_SCHEMA_CONSUL)

    if runtime_type:
        runtime_type_config = runtime_config.get(runtime_type, {})
        etcd_uri = runtime_type_config.get(ETCD_URI_KEY)
        if etcd_uri:
            return _get_distributed_system_url(
                DISTRIBUTED_SCHEMA_ETCD, etcd_uri)

    raise RuntimeError(
        "No distributed system configuration found.")

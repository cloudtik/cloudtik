from typing import List

from cloudtik.core._private.runtime_factory import BUILT_IN_RUNTIME_CONSUL
from cloudtik.runtime.common.leader_election.consul_leader_election import ConsulLeaderElection
from cloudtik.runtime.common.leader_election.leader_election_base import LeaderElection


# A factory method for creating a leader election based on runtime configuration
# If consul is configured, we create a consul leader election
# New implementations can be added
def get_runtime_leader_election(
        runtime_types: List[str], service_name: str,
        leader_ttl=None, leader_elect_delay=None) -> LeaderElection:
    if (runtime_types
            and BUILT_IN_RUNTIME_CONSUL in runtime_types):
        return ConsulLeaderElection(
            service_name,
            leader_ttl=leader_ttl,
            leader_elect_delay=leader_elect_delay)

    raise RuntimeError("No leader election runtime found.")

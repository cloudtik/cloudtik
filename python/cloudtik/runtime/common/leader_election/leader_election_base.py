
# default leader ttl to 10 seconds
DEFAULT_LEADER_TTL_SECONDS = 10

# A old leader have most N seconds to stop on his current work
# During N seconds, nobody will be able to elect leader
DEFAULT_LEADER_DELAY_SECONDS = 10


def _get_required(value, default):
    if value is not None:
        return value
    return default


class LeaderElectionException(RuntimeError):
    pass


class LeaderElection(object):
    def __init__(
            self,
            key,
            leader_ttl=None,
            leader_elect_delay=None):
        assert key, 'Key is required for leader selection.'
        self.key = key
        self.leader_ttl = _get_required(
            leader_ttl, DEFAULT_LEADER_TTL_SECONDS)
        self.leader_elect_delay = _get_required(
            leader_elect_delay, DEFAULT_LEADER_DELAY_SECONDS)

    def elect(self) -> bool:
        """
        Attempt to acquire the lock for leader.

        :return: True if the lock was successfully acquired,
            false if it was not (unreachable if failing hard)
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

    def step_down(self):
        """
        Release the lock immediately. Does nothing if never locked.
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

    def get_current_leader(self):
        """
        Query the key for current leader
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

    def is_leader(self, current_leader):
        """
        Check whether this instance is the leader. Return True if it is
        the current leader
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

    def watch(self, current_leader):
        """
        Watch the changes of the current leader. This may be called in a
        separate thread than the main methods.
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

    def heartbeat(self, current_leader):
        """
        Heartbeat to the session for keep the current leader alive. This
        may be called in a separate thread than the main methods.
        """
        raise RuntimeError(
            "A leader election implementation needs to override this.")

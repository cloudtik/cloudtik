from datetime import datetime

from cloudtik.runtime.common.consul_utils import \
    create_session, acquire_key, destroy_session, renew_session, \
    get_key, query_key_blocking, ConsulClient
from cloudtik.runtime.common.leader_election.leader_election_base import LeaderElection

# the key will always be substituted into this pattern
# a good prefix is recommended for organization
FULL_KEY_PATTERN = 'cloudtik/leader/%s'


class ConsulLeaderElection(LeaderElection):
    def __init__(
            self,
            key,
            leader_ttl=None,
            leader_elect_delay=None,
            endpoints=None):
        """
        Args:
        key: the unique key to lock
        leader_ttl: how long the lock will stay alive if it is never released,
            this is controlled by Consul's Session TTL and may stay alive a bit longer according
            to their docs. As of the current version of Consul, this must be between 10s and 86400s
        endpoints: The endpoints for the Consul cluster. None for connecting with local.
        """
        super().__init__(key, leader_ttl, leader_elect_delay)
        self.client = ConsulClient(endpoints)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = None
        self.leader = False
        # how long to hold locks after session times out.
        assert 10 <= self.leader_ttl <= 86400, \
            'leader_ttl must be between 10 and 86400 to due to Consul session ttl settings'
        assert 0 <= self.leader_elect_delay <= 60, \
            'leader_elect_delay must be between 0 and 60 seconds to due to Consul session lock delay settings'

    def elect(self) -> bool:
        """
        Attempt to acquire the lock for leader

        return: True if the lock was successfully acquired,
            false if it was not (unreachable if failing hard)
        """
        assert not self.leader, 'Already elect as leader.'

        session = self._create_session()
        self.session_id = session["ID"]
        self.leader = self._acquire_key()
        if not self.leader:
            # failed to become leader
            self._destroy_session()
        return self.leader

    def step_down(self):
        """
        Release the lock immediately. Does nothing if never locked.
        """
        if not self.leader:
            return False
        return self._release_key()

    def get_current_leader(self):
        """
        Query the key for current leader
        """
        keys = get_key(self.client, self.full_key)
        if not keys:
            # no leader
            return None
        key_meta = keys[0]
        session_id = self._get_session_id(key_meta)
        if not session_id:
            return None
        return key_meta

    def is_leader(self, current_leader):
        """
        Check whether this instance is the leader. Return True if it is
        the current leader
        """
        if not self.session_id:
            return False
        if not current_leader:
            return False
        session_id = self._get_session_id(current_leader)
        if self.session_id != session_id:
            return False
        return True

    def watch(self, current_leader):
        """
        Watch the changes of the current leader. This may be called in a
        separate thread than the main methods.
        """
        if not current_leader:
            return None
        index = self._get_modify_index(current_leader)
        keys = query_key_blocking(self.client, self.full_key, index)
        if not keys:
            return None
        key_meta = keys[0]
        modify_index = self._get_modify_index(key_meta)
        if modify_index < index:
            # While indexes in general are monotonically increasing,
            # there are several real-world scenarios in which they can go backwards for a given query.
            # Implementations must check to see if a returned index is lower than the previous value,
            # and if it is, should reset index to 0 - effectively restarting their blocking loop.
            self._reset_modify_index(key_meta)
        return key_meta

    def heartbeat(self, current_leader):
        """
        Heartbeat to the session for keep the current leader alive. This
        may be called in a separate thread than the main methods.
        """
        if not current_leader:
            return
        session_id = self._get_session_id(current_leader)
        renew_session(self.client, session_id)

    def _create_session(self):
        # Consul prevents any of the previously held locks from being re-acquired
        # for the lock-delay interval. The purpose of this delay is to allow the
        # potentially still live leader to detect the invalidation and stop processing
        # requests that may lead to inconsistent state. While not a bulletproof method,
        # it does avoid the need to introduce sleep states into application logic and
        # can help mitigate many issues. Clients are able to disable this mechanism
        # by providing a zero delay value.
        session_lock_delay = self.leader_elect_delay
        # how long to keep the session alive without a renew (heartbeat/keepalive) sent.
        # we are using this to time out the individual lock
        session_ttl = self.leader_ttl

        # delete locks when session is invalidated/destroyed
        session_invalidate_behavior = 'delete'

        session = create_session(
            self.client,
            lock_delay=session_lock_delay,
            ttl=session_ttl,
            behavior=session_invalidate_behavior)
        return session

    def _destroy_session(self):
        if not self.session_id:
            return False
        result = destroy_session(
            self.client, session_id=self.session_id)
        if result:
            self.session_id = None
            self.leader = False
        return result

    def _acquire_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = dict(locked_at=str(datetime.now()))
        return acquire_key(
            self.client,
            session_id=self.session_id,
            key=self.full_key,
            data=data)

    def _release_key(self):
        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Consul.
        return self._destroy_session()

    @staticmethod
    def _get_modify_index(key_meta):
        modify_index = key_meta.get("ModifyIndex")
        if modify_index <= 0:
            raise RuntimeError(
                "Invalid index: should always be greater than zero.")
        return modify_index

    @staticmethod
    def _reset_modify_index(key_meta):
        key_meta["ModifyIndex"] = 0

    @staticmethod
    def _get_session_id(key_meta):
        return key_meta.get("Session")

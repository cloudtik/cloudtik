from datetime import datetime

from cloudtik.runtime.common.etcd_utils import \
    create_session, acquire_key, destroy_session, renew_session, \
    get_key, query_key_blocking, EtcdClient
from cloudtik.runtime.common.leader_election.leader_election_base import LeaderElection

# the key will always be substituted into this pattern
# a good prefix is recommended for organization
FULL_KEY_PATTERN = 'cloudtik/leader/%s'


class EtcdLeaderElection(LeaderElection):
    def __init__(
            self,
            endpoints,
            key,
            leader_ttl=None,
            leader_elect_delay=None):
        """
        Args:
        endpoints: The endpoints for the Etcd cluster
        key: the unique key to lock
        leader_ttl: how long the lock will stay alive if it is never released,
            this is controlled by Etcd lease TTL and may stay alive a bit longer according
            to their docs.
        """
        super().__init__(key, leader_ttl, leader_elect_delay)
        self.client = EtcdClient(endpoints)
        self.full_key = FULL_KEY_PATTERN % key
        self.session_id = None
        self.leader = False

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
        resp = get_key(self.client, self.full_key)
        if not resp:
            # no leader
            return None

        key_meta = self._get_key_meta(resp)
        if not key_meta:
            return None

        global_index = self._get_global_revision(resp)
        if global_index:
            key_meta["global_revision"] = global_index
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
            return
        # watch use global revision for watch if exists
        revision = self._get_key_revision(current_leader)
        query_key_blocking(self.client, self.full_key, revision)

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
        # how long to keep the session alive without a renew (heartbeat/keepalive) sent.
        # we are using this to time out the individual lock
        session_ttl = self.leader_ttl
        session = create_session(
            self.client,
            ttl=session_ttl)
        return session

    def _destroy_session(self):
        if not self.session_id:
            return False
        result = destroy_session(
            self.client, session_id=self.session_id)
        self.session_id = None
        self.leader = False
        return result

    def _acquire_key(self):
        assert self.session_id, 'Must have a session id to acquire key'
        data = str(datetime.now())
        return acquire_key(
            self.client,
            session_id=self.session_id,
            key=self.full_key,
            value=data)

    def _release_key(self):
        # destroying the session will is the safest way to release the lock. we'd like to delete the
        # key, but since it's possible we don't actually have the lock anymore (in distributed systems,
        # there is no spoon). It's best to just destroy the session and let the lock get cleaned up by
        # Etcd.
        return self._destroy_session()

    def _get_key_meta(self, resp):
        keys = resp.get("kvs")
        if not keys:
            return None
        key_meta = keys[0]
        session_id = self._get_session_id(key_meta)
        if not session_id:
            return None
        return key_meta

    @staticmethod
    def _get_global_revision(resp):
        return resp.get("header", {}).get("revision")

    @staticmethod
    def _get_key_revision(key_meta):
        global_revision = key_meta.get("global_revision")
        if global_revision:
            return global_revision
        return key_meta.get("mod_revision")

    @staticmethod
    def _get_session_id(key_meta):
        return key_meta.get("lease")

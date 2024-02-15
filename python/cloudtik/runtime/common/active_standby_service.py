import logging
import queue
import threading
import time

from cloudtik.core._private.util.service.service_runner import ServiceRunner
from cloudtik.runtime.common.leader_election.runtime_leader_election import get_runtime_leader_election

logger = logging.getLogger(__name__)


class ActiveStandbyService(ServiceRunner):
    """Service daemon class for active standby service.
    The active service is elected using leader election algorithm provided by
    a distributed system such as Consul or ZooKeeper.
    """

    def __init__(
            self,
            coordinator_url: str,
            service_name: str,
            leader_ttl: int = None,
            leader_elect_delay: int = None):
        super().__init__()
        self.leader_election = get_runtime_leader_election(
            coordinator_url, service_name,
            leader_ttl=leader_ttl,
            leader_elect_delay=leader_elect_delay)
        self.leader_ttl = self.leader_election.leader_ttl
        self.leader_elect_delay = self.leader_election.leader_elect_delay
        self.leader = False
        self.current_leader = None
        # Use a queue for controlling leader change watch
        self.watch_leader = queue.Queue()
        self.leader_changed = threading.Event()
        self.heartbeat_leader = None
        self.heartbeat_leader_lock = threading.Lock()

    def run(self):
        self._run_watch_thread()
        self._run_heartbeat_thread()
        while True:
            if self._is_stop():
                break

            # we try a leader election
            self.leader = self._try_leader_election()

            # get the current leader
            self.current_leader = self.leader_election.get_current_leader()
            if not self.current_leader:
                # we didn't find a current leader
                if self.leader:
                    self._step_down()
                # backoff and try election
                self._backoff_elect_delay()
                continue

            if self.leader:
                # elected as leader: will do the updates
                # to avoid the try leader election will for every update
                # we can watch the key for changes, only after there is a change
                self._start_watch_leader_change(self.current_leader)
                self._start_leader_heartbeat(self.current_leader)
                logger.info("Become leader. Run as active.")
                while True:
                    if self._is_stop():
                        break
                    leader, current_leader = self._am_i_leader()
                    if not leader:
                        logger.info("Lost leader. Step down.")
                        self._stop_leader_heartbeat()
                        # I think I am the leader, but seems the world is changing
                        self._step_down()
                        break
                    if self.current_leader != current_leader:
                        # I am still the leader but modify index may change
                        self.current_leader = current_leader
                        self._start_watch_leader_change(self.current_leader)

                    # leader: do the work
                    self._run()
            else:
                # Other one is a leader: wait for a change and try a new election
                logger.debug("There is a leader. Waiting for change.")
                self._start_watch_leader_change(self.current_leader)
                self._wait_leader_change()
            # backoff and try election
            self._backoff_elect_delay()

        # exit, we should release if I am the leader
        if self.leader:
            self._step_down()

    def _is_stop(self):
        if self.stop_event and self.stop_event.is_set():
            return True
        return False

    def _backoff_elect_delay(self):
        if self.leader_elect_delay > 0:
            time.sleep(self.leader_elect_delay)

    def _try_leader_election(self):
        leader = self.leader_election.elect()
        return leader

    def _step_down(self):
        try:
            self.leader_election.step_down()
            self.leader = False
        except Exception as e:
            logger.exception(
                "Error happened when stepping down: " + str(e))

    def _start_watch_leader_change(self, current_leader):
        self.leader_changed.clear()
        self.watch_leader.put(current_leader)

    def _start_leader_heartbeat(self, current_leader):
        with self.heartbeat_leader_lock:
            self.heartbeat_leader = current_leader

    def _stop_leader_heartbeat(self):
        with self.heartbeat_leader_lock:
            self.heartbeat_leader = None

    def _get_heartbeat_leader(self):
        with self.heartbeat_leader_lock:
            heartbeat_leader = self.heartbeat_leader
        return heartbeat_leader

    def _am_i_leader(self):
        if not self.leader_changed.is_set():
            # Things are not changed
            return self.leader, self.current_leader

        # Things changed, check the value
        current_leader = self.leader_election.get_current_leader()
        is_leader = self.leader_election.is_leader(current_leader)
        return is_leader, current_leader

    def _wait_leader_change(self):
        # wait the leader change event
        self.leader_changed.wait()

    def _run_watch_thread(self):
        thread = threading.Thread(target=self._watch_leader_change)
        # ensure when main thread exits, the thread will stop automatically.
        thread.daemon = True
        thread.start()

    def _watch_leader_change(self):
        while True:
            # get the current leader to watch
            current_leader = self.watch_leader.get()
            try:
                # watch (should not depending on the status set at main thread)
                self.leader_election.watch(current_leader)
            except Exception:
                # If there is an error, we consider it a change
                pass
            # MAY change
            self.leader_changed.set()

    def _run_heartbeat_thread(self):
        thread = threading.Thread(target=self._heartbeat)
        # ensure when main thread exits, the thread will stop automatically.
        thread.daemon = True
        thread.start()

    def _heartbeat(self):
        # Sleep half of leader TTL
        interval = self.leader_ttl // 2
        while True:
            try:
                current_leader = self._get_heartbeat_leader()
                if current_leader:
                    # should not depending on the status set at main thread
                    self.leader_election.heartbeat(current_leader)
            except Exception:
                # heartbeat error, ignore
                pass
            time.sleep(interval)

    def _run(self):
        # The method is called only when this service is a leader
        # Override by subclass to run the real work
        pass

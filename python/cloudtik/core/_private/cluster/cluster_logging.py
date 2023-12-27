import json
import logging
import os
import sys
import threading
from typing import Any, Dict

import colorama
import redis

from cloudtik.core._private.constants import LOG_FILE_CHANNEL
from cloudtik.core._private.util import core_utils
from cloudtik.core._private.util.core_utils import get_node_ip_address, split_list
from cloudtik.core._private.util.logging_utils import global_standard_stream_dispatcher
from cloudtik.core._private.util.redis_utils import create_redis_client

logger = logging.getLogger(__name__)

LOGGER_ID_CLUSTER_CONTROLLER = "cluster-controller"
LOGGER_ID_NODE_MONITOR = "node-monitor"

LOGGING_DATA_NODE_ID = "id"
LOGGING_DATA_NODE_IP = "ip"
LOGGING_DATA_NODE_TYPE = "type"
LOGGING_DATA_PID = "pid"
LOGGING_DATA_RUNTIME = "runtime"


def print_logs(
        redis_address, redis_password,
        runtimes, node_types, node_ips):
    runtimes = split_list(runtimes) if runtimes else None
    node_types = split_list(node_types) if node_types else None
    node_ips = split_list(node_ips) if node_ips else None
    worker = PrintLogsWorker(
        redis_address, redis_password,
        runtimes, node_types, node_ips)

    try:
        worker.print()
        worker.join()
    except KeyboardInterrupt:
        worker.cancel()
        worker.join()
    finally:
        global_standard_stream_dispatcher.remove_handler("print_logs")


def print_logs_data(data: Dict[str, str], print_file: Any):
    def prefix_for(data: Dict[str, str]) -> str:
        """The prefix for this log line."""
        runtime = data.get(LOGGING_DATA_RUNTIME)
        if runtime:
            prefix = f"{runtime}: "
        else:
            prefix = ""
        pid = data.get(LOGGING_DATA_PID)
        if not isinstance(pid, str):
            return prefix + "pid="
        else:
            return prefix

    def message_for(data: Dict[str, str], line: str) -> str:
        """The printed message of this log line."""
        return line

    def color_for(data: Dict[str, str], line: str) -> str:
        """The color for this log line."""
        if (
            data.get(LOGGING_DATA_PID) == LOGGER_ID_NODE_MONITOR
        ):
            return colorama.Fore.YELLOW
        elif data.get(LOGGING_DATA_PID) == LOGGER_ID_CLUSTER_CONTROLLER:
            if "Error:" in line or "Warning:" in line:
                return colorama.Fore.YELLOW
            else:
                return colorama.Fore.CYAN
        elif os.getenv("CLOUDTIK_PRINT_COLOR_PREFIX") == "1":
            colors = [
                # colorama.Fore.BLUE, # Too dark
                colorama.Fore.MAGENTA,
                colorama.Fore.CYAN,
                colorama.Fore.GREEN,
                # colorama.Fore.WHITE, # Too light
                # colorama.Fore.RED,
                colorama.Fore.LIGHTBLACK_EX,
                colorama.Fore.LIGHTBLUE_EX,
                # colorama.Fore.LIGHTCYAN_EX, # Too light
                # colorama.Fore.LIGHTGREEN_EX, # Too light
                colorama.Fore.LIGHTMAGENTA_EX,
                # colorama.Fore.LIGHTWHITE_EX, # Too light
                # colorama.Fore.LIGHTYELLOW_EX, # Too light
            ]
            pid = data.get(LOGGING_DATA_PID, 0)
            try:
                i = int(pid)
            except ValueError:
                i = abs(hash(pid))
            return colors[i % len(colors)]
        else:
            return colorama.Fore.CYAN

    pid = data.get(LOGGING_DATA_PID)
    lines = data.get("lines", [])

    if data.get(LOGGING_DATA_NODE_IP) == data.get("localhost"):
        for line in lines:
            print(
                "{}({}{}){} {}".format(
                    color_for(data, line),
                    prefix_for(data),
                    pid,
                    colorama.Style.RESET_ALL,
                    message_for(data, line),
                ),
                file=print_file,
            )
    else:
        for line in lines:
            print(
                "{}({}{}, ip={}){} {}".format(
                    color_for(data, line),
                    prefix_for(data),
                    pid,
                    data.get(LOGGING_DATA_NODE_IP),
                    colorama.Style.RESET_ALL,
                    message_for(data, line),
                ),
                file=print_file,
            )


def print_to_std_stream(data):
    print_file = sys.stderr if data["is_err"] else sys.stdout
    print_logs_data(data, print_file)


class PrintLogsWorker:
    def __init__(
            self, redis_address, redis_password,
            runtimes=None, node_types=None, node_ips=None):
        self.redis_address = redis_address
        self.redis_password = redis_password
        self.runtimes = runtimes
        self.node_types = node_types
        self.node_ips = node_ips

        self.filter_logs_by_runtimes = False if not runtimes else True
        self.filter_logs_by_node_types = False if not node_types else True
        self.filter_logs_by_node_ips = False if not node_ips else True

        self.print_thread = None
        self.threads_stopped = threading.Event()
        self.redis_client = create_redis_client(
            redis_address, redis_password)

    def print(self):
        global_standard_stream_dispatcher.add_handler(
            "print_logs", print_to_std_stream)
        self.print_thread = threading.Thread(
            target=self.print_logs, name="print_logs")
        self.print_thread.daemon = True
        self.print_thread.start()

    def join(self):
        if self.print_thread is None:
            return
        self.print_thread.join()

    def cancel(self):
        self.threads_stopped.set()

    def print_logs(self):
        """Prints log messages from workers on all nodes in the same job.
        """
        pubsub_client = self.redis_client.pubsub(
            ignore_subscribe_messages=True)
        pubsub_client.subscribe(LOG_FILE_CHANNEL)
        localhost = get_node_ip_address()
        try:
            # Keep track of the number of consecutive log messages that have
            # been received with no break in between. If this number grows
            # continually, then the worker is probably not able to process the
            # log messages as rapidly as they are coming in.
            num_consecutive_messages_received = 0

            while True:
                # Exit if we received a signal that we should stop.
                if self.threads_stopped.is_set():
                    return

                msg = pubsub_client.get_message()
                if msg is None:
                    num_consecutive_messages_received = 0
                    self.threads_stopped.wait(timeout=0.01)
                    continue
                num_consecutive_messages_received += 1
                if (num_consecutive_messages_received % 100 == 0
                        and num_consecutive_messages_received > 0):
                    logger.warning(
                        "The driver may not be able to keep up with the "
                        "stdout/stderr of the workers. To avoid forwarding "
                        "logs to the driver, use "
                        "'ray.init(log_to_driver=False)'.")

                data = json.loads(core_utils.decode(msg["data"]))

                if self._filtered(data):
                    continue

                data["localhost"] = localhost
                global_standard_stream_dispatcher.emit(data)

        except (OSError, redis.exceptions.ConnectionError) as e:
            logger.error(f"print_logs: {e}")
        finally:
            # Close the pubsub client to avoid leaking file descriptors.
            pubsub_client.close()

    def _filtered(self, data):
        # Don't show logs from other runtimes if filtering by runtime
        if self.filter_logs_by_runtimes:
            runtime = data.get(LOGGING_DATA_RUNTIME)
            if runtime and runtime not in self.runtimes:
                return True
        if self.filter_logs_by_node_types:
            node_type = data.get(LOGGING_DATA_NODE_TYPE)
            if node_type and node_type not in self.node_types:
                return True
        if self.filter_logs_by_node_ips:
            node_ip = data.get(LOGGING_DATA_NODE_IP)
            if node_ip and node_ip not in self.node_ips:
                return True
        return False

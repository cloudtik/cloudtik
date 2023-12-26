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

LOGGER_ID_CLUSTER_CONTROLLER = "cloudtik_cluster_controller"
LOGGER_ID_NODE_MONITOR = "cloudtik_node_monitor"


def print_logs(redis_address, redis_password, runtimes):
    runtimes = split_list(runtimes) if runtimes else None
    worker = PrintLogsWorker(
        redis_address, redis_password, runtimes)

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
        if data.get("id") in [LOGGER_ID_CLUSTER_CONTROLLER, LOGGER_ID_NODE_MONITOR]:
            return ""
        else:
            res = "id="
            return res

    def message_for(data: Dict[str, str], line: str) -> str:
        """The printed message of this log line."""
        return line

    def color_for(data: Dict[str, str], line: str) -> str:
        """The color for this log line."""
        if (
            data.get("id") == LOGGER_ID_NODE_MONITOR
        ):
            return colorama.Fore.YELLOW
        elif data.get("id") == LOGGER_ID_CLUSTER_CONTROLLER:
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
            log_id = data.get("id", 0)
            try:
                i = int(log_id)
            except ValueError:
                i = abs(hash(log_id))
            return colors[i % len(colors)]
        else:
            return colorama.Fore.CYAN

    log_id = data.get("id")
    lines = data.get("lines", [])

    if data.get("ip") == data.get("localhost"):
        for line in lines:
            print(
                "{}({}{}){} {}".format(
                    color_for(data, line),
                    prefix_for(data),
                    log_id,
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
                    log_id,
                    data.get("ip"),
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
            self, redis_address, redis_password, runtimes):
        self.redis_address = redis_address
        self.redis_password = redis_password
        self.runtimes = runtimes
        self.print_thread = None
        self.threads_stopped = threading.Event()
        self.filter_logs_by_runtimes = False if not runtimes else True
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

                # Don't show logs from other runtimes if filtering by runtime
                if (self.filter_logs_by_runtimes and data["runtime"]
                        and data["runtime"] not in self.runtimes):
                    continue
                data["localhost"] = localhost
                global_standard_stream_dispatcher.emit(data)

        except (OSError, redis.exceptions.ConnectionError) as e:
            logger.error(f"print_logs: {e}")
        finally:
            # Close the pubsub client to avoid leaking file descriptors.
            pubsub_client.close()

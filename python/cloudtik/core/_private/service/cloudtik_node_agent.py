"""Node control loop daemon."""

import argparse
import logging.handlers
import sys
import signal
import time
import traceback
import threading
from multiprocessing.synchronize import Event
from typing import Optional
import json
import psutil
import subprocess

import cloudtik
from cloudtik.core._private import constants
from cloudtik.core._private.util.core_utils import get_node_ip_address, split_list, get_process_of_pid_file
from cloudtik.core._private.util.logging_utils import setup_component_logger
from cloudtik.core._private.metrics.metrics_collector import MetricsCollector
from cloudtik.core._private.state.control_state import ControlState
from cloudtik.core._private.state.state_utils import NODE_STATE_NODE_IP, NODE_STATE_NODE_ID, NODE_STATE_NODE_KIND, \
    NODE_STATE_HEARTBEAT_TIME, NODE_STATE_NODE_TYPE, NODE_STATE_TIME, NODE_STATE_NODE_SEQ_ID
from cloudtik.core._private.utils import get_runtime_processes, make_node_id

logger = logging.getLogger(__name__)

# print every 30 minutes for repeating errors
LOG_ERROR_REPEAT_SECONDS = 30 * 60


class NodeMonitor:
    """Node Monitor for node heartbeats and node status updates
    """

    def __init__(
            self,
            node_id,
            node_ip,
            node_kind,
            node_type,
            node_seq_id,
            redis_address,
            redis_password=None,
            static_resource_list=None,
            stop_event: Optional[Event] = None,
            runtimes: str = None):
        if not node_ip:
            node_ip = get_node_ip_address()
        if node_id is None:
            node_id = make_node_id(node_ip)
        self.node_id = node_id
        self.node_ip = node_ip
        self.node_kind = node_kind
        self.node_type = node_type
        self.node_seq_id = node_seq_id

        (redis_host, redis_port) = redis_address.split(":")
        self.redis_address = redis_address
        self.redis_password = redis_password

        self.static_resource_list = static_resource_list
        # node_info store the basic aliveness of the current node
        self.node_info = {
            NODE_STATE_NODE_ID: node_id,
            NODE_STATE_NODE_IP: node_ip,
            NODE_STATE_NODE_KIND: node_kind,
        }
        if node_type:
            self.node_info[NODE_STATE_NODE_TYPE] = node_type
        if node_seq_id:
            self.node_info[NODE_STATE_NODE_SEQ_ID] = node_seq_id

        self.node_metrics = {
            NODE_STATE_NODE_ID: node_id,
            NODE_STATE_NODE_IP: node_ip,
            NODE_STATE_NODE_KIND: node_kind,
            "metrics": {},
        }
        self.old_processes = {}
        self.node_processes = {
            NODE_STATE_NODE_ID: node_id,
            NODE_STATE_NODE_IP: node_ip,
            "process": self.old_processes
        }
        self.metrics_collector = None

        # Can be used to signal graceful exit from monitor loop.
        self.stop_event = stop_event  # type: Optional[Event]

        self.control_state = ControlState()
        self.control_state.initialize_control_state(
            redis_host, redis_port, redis_password)
        self.node_table = self.control_state.get_node_table()
        self.node_processes_table = self.control_state.get_node_processes_table()
        self.node_metrics_table = self.control_state.get_node_metrics_table()

        self.processes_to_check = constants.CLOUDTIK_PROCESSES
        runtime_list = split_list(runtimes) if runtimes else None
        self.processes_to_check.extend(get_runtime_processes(runtime_list))

        logger.info("Monitor: Started")

    def _run(self):
        """Run the monitor loop."""
        self._run_heartbeat()
        self._update()

    def _update(self):
        last_error_str = None
        last_error_num = 0
        interval = constants.CLOUDTIK_UPDATE_INTERVAL_S
        log_repeat_errors = LOG_ERROR_REPEAT_SECONDS // interval
        while True:
            if self.stop_event and self.stop_event.is_set():
                break

            # Wait for update interval before processing the next
            # round of messages.
            try:
                self._update_processes()
                self._update_metrics()
                if last_error_str is not None:
                    # if this is a recover from many errors, we print a recovering message
                    if last_error_num >= log_repeat_errors:
                        logger.info(
                            "Recovering from {} repeated errors.".format(last_error_num))
                    last_error_str = None
            except Exception as e:
                error_str = str(e)
                if last_error_str != error_str:
                    logger.exception(
                        "Error happened when updating: " + error_str)
                    logger.exception(traceback.format_exc())
                    last_error_str = error_str
                    last_error_num = 1
                else:
                    last_error_num += 1
                    if last_error_num % log_repeat_errors == 0:
                        logger.error(
                            "Error happened {} times for updating: {}".format(
                                last_error_num, error_str))

            time.sleep(interval)

    def _handle_failure(self, error):
        logger.exception(
            f"Error in node monitor loop:\n{error}")

    def _signal_handler(self, sig, frame):
        logger.info(
            f"Terminated with signal {sig}")
        sys.exit(sig + 128)

    def _run_heartbeat(self):
        thread = threading.Thread(target=self._heartbeat)
        # ensure when node_monitor exits, the thread will stop automatically.
        thread.daemon = True
        thread.start()

    def _heartbeat(self):
        last_error_str = None
        last_error_num = 0
        interval = constants.CLOUDTIK_HEARTBEAT_PERIOD_SECONDS
        log_repeat_errors = LOG_ERROR_REPEAT_SECONDS // interval
        while True:
            time.sleep(interval)
            now = time.time()
            self.node_info[NODE_STATE_HEARTBEAT_TIME] = now
            node_info_as_json = json.dumps(self.node_info)
            try:
                self.node_table.put(self.node_id, node_info_as_json)
                if last_error_str is not None:
                    # if this is a recover from many errors, we print a recovering message
                    if last_error_num >= log_repeat_errors:
                        logger.info(
                            "Recovering from {} repeated errors.".format(last_error_num))
                    last_error_str = None
            except Exception as e:
                error_str = str(e)
                if last_error_str != error_str:
                    logger.exception(
                        "Error happened when heartbeat: " + str(e))
                    logger.exception(traceback.format_exc())
                    last_error_str = error_str
                    last_error_num = 1
                else:
                    last_error_num += 1
                    if last_error_num % log_repeat_errors == 0:
                        logger.error(
                            "Error happened {} times for heartbeat: {}".format(
                                last_error_num, error_str))

    def _update_processes(self):
        self._refresh_processes()
        self._publish_processes()

    def _refresh_processes(self):
        """check CloudTik runtime processes on the local machine."""
        processes_info = []
        for proc in psutil.process_iter(["name", "cmdline"]):
            try:
                processes_info.append((proc, proc.name(), proc.cmdline()))
            except psutil.Error:
                pass

        found_process = {}
        for keyword, filter_by_cmd, process_name, node_kind in self.processes_to_check:
            if (self.node_kind != node_kind) and ("node" != node_kind):
                continue

            if filter_by_cmd is None:
                # the keyword is the path to PID file
                proc = get_process_of_pid_file(keyword)
                if proc is not None:
                    found_process[process_name] = proc.status()
            else:
                # the keyword is command name or arguments
                if filter_by_cmd and len(keyword) > 15:
                    # getting here is an internal bug, so we do not use cli_logger
                    msg = ("The filter string should not be more than {} "
                           "characters. Actual length: {}. Filter: {}").format(
                        15, len(keyword), keyword)
                    raise ValueError(msg)
                found_process[process_name] = "-"
                for candidate in processes_info:
                    proc, proc_cmd, proc_args = candidate
                    corpus = (proc_cmd
                              if filter_by_cmd else subprocess.list2cmdline(proc_args))
                    if keyword in corpus:
                        found_process[process_name] = proc.status()

        if found_process != self.old_processes:
            logger.info(
                "Cloudtik processes status changed, latest process information: {}".format(
                    str(found_process)))
            self.node_processes["process"] = found_process
            self.old_processes = found_process

    def _update_metrics(self):
        self._refresh_metrics()
        self._publish_metrics()

    def _refresh_metrics(self):
        if self.metrics_collector is None:
            self.metrics_collector = MetricsCollector()

        metrics = self.metrics_collector.get_all_metrics()
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "Metrics collected for node: {}".format(metrics))
        self.node_metrics["metrics"] = metrics

    def _publish_processes(self):
        now = time.time()
        node_processes = self.node_processes
        node_processes[NODE_STATE_TIME] = now
        node_processes_as_json = json.dumps(node_processes)
        self.node_processes_table.put(self.node_id, node_processes_as_json)

    def _publish_metrics(self):
        now = time.time()
        node_metrics = self.node_metrics
        node_metrics[NODE_STATE_TIME] = now
        node_metrics_as_json = json.dumps(node_metrics)
        self.node_metrics_table.put(self.node_id, node_metrics_as_json)

    def run(self):
        # Register signal handlers for cluster scaler termination.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        try:
            self._run()
        except Exception:
            self._handle_failure(traceback.format_exc())
            raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse the arguments of the Node Monitor")
    parser.add_argument(
        "--node-id",
        required=False,
        type=str,
        default=None,
        help="The unique node id to use to for this node.")
    parser.add_argument(
        "--node-ip",
        required=False,
        type=str,
        default=None,
        help="The IP address of this node.")
    parser.add_argument(
        "--node-kind",
        required=True,
        type=str,
        help="the node kind of the current node: head or worker")
    parser.add_argument(
        "--node-type",
        required=False,
        type=str,
        default=None,
        help="the node type of the this node")
    parser.add_argument(
        "--node-seq-id",
        required=False,
        type=str,
        default=None,
        help="the node seq id of the this node")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="the address to use for Redis")
    parser.add_argument(
        "--redis-password",
        required=False,
        type=str,
        default=None,
        help="the password to use for Redis")
    parser.add_argument(
        "--logging-level",
        required=False,
        type=str,
        default=constants.LOGGER_LEVEL_INFO,
        choices=constants.LOGGER_LEVEL_CHOICES,
        help=constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
        "--logging-format",
        required=False,
        type=str,
        default=constants.LOGGER_FORMAT,
        help=constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
        "--logging-filename",
        required=False,
        type=str,
        default=constants.LOG_FILE_NAME_NODE_MONITOR,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f"\"{constants.LOG_FILE_NAME_NODE_MONITOR}\"")
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory "
        "processes.")
    parser.add_argument(
        "--logging-rotate-bytes",
        required=False,
        type=int,
        default=constants.LOGGING_ROTATE_MAX_BYTES,
        help="Specify the max bytes for rotating "
        "log file, default is "
        f"{constants.LOGGING_ROTATE_MAX_BYTES} bytes.")
    parser.add_argument(
        "--logging-rotate-backup-count",
        required=False,
        type=int,
        default=constants.LOGGING_ROTATE_BACKUP_COUNT,
        help="Specify the backup count of rotated log file, default is "
        f"{constants.LOGGING_ROTATE_BACKUP_COUNT}.")
    parser.add_argument(
        "--static_resource_list",
        required=False,
        type=str,
        default="",
        help="The static resource list of this node.")
    parser.add_argument(
        "--runtimes",
        required=False,
        type=str,
        default=None,
        help="The runtimes enabled for this cluster.")
    args = parser.parse_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=args.logging_rotate_bytes,
        backup_count=args.logging_rotate_backup_count)

    logger.info(
        f"Starting Node Monitor using CloudTik installation: {cloudtik.__file__}")
    logger.info(
        f"CloudTik version: {cloudtik.__version__}")
    logger.info(
        f"CloudTik commit: {cloudtik.__commit__}")
    logger.info(
        f"Node Monitor started with command: {sys.argv}")

    node_monitor = NodeMonitor(
        args.node_id,
        args.node_ip,
        args.node_kind,
        args.node_type,
        args.node_seq_id,
        args.redis_address,
        redis_password=args.redis_password,
        static_resource_list=args.static_resource_list,
        runtimes=args.runtimes,)

    node_monitor.run()

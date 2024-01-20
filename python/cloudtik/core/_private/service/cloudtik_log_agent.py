import argparse
import errno
import glob
import json
import logging.handlers
import os
from pathlib import Path
import platform
import re
import shutil
import time
import traceback

import cloudtik.core._private.constants as constants
import cloudtik.core._private.utils as utils
from cloudtik.core._private.cluster.cluster_logging import LOGGER_ID_CLUSTER_CONTROLLER, LOGGER_ID_NODE_MONITOR, \
    LOGGING_DATA_NODE_ID, LOGGING_DATA_NODE_IP, LOGGING_DATA_NODE_TYPE, LOGGING_DATA_PID, LOGGING_DATA_RUNTIME
from cloudtik.core._private.runtime_factory import _get_runtime_cls
from cloudtik.core._private.util.core_utils import get_node_ip_address, split_list
from cloudtik.core._private.util.logging_utils import setup_component_logger
from cloudtik.core._private.util.redis_utils import create_redis_client

# TODO (haifeng): check what is this comment about
# Logger for this module. It should be configured at the entry point
# into the program using CloudTik. It provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

# The groups are job id, and pid.
JOB_LOG_PATTERN = re.compile(".*job.*-([0-9a-f]+)-(\d+).")

# The groups are worker string pid, and ignored session id.
PID_LOG_PATTERN = re.compile("(.*)-(\d+).")

# Log name update interval under pressure.
# We need it because log name update is CPU intensive and uses 100%
# of cpu when there are many log files.
LOG_NAME_UPDATE_INTERVAL_S = float(
    os.getenv("CLOUDTIK_NAME_UPDATE_INTERVAL_S", 0.5))
# Once there are more files than this threshold,
# log monitor start giving backpressure to lower cpu usages.
LOG_MONITOR_MANY_FILES_THRESHOLD = int(
    os.getenv("CLOUDTIK_LOG_MONITOR_MANY_FILES_THRESHOLD", 1000))

# print every 30 minutes for repeating errors
LOG_ERROR_REPEAT_SECONDS = 30 * 60


def is_proc_alive(pid):
    # Import locally to make sure the bundled version is used if needed
    import psutil

    try:
        return psutil.Process(pid).is_running()
    except psutil.NoSuchProcess:
        # The process does not exist.
        return False


def _get_pid_from_log_file(file_path):
    job_match = JOB_LOG_PATTERN.match(file_path)
    if job_match:
        try:
            return int(job_match.group(2))
        except ValueError:
            pass

    # use file name without extension as pid
    log_file_stem = Path(file_path).stem
    pid_match = PID_LOG_PATTERN.match(log_file_stem)
    if pid_match:
        worker_pid = pid_match.group(1)
        if worker_pid:
            return worker_pid

    return log_file_stem


class LogFileInfo:
    def __init__(
            self,
            filename=None,
            size_when_last_opened=None,
            file_position=None,
            file_handle=None,
            is_err_file=False,
            worker_pid=None,
            runtime_name=None):
        assert (filename is not None and size_when_last_opened is not None
                and file_position is not None)
        self.filename = filename
        self.size_when_last_opened = size_when_last_opened
        self.file_position = file_position
        self.file_handle = file_handle
        self.is_err_file = is_err_file
        self.worker_pid = worker_pid
        self.runtime_name = runtime_name

    def reopen_if_necessary(self):
        """Check if the file's inode has changed and reopen it if necessary.
        There are a variety of reasons what we would logically consider a file
        would have different inodes, such as log rotation or file syncing
        semantics.
        """
        open_inode = None
        if self.file_handle and not self.file_handle.closed:
            open_inode = os.fstat(self.file_handle.fileno()).st_ino
        new_inode = os.stat(self.filename).st_ino
        if open_inode != new_inode:
            self.file_handle = open(self.filename, "rb")
            self.file_handle.seek(self.file_position)

    def __repr__(self):
        return (
            "FileInfo(\n"
            f"\tfilename: {self.filename}\n"
            f"\tsize_when_last_opened: {self.size_when_last_opened}\n"
            f"\tfile_position: {self.file_position}\n"
            f"\tfile_handle: {self.file_handle}\n"
            f"\tis_err_file: {self.is_err_file}\n"
            f"\tworker_pid: {self.worker_pid}\n"
            f"\truntime_name: {self.runtime_name}\n"
            ")"
        )


class LogMonitor:
    """A monitor process for monitoring log files.

    This class maintains a list of open files and a list of closed log files. We
    can't simply leave all files open because we'll run out of file
    descriptors.

    The "run" method of this class will cycle between doing several things:
    1. First, it will check if any new files have appeared in the log
       directory. If so, they will be added to the list of closed files.
    2. Then, if we are unable to open any new files, we will close all of the
       files.
    3. Then, we will open as many closed files as we can that may have new
       lines (judged by an increase in file size since the last time the file
       was opened).
    4. Then we will loop through the open files and see if there are any new
       lines in the file. If so, we will publish them to Redis.

    Attributes:
        node_id (str): The id of this node. Typical the stable host name if available
        node_ip (str): The ip address of this node.
        node_type (str): The node type of this node.
        logs_dir (str): The directory that the log files are in.
        redis_client: A client used to communicate with the Redis server.
        log_filenames (set): This is the set of filenames of all files in
            open_file_infos and closed_file_infos.
        open_file_infos (list[LogFileInfo]): Info for all of the open files.
        closed_file_infos (list[LogFileInfo]): Info for all of the closed
            files.
        can_open_more_files (bool): True if we can still open more files and
            false otherwise.
    """

    def __init__(
            self,
            node_id,
            node_ip,
            node_type,
            logs_dir,
            redis_address,
            redis_password=None,
            runtimes=None,
            max_files_open: int = constants.LOG_MONITOR_MAX_OPEN_FILES,
    ):
        """Initialize the log monitor object."""
        if not node_ip:
            node_ip = get_node_ip_address()
        if node_id is None:
            node_id = utils.make_node_id(node_ip)
        self.node_id = node_id
        self.node_ip = node_ip
        self.node_type = node_type

        self.logs_dir = logs_dir
        self.redis_client = create_redis_client(
            redis_address, password=redis_password)
        self.runtimes = split_list(runtimes) if runtimes else None
        self.log_filenames = set()
        self.open_file_infos = []
        self.closed_file_infos = []
        self.can_open_more_files = True
        self.max_files_open: int = max_files_open
        self.runtime_logs = self._get_runtime_log_dirs()

    def _get_runtime_log_dirs(self):
        if not self.runtimes:
            return None

        # Iterate through all the runtimes
        runtime_log_dirs = {}
        for runtime_type in self.runtimes:
            runtime_cls = _get_runtime_cls(runtime_type)
            runtime_logs = runtime_cls.get_logs()
            if not runtime_logs:
                continue
            log_dirs = []
            for category in runtime_logs:
                log_dir = runtime_logs[category]
                log_dirs += [log_dir]
            runtime_log_dirs[runtime_type] = log_dirs
        return runtime_log_dirs

    def _close_all_files(self):
        """Close all open files (so that we can open more)."""
        while len(self.open_file_infos) > 0:
            file_info = self.open_file_infos.pop(0)
            file_info.file_handle.close()
            file_info.file_handle = None
            proc_alive = True
            # Test if the worker process that generated the log file
            # is still alive. Only applies to worker processes.
            # For all other system components, we always assume they are alive.
            if (
                    file_info.runtime_name is None
                    and file_info.worker_pid != LOGGER_ID_NODE_MONITOR
                    and file_info.worker_pid != LOGGER_ID_CLUSTER_CONTROLLER
                    and file_info.worker_pid is not None
                    and not isinstance(file_info.worker_pid, str)
            ):
                proc_alive = is_proc_alive(file_info.worker_pid)
                if not proc_alive:
                    # TODO: handle completed job log
                    # The process is not alive any more, so move the log file
                    # out of the log directory so glob.glob will not be slowed
                    # by it.
                    target = os.path.join(
                        self.logs_dir, "old", os.path.basename(file_info.filename)
                    )
                    try:
                        shutil.move(file_info.filename, target)
                    except (IOError, OSError) as e:
                        if e.errno == errno.ENOENT:
                            logger.warning(
                                f"Warning: The file {file_info.filename} was not found."
                            )
                        else:
                            raise e

            if proc_alive:
                self.closed_file_infos.append(file_info)

        self.can_open_more_files = True

    def update_log_filenames(self):
        """Update the list of log files to monitor."""
        system_log_paths = []
        # segfaults and other serious errors are logged here
        system_log_paths += glob.glob(
            f"{self.logs_dir}/cloudtik_node_monitor*[.log|.out|.err]")
        # monitor logs are needed to report cluster scaler events
        system_log_paths += glob.glob(
            f"{self.logs_dir}/cloudtik_cluster_controller*[.log|.out|.err]")

        self._update_log_paths(
            system_log_paths, runtime_name=constants.CLOUDTIK_RUNTIME_NAME)

        self._update_runtime_log_paths()

        # user job logs are here
        user_log_paths = []
        user_logs_dir = os.path.expanduser("~/user/logs")
        user_log_paths += glob.glob(f"{user_logs_dir}/*.log")

        self._update_log_paths(
            user_log_paths, runtime_name=None)

    def _update_runtime_log_paths(self):
        if not self.runtime_logs:
            return
        for runtime_type, log_dirs in self.runtime_logs.items():
            log_paths = []
            for log_dir in log_dirs:
                log_paths += glob.glob(f"{log_dir}/*.log")
            self._update_log_paths(
                log_paths, runtime_name=runtime_type)

    def _update_log_paths(self, log_paths, runtime_name):
        for file_path in log_paths:
            if os.path.isfile(
                    file_path) and file_path not in self.log_filenames:
                is_err_file = file_path.endswith("err")
                worker_pid = _get_pid_from_log_file(file_path)

                self.log_filenames.add(file_path)
                self.closed_file_infos.append(
                    LogFileInfo(
                        filename=file_path,
                        size_when_last_opened=0,
                        file_position=0,
                        file_handle=None,
                        is_err_file=is_err_file,
                        worker_pid=worker_pid,
                        runtime_name=runtime_name))
                log_filename = os.path.basename(file_path)
                logger.info(f"Beginning to track file {log_filename}")

    def open_closed_files(self):
        """Open some closed files if they may have new lines.

        Opening more files may require us to close some of the already open
        files.
        """
        if not self.can_open_more_files:
            # If we can't open any more files. Close all of the files.
            self._close_all_files()

        files_with_no_updates = []
        while len(self.closed_file_infos) > 0:
            if len(self.open_file_infos) >= self.max_files_open:
                self.can_open_more_files = False
                break

            file_info = self.closed_file_infos.pop(0)
            assert file_info.file_handle is None
            # Get the file size to see if it has gotten bigger since we last
            # opened it.
            try:
                file_size = os.path.getsize(file_info.filename)
            except (IOError, OSError) as e:
                # Catch "file not found" errors.
                if e.errno == errno.ENOENT:
                    logger.warning(
                        f"Warning: The file {file_info.filename} "
                        "was not found.")
                    self.log_filenames.remove(file_info.filename)
                    continue
                raise e

            # If some new lines have been added to this file, try to reopen the
            # file.
            if file_size > file_info.size_when_last_opened:
                try:
                    f = open(file_info.filename, "rb")
                except (IOError, OSError) as e:
                    if e.errno == errno.ENOENT:
                        logger.warning(
                            f"Warning: The file {file_info.filename} "
                            "was not found.")
                        self.log_filenames.remove(file_info.filename)
                        continue
                    else:
                        raise e

                f.seek(file_info.file_position)
                file_info.size_when_last_opened = file_size
                file_info.file_handle = f
                self.open_file_infos.append(file_info)
            else:
                files_with_no_updates.append(file_info)

        if len(self.open_file_infos) >= self.max_files_open:
            self.can_open_more_files = False
        # Add the files with no changes back to the list of closed files.
        self.closed_file_infos += files_with_no_updates

    def check_log_files_and_publish_updates(self):
        """Get any changes to the log files and push updates to Redis.

        Returns:
            True if anything was published and false otherwise.
        """
        anything_published = False
        lines_to_publish = []

        def flush():
            nonlocal lines_to_publish
            nonlocal anything_published
            if len(lines_to_publish) > 0:
                data = {
                    LOGGING_DATA_NODE_ID: self.node_id,
                    LOGGING_DATA_NODE_IP: self.node_ip,
                    LOGGING_DATA_NODE_TYPE: self.node_type,
                    LOGGING_DATA_PID: file_info.worker_pid,
                    LOGGING_DATA_RUNTIME: file_info.runtime_name,
                    "is_err": file_info.is_err_file,
                    "lines": lines_to_publish,
                }
                self.redis_client.publish(constants.LOG_FILE_CHANNEL,
                                          json.dumps(data))
                anything_published = True
                lines_to_publish = []

        for file_info in self.open_file_infos:
            assert not file_info.file_handle.closed
            file_info.reopen_if_necessary()

            max_num_lines_to_read = constants.LOG_MONITOR_NUM_LINES_TO_READ
            for _ in range(max_num_lines_to_read):
                try:
                    next_line = file_info.file_handle.readline()
                    # Replace any characters not in UTF-8 with
                    # a replacement character, see
                    # https://stackoverflow.com/a/38565489/10891801
                    next_line = next_line.decode("utf-8", "replace")
                    if next_line == "":
                        break
                    next_line = next_line.rstrip("\r\n")

                    lines_to_publish.append(next_line)
                except Exception:
                    logger.error(
                        f"Error: Reading file: {file_info.filename}, "
                        f"position: {file_info.file_info.file_handle.tell()} "
                        "failed.")
                    raise

            # TODO (haifeng) : correct and add the processes we will have
            if file_info.file_position == 0:
                if "/cloudtik_node_monitor" in file_info.filename:
                    file_info.worker_pid = LOGGER_ID_NODE_MONITOR
                elif "/cloudtik_cluster_controller" in file_info.filename:
                    file_info.worker_pid = LOGGER_ID_CLUSTER_CONTROLLER

            # Record the current position in the file.
            file_info.file_position = file_info.file_handle.tell()
            flush()

        return anything_published

    def should_update_filenames(self, last_file_updated_time: float) -> bool:
        """Return true if filenames should be updated.
        This method is used to apply the backpressure on file updates because
        that requires heavy glob operations which use lots of CPUs.
        Args:
            last_file_updated_time: The last time filenames are updated.
        Returns:
            True if filenames should be updated. False otherwise.
        """
        elapsed_seconds = float(time.time() - last_file_updated_time)
        return (
                len(self.log_filenames) < LOG_MONITOR_MANY_FILES_THRESHOLD
                or elapsed_seconds > LOG_NAME_UPDATE_INTERVAL_S
        )

    def run(self):
        """Run the log monitor.

        This will query Redis once every second to check if there are new log
        files to monitor. It will also store those log files in Redis.
        """

        last_updated = time.time()
        last_error_str = None
        last_error_num = 0
        interval = 1
        log_repeat_errors = LOG_ERROR_REPEAT_SECONDS // interval
        while True:
            if self.should_update_filenames(last_updated):
                self.update_log_filenames()
                last_updated = time.time()

            self.open_closed_files()

            try:
                anything_published = self.check_log_files_and_publish_updates()
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
                        "Error happened when publishing: " + error_str)
                    logger.exception(traceback.format_exc())
                    last_error_str = error_str
                    last_error_num = 1
                else:
                    last_error_num += 1
                    if last_error_num % log_repeat_errors == 0:
                        logger.error(
                            "Error happened {} times for updating: {}".format(
                                last_error_num, error_str))
                # if there is error, wait for some time
                time.sleep(interval)
            else:
                # If nothing was published, then wait a little bit before checking
                # for logs to avoid using too much CPU.
                if not anything_published:
                    time.sleep(0.1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=("Parse Redis server for the "
                     "log monitor to connect "
                     "to."))
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
        "--node-type",
        required=False,
        type=str,
        default=None,
        help="the node type of the this node")
    parser.add_argument(
        "--redis-address",
        required=True,
        type=str,
        help="The address to use for Redis.")
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
        default=constants.LOG_FILE_NAME_LOG_MONITOR,
        help="Specify the name of log file, "
        "log to stdout if set empty, default is "
        f"\"{constants.LOG_FILE_NAME_LOG_MONITOR}\"")
    parser.add_argument(
        "--logs-dir",
        required=True,
        type=str,
        help="Specify the path of the temporary directory used by cluster "
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

    log_monitor = LogMonitor(
        args.node_id,
        args.node_ip,
        args.node_type,
        args.logs_dir,
        args.redis_address,
        redis_password=args.redis_password,
        runtimes=args.runtimes)

    try:
        log_monitor.run()
    except Exception as e:
        # Something went wrong
        redis_client = create_redis_client(
            args.redis_address, password=args.redis_password)
        traceback_str = utils.format_error_message(
            traceback.format_exc())
        message = (f"The log monitor on node {platform.node()} "
                   f"failed with the following error:\n{traceback_str}")
        utils.publish_error(
            constants.ERROR_CLUSTER_CONTROLLER_DIED,
            message,
            redis_client=redis_client)
        logger.error(message)
        raise e

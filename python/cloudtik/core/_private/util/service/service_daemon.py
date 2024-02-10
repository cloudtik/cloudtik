import os
import sys
from shlex import quote

from cloudtik.core._private import constants
from cloudtik.core._private.cli_logger import cli_logger
from cloudtik.core._private.constants import SESSION_LATEST
from cloudtik.core._private.util.core_utils import get_cloudtik_temp_dir, check_process_exists, stop_process_tree, \
    get_named_log_file_handles, get_cloudtik_home_dir, create_shared_directory, write_pid_file, read_pid_from_pid_file
from cloudtik.core._private.services import start_cloudtik_process

SERVICE_DAEMON_PATH = os.path.abspath(os.path.dirname(__file__))
PROCESS_SERVICE_DAEMON = "cloudtik_service_daemon"


def _get_logging_name(identifier):
    return identifier


def service_daemon(
        identifier, command,
        service_class, pull_script,
        service_args,
        logs_dir=None, redirect_output=True):
    if not identifier:
        raise ValueError(
            " identifier cannot be empty.")
    if command == "start":
        if not service_class and not pull_script:
            raise ValueError(
                "You need either specify a service class or a pull script.")
        start_service_daemon(
            identifier, service_class, pull_script, service_args,
            logs_dir=logs_dir,
            redirect_output=redirect_output)
    elif command == "stop":
        _stop_service_daemon(identifier)
    else:
        raise ValueError(
            "Invalid command parameter: {}".format(command))


def get_service_daemon_process_file(identifier: str):
    return os.path.join(
        get_cloudtik_temp_dir(), "cloudtik-service-{}".format(identifier))


def get_service_daemon_pid(process_file: str):
    pid = read_pid_from_pid_file(process_file)
    if pid is None:
        return None
    if not check_process_exists(pid):
        return None
    return pid


def _start_service_daemon(
        identifier,
        service_class,
        pull_script,
        service_args,
        logs_dir,
        logging_name,
        stdout_file=None,
        stderr_file=None,
        logging_level=None,
        max_bytes=0,
        backup_count=0):
    """Run a process to controller the other processes.

    Args:
        identifier (str): The identifier of the service.
        service_class (str): The service runner module and class.
        pull_script (str): The puller script file.
        service_args(List[str]): The list of arguments pass to the service runner.
        logs_dir(str): The path to the log directory.
        stdout_file: A file handle opened for writing to redirect stdout to. If
            no redirection should happen, then this should be None.
        stderr_file: A file handle opened for writing to redirect stderr to. If
            no redirection should happen, then this should be None.
        logging_level (str): The logging level to use for the process.
        max_bytes (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's maxBytes.
        backup_count (int): Log rotation parameter. Corresponding to
            RotatingFileHandler's backupCount.
    Returns:
        ProcessInfo for the process that was started.
    """
    service_daemon_path = os.path.join(SERVICE_DAEMON_PATH, "cloudtik_service_daemon.py")
    command = [
        sys.executable,
        "-u",
        service_daemon_path,
        f"--logs-dir={logs_dir}",
        f"--logging-rotate-bytes={max_bytes}",
        f"--logging-rotate-backup-count={backup_count}",
        f"--logging-filename={logging_name}.log",
    ]
    if logging_level:
        command.append("--logging-level=" + logging_level)

    command.append("--identifier=" + identifier)
    if service_class:
        command.append("--service-class=" + quote(service_class))
    if pull_script:
        command.append("--pull-script=" + quote(pull_script))
    if service_args:
        command += list(service_args)

    process_info = start_cloudtik_process(
        command,
        PROCESS_SERVICE_DAEMON,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        fate_share=False)
    return process_info


def start_service_daemon(
        identifier,
        service_class, pull_script,
        service_args,
        logs_dir=None, redirect_output=True):
    service_daemon_process_file = get_service_daemon_process_file(identifier)
    pid = get_service_daemon_pid(service_daemon_process_file)
    if pid is not None:
        cli_logger.print(
            "The service daemon for {} is already running. "
            "If you want to restart, stop it first and start.", identifier)
        return

    if not logs_dir:
        home_dir = get_cloudtik_home_dir()
        logs_dir = os.path.join(home_dir, SESSION_LATEST, "logs")

    create_shared_directory(logs_dir)

    logging_name = _get_logging_name(identifier)
    stdout_file, stderr_file = get_named_log_file_handles(
        logs_dir, logging_name,
        redirect_output=redirect_output)

    # Configure log parameters.
    logging_level = os.getenv(
        constants.CLOUDTIK_LOGGING_LEVEL_ENV,
        constants.LOGGER_LEVEL_INFO)
    max_bytes = int(
        os.getenv(
            constants.CLOUDTIK_LOGGING_ROTATE_MAX_BYTES_ENV,
            constants.LOGGING_ROTATE_MAX_BYTES))
    backup_count = int(
        os.getenv(
            constants.CLOUDTIK_LOGGING_ROTATE_BACKUP_COUNT_ENV,
            constants.LOGGING_ROTATE_BACKUP_COUNT))

    process_info = _start_service_daemon(
        identifier,
        service_class,
        pull_script,
        service_args,
        logs_dir,
        logging_name,
        stdout_file=stdout_file,
        stderr_file=stderr_file,
        logging_level=logging_level,
        max_bytes=max_bytes,
        backup_count=backup_count)

    pid = process_info.process.pid
    write_pid_file(service_daemon_process_file, pid)

    cli_logger.print(
        "Successfully started service daemon: {}".format(identifier))


def _stop_service_daemon(identifier):
    # find the pid file and stop it
    service_daemon_process_file = get_service_daemon_process_file(identifier)
    pid = get_service_daemon_pid(service_daemon_process_file)
    if pid is None:
        cli_logger.print(
            "The service daemon for {} was not started.", identifier)
        return

    stop_process_tree(pid)
    os.remove(service_daemon_process_file)
    cli_logger.print(
        "Successfully stopped service daemon of {}.", identifier)

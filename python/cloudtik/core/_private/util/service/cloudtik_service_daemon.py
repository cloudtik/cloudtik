"""Pull server daemon."""

import argparse
import logging.handlers
import signal
import sys
import traceback
from multiprocessing.synchronize import Event
from typing import Optional

import cloudtik
from cloudtik.core._private import constants
from cloudtik.core._private.util.core_utils import load_class
from cloudtik.core._private.util.logging_utils import setup_component_logger
from cloudtik.core._private.util.service.pull_job import ScriptPullJob, PULL_INTERVAL_ARGUMENT_NAME
from cloudtik.core._private.util.service.service_daemon import PROCESS_SERVICE_DAEMON
from cloudtik.core._private.util.service.service_runner import cmd_args_to_call_args

logger = logging.getLogger(__name__)

LOG_FILE_NAME_SERVICE_DAEMON = f"{PROCESS_SERVICE_DAEMON}.log"


class ServiceDaemon:
    """Service daemon to run a task such as pulla ing task with a specific interval.
    The service task can be in the form of a Service Runner class a python module, or
    if it is pulling, a python script, or shell script to run.
    """

    def __init__(
            self,
            identifier,
            service_class, pull_script,
            service_args=None,
            stop_event: Optional[Event] = None):
        self.identifier = identifier
        self.service_class = service_class
        self.pull_script = pull_script
        self.service_args = service_args

        # Can be used to signal graceful exit from main loop.
        self.stop_event = stop_event  # type: Optional[Event]

        self.service_runner = self._create_service_runner()
        self.service_runner.stop_event = stop_event

        logger.info(
            "Service {}: Started".format(identifier))

    def _create_service_runner(self):
        args, kwargs = cmd_args_to_call_args(self.service_args)
        if self.service_class:
            service_runner_cls = load_class(self.service_class)
            return service_runner_cls(*args, **kwargs)
        else:
            interval = kwargs.get(PULL_INTERVAL_ARGUMENT_NAME)
            return ScriptPullJob(
                interval, self.pull_script, self.service_args)

    def _handle_failure(self, error):
        logger.exception(
            f"Error in service loop:\n{error}")

    def _signal_handler(self, sig, frame):
        logger.info(
            f"Terminated with signal {sig}")
        sys.exit(sig + 128)

    def run(self):
        # Register signal handlers for service daemon termination.
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        try:
            self.service_runner.run()
        except Exception:
            self._handle_failure(traceback.format_exc())
            raise


def main():
    parser = argparse.ArgumentParser(
        description="Parse the arguments of Service")
    parser.add_argument(
        "--identifier",
        required=True,
        type=str,
        help="The identifier of this pull instance.")
    parser.add_argument(
        "--service-class",
        required=False,
        type=str,
        help="The python module and class to run.")
    parser.add_argument(
        "--pull-script",
        required=False,
        type=str,
        help="The bash script or python script to run for pulling.")
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
        default=LOG_FILE_NAME_SERVICE_DAEMON,
        help="Specify the name of log file, "
             "log to stdout if set empty, default is "
             f"\"{LOG_FILE_NAME_SERVICE_DAEMON}\"")
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
    args, argv = parser.parse_known_args()
    setup_component_logger(
        logging_level=args.logging_level,
        logging_format=args.logging_format,
        log_dir=args.logs_dir,
        filename=args.logging_filename,
        max_bytes=args.logging_rotate_bytes,
        backup_count=args.logging_rotate_backup_count)

    logger.info(
        f"Starting Service daemon using CloudTik installation: {cloudtik.__file__}")
    logger.info(
        f"CloudTik version: {cloudtik.__version__}")
    logger.info(
        f"CloudTik commit: {cloudtik.__commit__}")
    logger.info(
        f"Service daemon started with command: {sys.argv}")

    service_daemon = ServiceDaemon(
        args.identifier,
        args.service_class,
        args.pull_script,
        service_args=argv,
    )

    service_daemon.run()


if __name__ == "__main__":
    main()

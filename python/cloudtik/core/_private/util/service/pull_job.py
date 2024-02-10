import logging
import subprocess
import time

from cloudtik.core._private.util.service.service_runner import ServiceRunner
from cloudtik.core._private.utils import get_run_script_command

logger = logging.getLogger(__name__)

PULL_INTERVAL_ARGUMENT_NAME = "interval"
DEFAULT_PULL_INTERVAL = 10

# print every 30 minutes for repeating errors
LOG_ERROR_REPEAT_SECONDS = 30 * 60


class PullJob(ServiceRunner):
    def __init__(self, interval=None):
        super().__init__()
        self.interval = interval
        if not self.interval:
            self.interval = DEFAULT_PULL_INTERVAL

    def run(self):
        last_error_str = None
        last_error_num = 0
        interval = self.interval
        log_repeat_errors = LOG_ERROR_REPEAT_SECONDS // interval
        while True:
            if self.stop_event and self.stop_event.is_set():
                break

            try:
                self.pull()
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
                        "Error happened when pulling: " + error_str)
                    last_error_str = error_str
                    last_error_num = 1
                else:
                    last_error_num += 1
                    if last_error_num % log_repeat_errors == 0:
                        logger.error(
                            "Error happened {} times for pulling: {}".format(
                                last_error_num, error_str))
            time.sleep(interval)

    def pull(self):
        pass


class ScriptPullJob(PullJob):
    def __init__(
            self,
            interval=None,
            pull_script=None,
            service_args=None):
        super().__init__(interval)
        self.pull_script = pull_script
        self.service_args = service_args
        self.pull_cmd = get_run_script_command(
            self.pull_script, self.service_args)

    def pull(self):
        try:
            subprocess.check_call(self.pull_cmd, shell=True)
        except subprocess.CalledProcessError as err:
            raise err

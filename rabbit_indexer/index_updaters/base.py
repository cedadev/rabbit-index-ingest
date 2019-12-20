
from datetime import datetime
import logging
import time
import os


class UpdateHandler:

    def __init__(self, path_tools, conf, refresh_interval=30):

        # Initialise update counter
        self.update_time = datetime.now()
        self.refresh_interval = refresh_interval * 60 # convert to seconds

        # Initialise Path Tools
        self.pt = path_tools

        # Setup logging
        self.logger = logging.getLogger()
        logging_level = conf.get('logging', 'log-level')
        self.logger.setLevel(getattr(logging, logging_level.upper()))

    def _update_mappings(self):
        """
        Need to make sure that the code is using the most up to date mapping, either in
        MOLES or the spot mapping.
        :return:
        """

        timedelta = (datetime.now() - self.update_time).seconds

        # Refresh if ∆t is greater than the refresh interval and another thread has not already
        # picked up the task.
        if timedelta > self.refresh_interval:
            self.logger.info('Refreshing mappings')

            successful = self.pt.update_mapping()

            # Reset timer if mapping update successful
            if successful:
                self.update_time = datetime.now()

    @staticmethod
    def _wait_for_file(message):

        if isinstance(message, str):
            if not os.path.exists(message.filepath):
                time.sleep(60)

            return


        timestamp = datetime.strptime(message.datetime,'%Y-%m-%d %H:%M:%S')

        t_delta = datetime.now() - timestamp

        if t_delta.seconds < 300:

            if not os.path.exists(message.filepath):
                time.sleep(60)

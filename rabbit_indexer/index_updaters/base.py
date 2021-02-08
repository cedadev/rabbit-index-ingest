

# Python Imports
from datetime import datetime
import logging
import time
import os
from dateutil.parser import parse

# Typing imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from rabbit_indexer.utils import PathTools
    from configparser import RawConfigParser
    from rabbit_indexer.queue_handler.queue_handler import IngestMessage


class UpdateHandler:
    """
    Base class for file/directory based rabbitMQ messages which are used to update
    the CEDA files indices.
    """

    def __init__(self, path_tools: 'PathTools', conf: 'RawConfigParser', refresh_interval: int = 30) -> None:
        """

        :param path_tools: rabbit_indexer.utils.PathTools object
        :param conf:
        :param refresh_interval: Time interval to refresh the MOLES and Spot mapping in minutes
        """

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
        MOLES or the spot mapping. This method updates the mappings on disk.
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
    def _wait_for_file(message: 'IngestMessage'):
        """
        There can be a time delay from the deposit message arriving at the server
        to the file being visible via the storage technology and indexing. This method updates
        puts a short wait in which will wait for the file to appear on disk.

        :param message: A rabbit_indexer.queue_handler.IngestMessage
        """

        timestamp = parse(message.datetime)

        t_delta = datetime.now() - timestamp

        if t_delta.seconds < 300:

            if not os.path.exists(message.filepath):
                time.sleep(60)

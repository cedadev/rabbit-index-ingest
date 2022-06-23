

# Python Imports
from datetime import datetime
import logging
import time
import os
from dateutil.parser import parse
from abc import ABC, abstractmethod
from rabbit_indexer.utils import PathTools

# Typing imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from rabbit_indexer.utils.yaml_config import YamlConfig
    from rabbit_indexer.queue_handler.queue_handler import IngestMessage


class UpdateHandler(ABC):
    """
    Abstract Base class for file/directory based rabbitMQ messages which are used to update
    the CEDA files indices.
    """

    def __init__(self, conf: 'YamlConfig', **kwargs) -> None:
        """

        :param conf: Configuration file
        """
        self.conf = conf
        self.pt = None
        self._setup_logging()
        self.logger.info('Initialising rabbitmq consumer')
        self.setup_extra(**kwargs)

    def _setup_logging(self):
        """
        Setup logging handler
        """

        log_level_str = self.conf.get('logging', 'log_level', default='info')
        log_level = getattr(logging, log_level_str.upper())
        
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

    def setup_extra(self, refresh_interval: int = 30, **kwargs):
        """
        Setup extra properties for the handler
        """

        # Initialise update counter
        self.logger.info('Initialising update counter with refresh_interval: {}'.format(refresh_interval))
        self.update_time = datetime.now()
        self.refresh_interval = refresh_interval * 60 # convert to seconds

        # Initialise Path Tools
        moles_obs_map_url = self.conf.get("moles", "moles_obs_map_url")

        self.logger.info('Downloading MOLES mapping')
        path_tools = PathTools(moles_mapping_url=moles_obs_map_url)
        self.pt = path_tools

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
    def _wait_for_file(message: 'IngestMessage', wait_time: int = 300):
        """
        There can be a time delay from the deposit message arriving at the server
        to the file being visible via the storage technology and indexing. This method updates
        puts a short wait in which will wait for the file to appear on disk.

        :param message: A rabbit_indexer.queue_handler.IngestMessage
        """

        timestamp = parse(message.datetime)

        t_delta = datetime.now() - timestamp

        if t_delta.seconds < wait_time:

            if not os.path.exists(message.filepath):
                time.sleep(60)

    @abstractmethod
    def process_event(self, message: 'IngestMessage') -> None:
        """
        Processing the message according to the action within the message

        :param message: The parsed rabbitMQ message
        """
        pass

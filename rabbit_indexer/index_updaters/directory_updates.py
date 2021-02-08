# encoding: utf-8
"""
Maintain the ceda directories listing
"""
__author__ = 'Richard Smith'
__date__ = '11 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

# Local imports
from rabbit_indexer.index_updaters.base import UpdateHandler
from rabbit_indexer.utils.decorators import wait_for_file

# Third-party imports
from ceda_elasticsearch_tools.index_tools import CedaDirs

# Python imports
import os

# Typing imports
from rabbit_indexer.queue_handler.queue_handler import IngestMessage
from rabbit_indexer.utils import PathTools
from configparser import RawConfigParser


class DirectoryUpdateHandler(UpdateHandler):
    """
    Handler to update the ceda-dirs directory index based on messages in the
    rabbit queue
    """

    def __init__(self, path_tools: PathTools, conf: RawConfigParser, refresh_interval: int = 30) -> None:
        """

        :param path_tools: An initialised rabbit_indexer.utils.PathTools object
        :param conf: A read configparser.ConfigParser object
        :param refresh_interval: Time in minutes before refreshing the mappings
        """
        super().__init__(path_tools, conf, refresh_interval)

        # Initialise the Elasticsearch connection
        self.index_updater = CedaDirs(
            index=conf.get('directory-index', 'es-index'),
            **{'headers': {
                'x-api-key': conf.get('elasticsearch', 'es-api-key')
            },
                'retry_on_timeout': True,
                'timeout': 30
            }
        )

    def process_event(self, message: IngestMessage):
        """
        Takes the events from rabbit and sends them to the appropriate processor

        :param message: rabbitMQ message
        """

        self.logger.info(f'{message.filepath}:{message.action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        # Send the message to the appropriate processor method
        if message.action == 'MKDIR':
            self._process_creations(message)

        elif message.action == 'RMDIR':
            self._process_deletions(message.filepath)

        elif message.action == 'SYMLINK':
            self._process_symlinks(message)

        elif message.action == '00README':
            self._process_readmes(message.filepath)

    def _process_creations(self, message: IngestMessage):
        """
        Process the creation of a new directory

        :param path: Directory path
        """

        self._wait_for_file(message)

        # Get the metadata
        metadata, _ = self.pt.generate_path_metadata(message.filepath)

        # Index new directory
        if metadata:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(message.filepath),
                        'document': metadata
                    }
                ]
            )

    def _process_deletions(self, path: str):
        """
        Process the deletion of a directory

        :param path: Directory path
        """

        # Delete directory
        self.index_updater.delete_dirs(
            [
                {
                    "id": self.pt.generate_id(path)
                }
            ]
        )

    def _process_symlinks(self, message: IngestMessage):
        """
        Method to make it explicit what action is being
        performed but the actual code to run is the same
        as for creations.

        :param message: parsed message from RabbitMQ
        """

        self._process_creations(message)

    @wait_for_file
    def _process_readmes(self, path: str):
        """
        Process the addition of a 00README file

        :param path: Path to the readme
        """

        # Get the directory containing the 00README
        path = os.path.dirname(path)

        # Get the content of the 00README
        content = self.pt.get_readme(path)

        if content:
            self.index_updater.update_readmes(
                [
                    {
                        "id": self.pt.generate_id(path),
                        "document": {"readme": content}
                    }
                ]
            )


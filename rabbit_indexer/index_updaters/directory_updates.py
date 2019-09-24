# encoding: utf-8
"""
Maintain the ceda directories listing
"""
__author__ = 'Richard Smith'
__date__ = '11 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools import CedaDirs
import os
from rabbit_indexer.index_updaters.base import UpdateHandler
from time import sleep
from rabbit_indexer.utils.decorators import wait_for_file

class DirectoryUpdateHandler(UpdateHandler):

    def __init__(self, path_tools, conf, refresh_interval=30):
        """

        :param path_tools: An initialised PathTools object
        :param refresh_interval: Time in minutes before refreshing the mappings
        """
        super().__init__(path_tools, conf, refresh_interval)

        # Initialise the Elasticsearch connection
        self.index_updater = CedaDirs(
            index=conf.get('directory-index', 'es-index'),
            host_url=conf.get('elasticsearch', 'es-host'),
            **{'http_auth': (
                conf.get('elasticsearch', 'es-user'),
                conf.get('elasticsearch', 'es-password')
            ),
                'retry_on_timeout': True,
                'timeout': 30
            }
        )

    def process_event(self, body):
        """
        Takes the events from rabbit and sends them to the appropriate processor

        :param action: Deposit Action. Can be one of: MKDIR|RMDIR|SYMLINK|00README
        :param path: The directory or readme path to process

        """

        message = self._decode_message(body)

        self.logger.info(f'{message.filepath}:{message.action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        # Send the message to the appropriate processor method
        if message.action == 'MKDIR':
            self._process_creations(message.filepath)

        elif message.action == 'RMDIR':
            self._process_deletions(message.filepath)

        elif message.action == 'SYMLINK':
            self._process_symlinks(message.filepath)

        elif message.action == '00README':
            self._process_readmes(message.filepath)

    @wait_for_file
    def _process_creations(self, path):
        """
        Process the creation of a new directory

        :param path: Directory path
        """

        # Check if directory exists.
        if not os.path.exists(path):
            sleep(60)

        # Get the metadata
        metadata, _ = self.pt.generate_path_metadata(path)

        # Index new directory
        if metadata:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(path),
                        'document': metadata
                    }
                ]
            )


    def _process_deletions(self, path):
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

    def _process_symlinks(self, path):
        """
        Method to make it explicit what action is being
        performed but the actual code to run is the same
        as for creations.

        :param path: filepath
        """

        self._process_creations(path)

    @wait_for_file
    def _process_readmes(self, path):
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


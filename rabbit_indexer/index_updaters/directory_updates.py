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
            )}
        )


    def process_event(self, path, action):
        """
        Takes the events from rabbit and sends them to the appropriate processor

        :param action: Deposit Action. Can be one of: MKDIR|RMDIR|SYMLINK|00README
        :param path: The directory or readme path to process

        """
        self.logger.info(f'{path}:{action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        # Send the message to the appropriate processor method
        if action == 'MKDIR':
            self._process_creations(path)

        elif action == 'RMDIR':
            self._process_deletions(path)

        elif action == 'SYMLINK':

            self._process_symlinks(path)
        elif action == '00README':

            self._process_readmes(path)


    def _process_creations(self, path):
        """
        Process the creation of a new directory

        :param path: Directory path
        """

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
        Process the creation of a symlinked directory

        :param path: Directory path
        """

        # Get the metadata
        metadata, _ = self.pt.generate_path_metadata(path)

        # Index the symlink
        if metadata:
            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(path),
                        'document': metadata
                    }
                ]
            )

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

    # def _process_spot_roots(self):
    #     """
    #     Periodically run an indexing job on the top level as these are not picked up in the
    #     normal way. This is run when a new spot mapping is downloaded.
    #     """
    #
    #     spot_paths = self.pt.spots.path2spotmapping
    #
    #     content_list = []
    #     for spot in spot_paths:
    #
    #         metadata, _ = self.pt.generate_path_metadata(spot)
    #
    #         if metadata:
    #             content_list.append({
    #                 "id": self.pt.generate_id(spot),
    #                 "document": metadata
    #             })
    #
    #     self.index_updater.add_dirs(content_list)

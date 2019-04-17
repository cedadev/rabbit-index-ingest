# encoding: utf-8
"""
Maintain the ceda directories listing
"""
__author__ = 'Richard Smith'
__date__ = '11 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools.index_updaters import CedaDirs
from configparser import ConfigParser
from datetime import datetime
from utils.path_tools import PathTools
import hashlib
import os


class DirectoryUpdateHandler:

    def __init__(self, refresh_interval=30):

        # Read in the config file
        conf = ConfigParser().read('index_updater.ini')

        # Convert refresh interval to seconds
        self.refresh_interval = refresh_interval * 60

        # Initialise the Elasticsearch connection
        self.index_updater = CedaDirs(
            index=conf.get('elasticsearch', 'es-index'),
            host_url=conf.get('elasticsearch', 'es-host'),
            **{'http_auth': (
                conf.get('elasticsearch', 'es-user'),
                conf.get('elasticsearch', 'es-password')
            )}
        )

        # Initialise update counter
        self.update_time = datetime.now()
        self.refreshing = False

        # Initialise Path Tools
        self.pt = PathTools(moles_mapping_url=)

    def process_event(self, action, path):
        """
        Takes the events from rabbit and sends them to the appropriate processor

        :param action: Deposit Action. Can be one of: MKDIR|RMDIR|SYMLINK|00README
        :param path: The directory or readme path to process

        """

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

    def _update_mappings(self):
        """
        Need to make sure that the code is using the most up to date mapping, either in
        MOLES or the spot mapping. This checks
        :return:
        """

        timedelta = (datetime.now() - self.update_time).seconds

        # Refresh if ∆t is greater than the refresh interval and another thread has not already
        # picked up the task.
        if timedelta > self.refresh_interval and not self.refreshing:

            # Set refreshing boolean to be True to avoid another thread trying to refresh the
            # mappings at the same time
            self.refreshing = True

            self.pt.update_mapping()

            self._process_spot_roots()

            # Reset timer and refreshing boolean
            self.update_time = datetime.now()
            self.refreshing = False


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
                list({
                    'id': hashlib.sha1(path).hexdigest(),
                    'document': metadata
                })
            )

    def _process_deletions(self, path):
        """
        Process the deletion of a directory

        :param path: Directory path
        """

        # Delete directory
        self.index_updater.delete_dirs(
            list({
                "id": hashlib.sha1(path).hexdigest()
            })
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
                list({
                    'id': hashlib.sha1(path).hexdigest(),
                    'document': metadata
                })
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
                list({
                    "id": hashlib.sha1(path).hexdigest(),
                    "document": {"readme": content}
                })
            )

    def _process_spot_roots(self):
        """
        Periodically run an indexing job on the top level as these are not picked up in the
        normal way. This is run when a new spot mapping is downloaded.
        """

        spot_paths = self.pt.spots.path2spotmapping

        content_list = []
        for spot in spot_paths:

            metadata, _ = self.pt.generate_path_metadata(spot)

            if metadata:
                content_list.append({
                    "id": hashlib.sha1(spot).hexdigest(),
                    "document": metadata
                })

        self.index_updater.add_dirs(content_list)
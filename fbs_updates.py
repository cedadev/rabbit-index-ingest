# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from configparser import ConfigParser
from ceda_elasticsearch_tools.index_tools.index_updaters import CedaFbi
import hashlib


class FBSUpdateHandler:
    """
    Class to handle the live updates of the FBI index using events from
    rabbitMQ.
    """

    def __init__(self):
        # Read in the config file
        conf = ConfigParser().read('index_updater.ini')

        # Convert refresh interval to seconds
        # self.refresh_interval = refresh_interval * 60

        # Initialise the Elasticsearch connection
        es_auth = {'http_auth': (
            conf.get('elasticsearch', 'es-user'),
            conf.get('elasticsearch', 'es-password')
        )}

        self.index_updater = CedaFbi(
            index=conf.get('files-index', 'es-index'),
            host_url=conf.get('elasticsearch', 'es-host'),
            **es_auth
        )

    def process_event(self, path, action):
        """

        :param path: The file path to process
        :param action: The action to perform on the filepath
        """

        if action == 'DEPOSIT':
            self._process_deposits(path)

        elif action == 'DELETE':
            self._process_deletions(path)

    def _process_deposits(self, path):
        """
        Take the given file path and add it to the FBI index
        :param path: File path
        """
        pass

    def _process_deletions(self, path):
        """
        Take the given file path and delete it from the FBI index
        :param path: File path
        """

        deletion_list = list(
            {'id': hashlib.sha1(path).hexdigest()}
        )

        self.index_updater.delete_files(deletion_list)

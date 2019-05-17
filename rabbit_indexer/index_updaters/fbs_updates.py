# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from configparser import RawConfigParser
from ceda_elasticsearch_tools.index_tools.index_updaters import CedaFbi
import hashlib
from datetime import datetime
#from fbs.proc.file_handlers.handler_picker import HandlerPicker
import os


class FBSUpdateHandler:
    """
    Class to handle the live updates of the FBI index using events from
    rabbitMQ.
    """

    def __init__(self, path_tools, refresh_interval=30):
        # Read in the config file
        base = os.path.dirname(__file__)

        conf = RawConfigParser()
        conf.read(os.path.join(base, '../conf/index_updater.ini'))
        self.calculate_md5 = conf.getboolean('files-index', 'calculate-md5')
        # self.handler_factory = HandlerPicker()
        self.level = conf.get('files-index', 'scan-level')

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

        # Initialise update counter
        self.update_time = datetime.now()
        self.refreshing = False
        self.refresh_interval = refresh_interval * 60

        # Initialise Path Tools
        self.pt = path_tools


    def process_event(self, path, action):
        """

        :param path: The file path to process
        :param action: The action to perform on the filepath
        """

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        if action == 'DEPOSIT':
            self._process_deposits(path)

        elif action == 'DELETE':
            self._process_deletions(path)

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

            successful = self.pt.update_mapping()

            # Reset timer if mapping update successful and reset refreshing boolean
            if successful:
                self.update_time = datetime.now()

            self.refreshing = False

    def _process_deposits(self, path):
        """
        Take the given file path and add it to the FBI index
        :param path: File path
        """

        handler = self.handler_factory.pick_best_handler(path)

        if handler is not None:
            handler_instance = handler(path, self.level, calculate_md5=self.calculate_md5)
            doc = handler_instance.get_metadata()

            if doc is not None:

                spot = self.pt.spots.get_spot(path)

                if spot is not None:
                    doc[0]['info']['spot_name'] = spot

            indexing_list = [{
                'id': self.pt.generate_id(path),
                'document': doc
            }]

            self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_body(file_data):

        data_length = len(file_data)

        doc = file_data[0]

        if file_data[1] is not None:
            doc['info']['phenomena'] = file_data[1]

        if data_length == 3:
            if file_data[2] is not None:
                doc['info']['spatial'] = file_data[2]

        return doc

    def _process_deletions(self, path):
        """
        Take the given file path and delete it from the FBI index
        :param path: File path
        """

        deletion_list = list(
            {'id': self.pt.generate_id(path)}
        )

        self.index_updater.delete_files(deletion_list)

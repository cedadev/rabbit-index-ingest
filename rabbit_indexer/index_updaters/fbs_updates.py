# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '17 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from ceda_elasticsearch_tools.index_tools import CedaFbi
from fbs.proc.file_handlers.handler_picker import HandlerPicker
from fbs.proc.common_util.util import cfg_read
import os
from rabbit_indexer.index_updaters.base import UpdateHandler
from rabbit_indexer.utils.decorators import wait_for_file


class FBSUpdateHandler(UpdateHandler):
    """
    Class to handle the live updates of the FBI index using events from
    rabbitMQ.
    """

    def __init__(self, path_tools, conf, refresh_interval=30):
        """
        Initialise the FBS Update Handler

        :param path_tools: PathTools Object
        :param refresh_interval: Time in minutes to refresh mappings
        """

        super().__init__(path_tools, conf, refresh_interval)

        # Read in the config file
        base = os.path.dirname(__file__)

        self.calculate_md5 = conf.getboolean('files-index', 'calculate-md5')
        self.handler_factory = HandlerPicker(cfg_read(os.path.join(base, '../conf/index_updater.ini')))
        self.level = conf.get('files-index', 'scan-level')

        # Initialise the Elasticsearch connection

        self.index_updater = CedaFbi(
            index=conf.get('files-index', 'es-index'),
            host_url=conf.get('elasticsearch', 'es-host'),
            **{'http_auth': (
                conf.get('elasticsearch', 'es-user'),
                conf.get('elasticsearch', 'es-password')
            ),
                'retry_on_timeout': True,
                'timeout': 60
            }
        )

    def process_event(self, body):
        """

        :param path: The file path to process
        :param action: The action to perform on the filepath
        """

        message = self._decode_message(body)

        self.logger.info(f'{message.filepath}:{message.action}')

        # Check to see if enough time has elapsed to update the mapping
        self._update_mappings()

        if message.action == 'DEPOSIT':
            self._process_deposits(message.filepath)

        elif message.action == 'DELETE':
            self._process_deletions(message.filepath)

    @wait_for_file
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
                    'document': self._create_body(doc)
                }]

                self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_body(file_data):

        data_length = len(file_data)

        doc = file_data[0]
        if data_length > 1:
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

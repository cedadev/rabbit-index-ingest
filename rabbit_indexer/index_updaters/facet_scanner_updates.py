# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .base import UpdateHandler
import logging
from facet_scanner.core.facet_scanner import FacetScanner
from ceda_elasticsearch_tools.elasticsearch import CEDAElasticsearchClient
from rabbit_indexer.utils import PathTools


class FacetScannerUpdateHandler(UpdateHandler):

    def __init__(self, conf):

        # Setup logging
        self.logger = logging.getLogger()
        logging_level = conf.get('logging', 'log-level')
        self.logger.setLevel(getattr(logging, logging_level.upper()))

        # Get the facet scanner class
        self.facet_scanner = FacetScanner()

        # Set up the Elasticsearch connection
        api_key = conf.get('elasticsearch', 'es-api-key')
        self.index = conf.get('files-index', 'es-index')

        self.es = CEDAElasticsearchClient(headers={'x-api-key': api_key})

    def process_event(self, message):
        """
        Scan the file for facets
        :param message:
        :return:
        """

        # Kill the running process
        if message.action == 'JSON_REFRESH':
            exit()

        elif message.action == 'DEPOSIT':
            self._process_deposits(message)

    def _process_deposits(self, message):

        # Wait to make sure that the file is accessible on the filesystem
        self._wait_for_file(message)

        # Get the handler for this filepath
        handler = self.facet_scanner.get_handler(message.filepath)

        # Extract the facets
        facets = handler.get_facets(message.filepath)

        # Build the project dictionary using the handlers project name attr
        project = {
            f'projects.{handler.project_name}': facets
        }

        # Send facets to elasticsearch
        self.es.update(
            index=self.index,
            id=PathTools.generate_id(message.filepath),
            body={'doc': project, 'doc_as_upsert': True}
        )

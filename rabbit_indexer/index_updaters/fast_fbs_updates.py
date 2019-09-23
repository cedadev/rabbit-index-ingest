# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Sep 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .fbs_updates import FBSUpdateHandler
import os


class FastFBSUpdateHandler(FBSUpdateHandler):
    """
    Override deposit methods to provide a way to create the document
    without touching the filesystem or requiring the file to actually
    be available.
    """

    def process_event(self, body):
        """
        Only use information which you can get from the message.
        Does not touch the file system
        :param body: ingest message
        """

        message = self._decode_message(body)

        if message.action == 'DEPOSIT':
            self._process_deposits(message)

        elif message.action == 'DELETE':
            self._process_deletions(message.filepath)

    def _process_deposits(self, message):
        """
        Take the given file path and add it to the FBI index.
        This is the fast version which just uses information
        which can be gleaned from the RabbitMQ message rather
        than getting information from the file.

        :param path: IngestMessage object
        """

        doc = self._create_doc_from_message(message)

        indexing_list = [{
            'id': self.pt.generate_id(message.filepath),
            'document': self._create_body(doc)
        }]

        self.index_updater.add_files(indexing_list)

    @staticmethod
    def _create_doc_from_message(message):
        """
        Creates the FBI document from the rabbit message.
        Does not touch the filesystem
        :param message: IngestMessage object
        :return: document to index to elasticsearch
        """

        filename = os.path.basename(message.filepath)
        dirname = os.path.dirname(message.filepath)
        file_type = os.path.splitext(filename)[1]

        if len(file_type) == 0:
            file_type = "File without extension."

        return [{
            'name_auto': filename,
            'type': file_type,
            'directory': dirname,
            'size': message.filesize,
            'name': filename,
            'location': 'on_disk',
        }]
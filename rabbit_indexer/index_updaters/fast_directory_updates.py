# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Sep 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from .directory_updates import DirectoryUpdateHandler
import os


# Typing imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from rabbit_indexer.queue_handler.queue_handler import IngestMessage


class FastDirectoryUpdateHandler(DirectoryUpdateHandler):
    """
    Handler for directory updates which does not touch the filesystem.
    This handler overwrites the creation methods to rely on the message
    content rather than looking on disk.

    This is to increase the speed at which directories are visible in the index
    """

    def _process_creations(self, message: 'IngestMessage'):
        """
        Process the creation of a new directory

        :param message: the parsed message object
        """

        path = message.filepath

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
        else:

            self.index_updater.add_dirs(
                [
                    {
                        'id': self.pt.generate_id(path),
                        'document': self._generate_doc_from_message(path)
                    }
                ]
            )

    @staticmethod
    def _generate_doc_from_message(path: str) -> dict:
        """
        Generate directory document from path without checking file system attributes

        :param path: filepath
        :return: document metadata
        """
        return {
            'depth': path.count('/'),
            'path': path,
            'type': 'dir',
            'dir': os.path.basename(path)
        }

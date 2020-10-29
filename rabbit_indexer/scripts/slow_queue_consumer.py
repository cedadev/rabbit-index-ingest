# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler
import configparser
import argparse
import logging
import os
from rabbit_indexer.index_updaters.fbs_updates import FBSUpdateHandler
from rabbit_indexer.index_updaters.directory_updates import DirectoryUpdateHandler
from rabbit_indexer.utils.consumer_setup import consumer_setup

logger = logging.getLogger()


class SlowQueueConsumer(QueueHandler):

    def _get_handlers(self):
        """
        Get the stream handlers. Method to allow subclasses to modify which handlers to load.
        """

        self.directory_handler = DirectoryUpdateHandler(path_tools=self.path_tools, conf=self._conf)
        self.fbs_handler = FBSUpdateHandler(path_tools=self.path_tools, conf=self._conf)

    def callback(self, ch, method, properties, body, connection):
        """
        Callback to run during basic consume routine.
        Arguments provided by pika standard message callback method

        :param ch: Channel
        :param method: pika method
        :param properties: pika header properties
        :param body: Message body
        :param connection: Pika connection
        """

        try:
            message = self.decode_message(body)

        except IndexError:
            # Acknowledge message if the message is not compliant
            self.acknowledge_message(ch, method.delivery_tag, connection)
            return

        try:
            if message.action in ['DEPOSIT', 'REMOVE']:
                self.fbs_handler.process_event(message)

                if message.filepath.endswith('00README'):
                    self.directory_handler.process_event(message)

            elif message.action in ['MKDIR', 'RMDIR', 'SYMLINK']:
                self.directory_handler.process_event(message)

            # Acknowledge message
            self.acknowledge_message(ch, method.delivery_tag, connection)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message.filepath}', exc_info=e)
            raise


def main():
    consumer_setup(SlowQueueConsumer, logger)


if __name__ == '__main__':
    main()

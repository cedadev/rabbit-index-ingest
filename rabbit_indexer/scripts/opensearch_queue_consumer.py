# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler.opensearch_queue_handler import OpensearchQueueHandler
from rabbit_indexer.index_updaters import FBSUpdateHandler
from rabbit_indexer.utils.consumer_setup import consumer_setup
import logging

logger = logging.getLogger()


class OpensearchQueueConsumer(OpensearchQueueHandler):

    def _get_handlers(self):
        """
        Get the stream handlers. Method to allow subclasses to modify which handlers to load.
        """

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

        # Filter by path for specific project
        if not message.filepath.startswith('/neodc/esacci'):
            self.acknowledge_message(ch, method.delivery_tag, connection)

        # Try to scan the file for level3 info
        try:
            if message.action in ['DEPOSIT', 'REMOVE']:
                self.fbs_handler.process_event(message)

            # Acknowledge message
            self.acknowledge_message(ch, method.delivery_tag, connection)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message.filepath}', exc_info=e)
            raise


if __name__ == '__main__':
    consumer_setup(OpensearchQueueConsumer, logger)
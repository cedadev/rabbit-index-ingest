# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler.opensearch_queue_handler import OpensearchQueueHandler
from rabbit_indexer.index_updaters.facet_scanner_updates import FacetScannerUpdateHandler
from rabbit_indexer.utils.consumer_setup import consumer_setup
import logging

logger = logging.getLogger()


class OpensearchQueueConsumer(OpensearchQueueHandler):

    def _get_handlers(self):
        self.facet_scanner = FacetScannerUpdateHandler(conf=self._conf)

    def queue_bind(self, channel):

        # Declare queue and bind queue to the fbi exchange
        channel.queue_declare(queue=self.queue_name, auto_delete=False)
        channel.queue_bind(exchange=self.opensearch_exchange, queue=self.queue_name, routing_key='')
        channel.queue_bind(exchange=self.opensearch_exchange, queue=self.queue_name, routing_key='opensearch.tagger.cci')

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

        # Filter by path for specific project
        if not message.filepath.startswith('/neodc/esacci'):
            self.acknowledge_message(ch, method.delivery_tag, connection)
            return

        # Try to extract the facet tags
        try:
            if message.action in ['DEPOSIT','JSON_REFRESH']:
                self.facet_scanner.process_event(message)

            # Acknowledge message
            self.acknowledge_message(ch, method.delivery_tag, connection)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message.filepath}', exc_info=e)
            raise


if __name__ == '__main__':
    consumer_setup(OpensearchQueueConsumer, logger)
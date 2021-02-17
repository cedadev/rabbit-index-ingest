# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler
import logging
import functools
from rabbit_indexer.index_updaters import FastFBSUpdateHandler
from rabbit_indexer.index_updaters import FastDirectoryUpdateHandler
from rabbit_indexer.utils.consumer_setup import consumer_setup

logger = logging.getLogger()


class FastQueueConsumer(QueueHandler):

    def _get_handlers(self):
        self.directory_handler = FastDirectoryUpdateHandler(path_tools=self.path_tools, conf=self._conf)
        self.fbs_handler = FastFBSUpdateHandler(path_tools=self.path_tools, conf=self._conf)

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
            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)
            return

        try:

            if message.action in ['DEPOSIT', 'REMOVE']:
                self.fbs_handler.process_event(message)

            elif message.action in ['MKDIR', 'RMDIR', 'SYMLINK']:
                self.directory_handler.process_event(message)

            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {message}', exc_info=e)
            raise


def main():
    consumer_setup(FastQueueConsumer, logger)


if __name__ == '__main__':
    main()

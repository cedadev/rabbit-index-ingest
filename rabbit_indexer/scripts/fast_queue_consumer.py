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
import functools
from rabbit_indexer.index_updaters import FastFBSUpdateHandler
from rabbit_indexer.index_updaters import FastDirectoryUpdateHandler

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

        # Decode the byte string to utf-8
        body = body.decode('utf-8')

        try:

            if self.deposit.match(body):
                self.fbs_handler.process_event(body)

            elif self.deletion.match(body):
                self.fbs_handler.process_event(body)

            elif self.mkdir.match(body):
                self.directory_handler.process_event(body)

            elif self.rmdir.match(body):
                self.directory_handler.process_event(body)

            elif self.symlink.match(body):
                self.directory_handler.process_event(body)

            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)

        except Exception as e:
            split_line = body.strip().split(":")
            filepath = split_line[3]

            # Catch all exceptions in the scanning code and log them
            logger.error(f'Error occurred while scanning: {filepath}', exc_info=e)
            raise

def main():
    # Command line arguments to get rabbit config file.
    parser = argparse.ArgumentParser(description='Begin the rabbit based deposit indexer')

    # Get default path for config
    base = os.path.dirname(__file__)
    default_config = os.path.join(base, '../conf/index_updater_fast.ini')

    parser.add_argument('--config', dest='config', help='Path to config file for rabbit connection', default=default_config)

    args = parser.parse_args()

    CONFIG_FILE = args.config
    conf = configparser.RawConfigParser()
    conf.read(CONFIG_FILE)

    # Setup logging
    logging_level = conf.get('logging', 'log-level')
    logger.setLevel(getattr(logging, logging_level.upper()))

    # Add formatting
    ch = logging.StreamHandler()
    ch.setLevel(getattr(logging, logging_level.upper()))
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    logger.addHandler(ch)

    queue_handler = FastQueueConsumer(conf)
    queue_handler.run()


if __name__ == '__main__':
    main()

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

logger = logging.getLogger()


class SlowQueueConsumer(QueueHandler):

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
    # Command line arguments to get rabbit config file.
    parser = argparse.ArgumentParser(description='Begin the rabbit based deposit indexer')

    # Get default path for config
    base = os.path.dirname(__file__)
    default_config = os.path.join(base, '../conf/index_updater_slow.ini')

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

    queue_handler = SlowQueueConsumer(conf)
    queue_handler.run()


if __name__ == '__main__':
    main()

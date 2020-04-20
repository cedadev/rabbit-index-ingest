# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '06 Jan 2020'
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


class NonEssentialQueueConsumer(QueueHandler):

    def _get_handlers(self):
        return

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

        message = self.decode_message(body)

        if message.action in ['DEPOSIT','MKDIR','RMDIR','SYMLINK']:
            self.reraise(ch, message)

        # Acknowledge message
        cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
        connection.add_callback_threadsafe(cb)

    def reraise(self, channel, message):

        msg = '{datetime}:{filepath}:{action}:{filesize}:{message}'.format(**message._asdict())

        channel.basic_publish(
            exchange=self.fbi_exchange,
            routing_key='',
            body=msg
        )

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

    queue_handler = NonEssentialQueueConsumer(conf)
    queue_handler.run()


if __name__ == '__main__':
    main()
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
from multiprocessing import Process

logger = logging.getLogger()

def timeout_method(func, args, timeout=60):
    """
    Start a new process with a timeout to handle problem files
    :param func: function to run
    :param args: function arguments
    :param timeout: seconds
    """

    # Create process
    action_process = Process(target=func, args=args)

    # Start process and wait for timeout
    action_process.start()
    action_process.join(timeout=timeout)

    # Terminate process
    action_process.terminate()


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

        message = self.decode_message(body)

        # If the message is redelivered or the path exists run processing
        if method.redelivered or os.path.exists(message.filepath):

            try:
                if message.action in ['DEPOSIT', 'REMOVE']:
                    self.fbs_handler.process_event(message)

                    if message.filename.endswith('00README'):
                        self.directory_handler.process_event(message)

                elif message.action in ['MKDIR', 'RMDIR', 'SYMLINK']:
                    self.directory_handler.process_event(message)

                # Acknowledge message
                cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
                connection.add_callback_threadsafe(cb)

            except Exception as e:
                # Catch all exceptions in the scanning code and log them
                split_line = body.strip().split(":")
                filepath = split_line[3]
                logger.error(f'Error occurred while scanning: {filepath}', exc_info=e)
                raise

        # Path does not exist and message has not been redelivered.
        # Send message to back of queue to build in a wait period
        cb = functools.partial(self._requeue_message, ch, method.delivery_tag)
        connection.add_callback_threadsafe(cb)


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

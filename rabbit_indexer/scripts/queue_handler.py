# encoding: utf-8
"""
Script to process events from the queue and call the relevant code base to update the index
"""
__author__ = 'Richard Smith'
__date__ = '11 Apr 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import pika
import configparser
import re
import uuid
import threading
from rabbit_indexer.utils import PathTools
from rabbit_indexer.index_updaters import FBSUpdateHandler
from rabbit_indexer.index_updaters import DirectoryUpdateHandler
import argparse
import logging
import os
import functools
import signal


class QueueHandler:
    """
    Organises the thread pool and callbacks for the Rabbit Messages
    """

    # Regex patterns
    deposit = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:DEPOSIT:")
    deletion = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:REMOVE:")
    mkdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:MKDIR:")
    rmdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:RMDIR:")
    symlink = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:SYMLINK:")
    readme00 = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*00README:")

    def __init__(self, conf):

        # Get the username and password for rabbit
        rabbit_user = conf.get('server', 'user')
        rabbit_password = conf.get('server', 'password')

        # Get moles api url
        moles_url = f'{conf.get("moles", "moles_url")}/api/v0/obs/all'

        # Get the server variables
        self.rabbit_server = conf.get('server', 'name')
        self.rabbit_vhost = conf.get('server', 'vhost')

        # Create the credentials object
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_password)

        # Set other shared attributes
        self.rabbit_route = conf.get('server', 'log_exchange')
        self.queue_name = conf.get('server', 'queue')
        self.path_tools = PathTools(moles_mapping_url=moles_url)
        self.processing_stop = False

        # Init event handlers
        self.directory_handler = DirectoryUpdateHandler(path_tools=self.path_tools, conf=conf)
        self.fbs_handler = FBSUpdateHandler(path_tools=self.path_tools, conf=conf)

        # Setup logging
        logging_level = conf.get('logging', 'log-level')
        logging.basicConfig(
            level=getattr(logging, logging_level.upper())
        )

        self.logger = logging.getLogger()

    def _connect(self):
        """
        Start Pika connection to server. This is run in each thread.

        :return: pika channel
        """

        # Start the rabbitMQ connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                self.rabbit_server,
                credentials=self.credentials,
                virtual_host=self.rabbit_vhost,
                heartbeat=300
            )
        )

        # Create a new channel
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        # Connect the channel to the exchange
        channel.exchange_declare(exchange=self.rabbit_route, exchange_type='fanout')

        # Declare queue
        channel.queue_declare(queue=self.queue_name, auto_delete=True)
        channel.queue_bind(exchange=self.rabbit_route, queue=self.queue_name)

        # Set callback
        callback = functools.partial(self._callback, connection=connection)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)

        return channel

    def _acknowledge_message(self, channel, delivery_tag):
        """
        Acknowledge message

        :param channel: Channel which message came from
        :param delivery_tag: Message id
        """

        self.logger.debug(f'Acknowledging message: {delivery_tag}')
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    def _callback(self, ch, method, properties, body, connection):
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

        split_line = body.strip().split(":")

        # Several unused splits but here to preserve the format of the message
        # date_hour = split_line[0]
        # min = split_line[1]
        # sec = split_line[2]
        filepath = split_line[3]
        action = split_line[4]
        # filesize = split_line[5]
        # message = ":".join(split_line[6:])

        try:

            if self.deposit.match(body):
                self.fbs_handler.process_event(filepath, action)

                if self.readme00.match(body):
                    self.directory_handler.process_event(filepath, action)

            elif self.deletion.match(body):
                self.fbs_handler.process_event(filepath, action)

            elif self.mkdir.match(body):
                self.directory_handler.process_event(filepath, action)

            elif self.rmdir.match(body):
                self.directory_handler.process_event(filepath, action)

            elif self.symlink.match(body):
                self.directory_handler.process_event(filepath, action)

            # Acknowledge message
            cb = functools.partial(self._acknowledge_message, ch, method.delivery_tag)
            connection.add_callback_threadsafe(cb)

        except Exception as e:
            # Catch all exceptions in the scanning code and log them
            self.logger.error(f'Error occurred while scanning: {filepath}', exc_info=e)

    def run(self):
        """
        Method to run when thread is started. Creates an AMQP connection for each thread
        and sets some exception handling.

        A common exception which occurs is StreamLostError. The connection should get reset if that happens.
        :return:
        """

        while True:
            channel = self._connect()

            try:
                channel.start_consuming()

            except KeyboardInterrupt:
                channel.stop_consuming()
                break

            except pika.exceptions.StreamLostError as e:
                # Log problem
                self.logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                self.logger.critical(e)

                channel.stop_consuming()
                break


def main():
    # Command line arguments to get rabbit config file.
    parser = argparse.ArgumentParser(description='Begin the rabbit based deposit indexer')

    # Get default path for config
    base = os.path.dirname(__file__)
    default_config = os.path.join(base, '../conf/index_updater.ini')

    parser.add_argument('--config', dest='config', help='Path to config file for rabbit connection', default=default_config)

    args = parser.parse_args()

    CONFIG_FILE = args.config
    conf = configparser.RawConfigParser()
    conf.read(CONFIG_FILE)

    queue_handler = QueueHandler(conf)
    queue_handler.run()


if __name__ == '__main__':
    main()

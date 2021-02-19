# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import pika
from rabbit_indexer.utils import YamlConfig
import logging
import functools
from collections import namedtuple
import json

# Typing imports
from pika.channel import Channel
from pika.connection import Connection
from pika.frame import Method
from pika.frame import Header

logger = logging.getLogger()

elastic_logger = logging.getLogger('elasticsearch')
elastic_logger.setLevel(logging.WARNING)


IngestMessage = namedtuple('IngestMessage',['datetime','filepath','action','filesize','message'])


class QueueHandler:
    """
    Organises the rabbitMQ connection and call back

    Class Variables:
        conf: The loaded configuration

    Parameters:
        conf: A YamlConfig Object
    """

    @staticmethod
    def decode_message(body: bytes) -> IngestMessage:
        """
        Takes the message and turns into a dictionary.
        String message format when split on :
            date_hour = split_line[0]
            min = split_line[1]
            sec = split_line[2]
            path = split_line[3]
            action = split_line[4]
            filesize = split_line[5]
            message = ":".join(split_line[6:])

        :param body: Message body, either a json string or text
        :return: IngestMessage
            {
                'datetime': ':'.join(split_line[:3]),
                'filepath': split_line[3],
                'action': split_line[4],
                'filesize': split_line[5],
                'message': ':'.join(split_line[6:])
            }

        """

        # Decode the byte string to utf-8
        body = body.decode('utf-8')

        try:
            msg = json.loads(body)
            return IngestMessage(**msg)

        except json.JSONDecodeError:
            # Assume the message is in the old format and split on :
            split_line = body.strip().split(":")

            msg = {
                'datetime': ':'.join(split_line[:3]),
                'filepath': split_line[3],
                'action': split_line[4],
                'filesize': split_line[5],
                'message': ':'.join(split_line[6:])
            }

        return IngestMessage(**msg)

    def __init__(self, conf: YamlConfig):
        """

        :param conf:
        """

        self.conf = conf
        self.queue_handler = None

        # Init event handlers
        self.get_handlers()

    def _connect(self):
        """
        Start Pika connection to server. This is run in each thread.

        :return: pika channel
        """

        # Get the username and password for rabbit
        rabbit_user = self.conf.get('rabbit_server', 'user')
        rabbit_password = self.conf.get('rabbit_server', 'password')

        # Get the server variables
        rabbit_server = self.conf.get('rabbit_server', 'name')
        rabbit_vhost = self.conf.get('rabbit_server', 'vhost')

        # Create the credentials object
        credentials = pika.PlainCredentials(rabbit_user, rabbit_password)

        print(self.conf.config)
        print(rabbit_server, rabbit_user, rabbit_password, rabbit_vhost)

        # Start the rabbitMQ connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=rabbit_server,
                credentials=credentials,
                virtual_host=rabbit_vhost,
                heartbeat=300
            )
        )

        # Get the exchanges to bind
        src_exchange = self.conf.get('rabbit_server', 'source_exchange')
        dest_exchange = self.conf.get('rabbit_server', 'dest_exchange')

        # Create a new channel
        channel = connection.channel()
        channel.basic_qos(prefetch_count=1)

        # Declare relevant exchanges
        channel.exchange_declare(exchange=src_exchange['name'], exchange_type=src_exchange['type'])
        channel.exchange_declare(exchange=dest_exchange['name'], exchange_type=dest_exchange['type'])

        # Bind source exchange to dest exchange
        channel.exchange_bind(destination=dest_exchange['name'], source=src_exchange['name'])

        # Declare queue and bind queue to the dest exchange
        queues = self.conf.get('rabbit_server', 'queues')
        for queue in queues:
            channel.queue_declare(queue=queue['name'], **queue['kwargs'])
            channel.queue_bind(exchange=dest_exchange['name'], queue=queue['name'])

            # Set callback
            callback = functools.partial(self.callback, connection=connection)
            channel.basic_consume(queue=queue['name'], on_message_callback=callback, auto_ack=False)

        return channel

    @staticmethod
    def _acknowledge_message(channel: Channel, delivery_tag: str):
        """
        Acknowledge message

        :param channel: Channel which message came from
        :param delivery_tag: Message id
        """

        logger.debug(f'Acknowledging message: {delivery_tag}')
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    def acknowledge_message(self, channel: Channel, delivery_tag: str, connection: Connection):
        """
        Acknowledge message and move onto the next. All of the required
        params come from the message callback params.

        :param channel: callback channel param
        :param delivery_tag: from the callback method param. eg. method.delivery_tag
        :param connection: connection object from the callback param
        """
        cb = functools.partial(self._acknowledge_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

    def callback(self, ch: Channel, method: Method, properties: Header, body: bytes, connection: Connection):
        """
        Abstract method to define the callback to run during basic consume routine.
        Arguments provided by pika standard message callback method

        :param ch: Channel
        :param method: pika method
        :param properties: pika header properties
        :param body: Message body
        :param connection: Pika connection
        """

        raise NotImplementedError

    def get_handlers(self):
        """
        Abstract method to load the stream handlers.
        """
        return NotImplementedError

    def run(self):
        """
        Method to run when thread is started. Creates an AMQP connection
        and sets some exception handling.

        A common exception which occurs is StreamLostError.
        The connection should get reset if that happens.

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
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                logger.critical(e)

                channel.stop_consuming()
                break



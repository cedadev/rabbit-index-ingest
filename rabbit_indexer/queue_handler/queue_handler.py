# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '23 Aug 2019'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import pika
from rabbit_indexer.utils import PathTools
import logging
import functools
from collections import namedtuple
import json

logger = logging.getLogger()

elastic_logger = logging.getLogger('elasticsearch')
elastic_logger.setLevel(logging.WARNING)


IngestMessage = namedtuple('IngestMessage',['datetime','filepath','action','filesize','message'])


class QueueHandler:
    """
    Organises the thread pool and callbacks for the Rabbit Messages
    """

    @staticmethod
    def decode_message(body):
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
        :return: dictionary of format
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

    def __init__(self, conf):

        # Get the username and password for rabbit
        rabbit_user = conf.get('server', 'user')
        rabbit_password = conf.get('server', 'password')

        # Get moles api url
        moles_obs_map_url = conf.get("moles", "moles_obs_map_url")

        # Get the server variables
        self.rabbit_server = conf.get('server', 'name')
        self.rabbit_vhost = conf.get('server', 'vhost')

        # Create the credentials object
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_password)

        # Set other shared attributes
        self.log_exchange = conf.get('server', 'log_exchange')
        self.fbi_exchange = conf.get('server', 'fbi_exchange')
        self.queue_name = conf.get('server', 'queue')
        self.path_tools = PathTools(moles_mapping_url=moles_obs_map_url)
        self.processing_stop = False

        self._conf = conf

        # Init event handlers
        self._get_handlers()

    def _get_handlers(self):
        """
        Get the stream handlers.
        """

        raise NotImplementedError


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

        # Declare relevant exchanges
        channel.exchange_declare(exchange=self.log_exchange, exchange_type='fanout')
        channel.exchange_declare(exchange=self.fbi_exchange, exchange_type='fanout')

        # Bind fbi exchange to log exchange
        channel.exchange_bind(destination=self.fbi_exchange, source=self.log_exchange)

        # Declare queue and bind queue to the fbi exchange
        channel.queue_declare(queue=self.queue_name, auto_delete=False)
        channel.queue_bind(exchange=self.fbi_exchange, queue=self.queue_name)

        # Set callback
        callback = functools.partial(self.callback, connection=connection)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)

        return channel

    @staticmethod
    def _acknowledge_message(channel, delivery_tag):
        """
        Acknowledge message

        :param channel: Channel which message came from
        :param delivery_tag: Message id
        """

        logger.debug(f'Acknowledging message: {delivery_tag}')
        if channel.is_open:
            channel.basic_ack(delivery_tag)

    def acknowledge_message(self, channel, delivery_tag, connection):
        """
        Acknowledge message and move onto the next. All of the required
        params come from the message callback params.
        :param channel: callback channel param
        :param delivery_tag: from the callback method param. eg. method.delivery_tag
        :param connection: connection object from the callback param
        """
        cb = functools.partial(self._acknowledge_message, channel, delivery_tag)
        connection.add_callback_threadsafe(cb)

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

        raise NotImplementedError

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
                logger.error('Connection lost, reconnecting', exc_info=e)
                continue

            except Exception as e:
                logger.critical(e)

                channel.stop_consuming()
                break



# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Apr 2020'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from rabbit_indexer.queue_handler import QueueHandler
import pika
import functools


class OpensearchQueueHandler(QueueHandler):

    def __init__(self, conf):
        super().__init__(conf)

        self.opensearch_exchange = conf.get('server', 'opensearch_exchange')

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
        channel.exchange_declare(exchange=self.opensearch_exchange, exchange_type='topic')

        # Bind opensearch exchange to log exchange
        channel.exchange_bind(destination=self.opensearch_exchange, source=self.log_exchange)

        # Declare queue and bind queue to the fbi exchange
        channel.queue_declare(queue=self.queue_name, auto_delete=False)
        channel.queue_bind(exchange=self.opensearch_exchange, queue=self.queue_name)

        # Set callback
        callback = functools.partial(self.callback, connection=connection)
        channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=False)

        return channel

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
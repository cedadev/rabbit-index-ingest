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
import os
import configparser
import re
import uuid
import threading
from utils.path_tools import PathTools
from fbs_updates import FBSUpdateHandler
from directory_updates import DirectoryUpdateHandler


class QueueHandler:
    # Regex patterns
    deposit = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:DEPOSIT:")
    deletion = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:REMOVE:")
    mkdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:MKDIR:")
    rmdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:RMDIR:")
    symlink = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:SYMLINK:")
    readme00 = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*00README:")

    def __init__(self, conf):

        # Get the username and password for rabbit
        rabbit_user = conf.get("server", "user")
        rabbit_password = conf.get("server", "password")

        # Get the server variables
        self.rabbit_server = conf.get("server", "name")
        self.rabbit_vhost = conf.get("server", "vhost")

        # Create the credentials object
        self.credentials = pika.PlainCredentials(rabbit_user, rabbit_password)

        # Set other shared attributes
        self.rabbit_route = conf.get("server", "log_exchange")
        self.thread_map = {}
        self.queue_name = f'elasticsearch_update_queue_{uuid.uuid4()}'
        self.path_tools = PathTools()

        # Init event handlers
        self.directory_handler = DirectoryUpdateHandler(path_tools=self.path_tools)
        self.fbs_handler = FBSUpdateHandler(path_tools=self.path_tools)

        # Create thread pool
        thread_list = []
        for i in range(5):
            thread = threading.Thread(
                target=self.run,
                name=f'Thread-{i}',
                daemon=True
            )
            thread_list.append(thread)

        for thread in thread_list:
            self.thread_map[thread.ident] = thread.name
            thread.start()

        for thread in thread_list:
            thread.join()

    def callback(self, ch, method, properties, body):
        # Decode the byte string to utf-8
        body = body.decode('utf-8')

        split_line = body.strip().split(":")

        # date_hour = split_line[0]
        # min = split_line[1]
        # sec = split_line[2]
        filepath = split_line[3]
        action = split_line[4]
        # filesize = split_line[5]
        # message = ":".join(split_line[6:])

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

    # def run(self):

    def run(self):

        # Start the rabbitMQ connection
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                self.rabbit_server,
                credentials=self.credentials,
                virtual_host=self.rabbit_vhost,
                heartbeat=50
            )
        )

        # Create a new channel per thread
        channel = connection.channel()

        # Connect the channel to the exchange
        channel.exchange_declare(exchange=self.rabbit_route, exchange_type='fanout')

        channel.queue_declare(queue=self.queue_name, auto_delete=True)
        channel.queue_bind(exchange=self.rabbit_route, queue=self.queue_name)

        channel.basic_consume(queue=self.queue_name,
                              on_message_callback=self.callback,
                              auto_ack=False)

        channel.start_consuming()


if __name__ == '__main__':
    CONFIG_FILE = os.path.join(os.environ["HOME"], ".deposit_server.cfg")
    conf = configparser.ConfigParser()
    conf.read(CONFIG_FILE)

    QueueHandler(conf)

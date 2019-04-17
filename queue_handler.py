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

from multiprocessing.pool import ThreadPool





class QueueHandler:

    # Regex patterns
    deposit = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:DEPOSIT:")
    deletion = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:REMOVE:")
    mkdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:MKDIR:")
    rmdir = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:RMDIR:")
    symlink = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*:SYMLINK:")
    readme00 = re.compile("^\d{4}[-](\d{2})[-]\d{2}.*00README:")

    def __init__(self, conf):
        rabbit_user = conf.get("server", "user")
        rabbit_password = conf.get("server", "password")

        rabbit_server = conf.get("server", "name")
        rabbit_vhost = conf.get("server", "vhost")
        rabbit_route = conf.get("server", "log_exchange")

        credentials = pika.PlainCredentials(rabbit_user, rabbit_password)

        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                rabbit_server,
                credentials=credentials,
                virtual_host=rabbit_vhost,
                heartbeat=50
            )
        )

        self.channel = connection.channel()

        self.channel.exchange_declare(exchange=rabbit_route,
                                 exchange_type='fanout')

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.queue_name = result.method.queue
        self.channel.queue_bind(exchange=rabbit_route, queue=self.queue_name)

        print(' [*] Waiting for logs. To exit press CTRL+C')

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
            print('deposit')
            #Add to readme list if the deposited file is a 00README
            if self.readme00.match(body):
                print('readme')

        elif self.deletion.match(body):
            print('delete')

        elif self.mkdir.match(body):
            print('mkdir')

        elif self.rmdir.match(body):
            print('rmdir')

        elif self.symlink.match(body):
            print('symlink')


    def run(self):
        self.channel.basic_consume(queue=self.queue_name,
                                   on_message_callback=self.callback,
                                   auto_ack=False)

        self.channel.start_consuming()



if __name__ == '__main__':


    CONFIG_FILE = os.path.join(os.environ["HOME"], ".deposit_server.cfg")
    conf = configparser.ConfigParser()
    conf.read(CONFIG_FILE)

    QueueHandler(conf).run()

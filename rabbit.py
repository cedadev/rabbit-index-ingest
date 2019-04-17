import pika
import os
import configparser

CONFIG_FILE = os.path.join(os.environ["HOME"], ".deposit_server.cfg")
conf = configparser.ConfigParser()
conf.read(CONFIG_FILE)
RABBIT_SERVER = conf.get("server", "name")
RABBIT_USER = conf.get("server", "user")
RABBIT_PASSWORD = conf.get("server", "password")
RABBIT_VHOST = conf.get("server", "vhost")
RABBIT_ROUTE = conf.get("server", "log_exchange")

credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASSWORD)
connection = pika.BlockingConnection(pika.ConnectionParameters(RABBIT_SERVER,
                                                               credentials=credentials,
                                                               virtual_host=RABBIT_VHOST,
                                                               heartbeat_interval=50))

channel = connection.channel()

channel.exchange_declare(exchange=RABBIT_ROUTE,
                         exchange_type='fanout')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange=RABBIT_ROUTE, queue=queue_name)

print(' [*] Waiting for logs. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] %r" % body)

channel.basic_consume(callback,
                      queue=queue_name,
                      no_ack=True)

channel.start_consuming()

# Rabbit Indexer

This code connects to the RabbitMQ exchange and reads messages created by the deposit server.
These messages are parsed and handlers and initialised to process the events.
DEPOSIT and REMOVE actions are sent to the `ceda-fbi` index and MKDIR, RMDIR, SYMLINK and 00README actions
are sent to the `ceda-dirs` index. 
This is to ensure that the content of the indices matches the archive for use with the archive_browser.
This repo serves as a base repo for the downstream specific handlers:
- [Facet Scanner](https://github.com/cedadev/rabbit_facet_scanner)
- [DBI Indexer](https://github.com/cedadev/rabbit-dbi-indexer)
- [FBI Indexer](https://github.com/cedadev/rabbit-fbi-indexer)

The diagram below is a rough sketch of how the events from the deposit server are picked up by this library.

![Service Diagram for Rabbit Indexer](docs/images/rabbit_indexer.png)

# Installing

In order to make it easy to set up the correct environment, this codebase should be installed as a package.
This can be done using:

1. Clone the environment `git clone https://github.com/cedadev/rabbit-index-ingest`

2. Install the package `pip install -e <path_to_setup.py>`


## Running

1. Activate the environment (On ingest machines this is `rabbit_fbi`)
2. `rabbit_event_indexer`

### rabbit_event_indexer

Console script which sets up the indexer based on the supplied
config file. Can be used by downstream indexers.

```
usage: rabbit_event_indexer [-h] --config CONFIG [CONFIG ...]

Begin the rabbit based deposit indexer

optional arguments:
  -h, --help            show this help message and exit
  --config CONFIG [CONFIG ...]
                        Path to config file for rabbit connection

```

## Configuration

Configuration for the rabbit_indexer is provided by a YAML file. The individual indexers
define the required sections and give an example template but the full configuration
options are described below.

The file format is split into sections:
- [rabbit_server](#rabbit_server)
- [indexer](#indexer)
- [logging](#logging)
- [moles](#moles)
- [elasticsearch](#elasticsearch)
- [directory_index](#directory-index)
- [files_index](#files-index)

### rabbit_server

| Parameter | Description |
|-----------|-------------|
| `name`                    | The FQDN for the rabbitMQ server |
| `user`                    | Username to log in as |
| `password`                | Password for login |
| `vhost`                   | The rabbitMQ namespace |
| `source_exchange`         | Map of values to define the source exchange as defined by [exchange](#exchange) |
| `dest_exchange`           | Map of values to define the destination exchange as defined by [exchange](#exchange) |
| `queues`                  | List of queues to connect to with parameters defined by [queue](#queue)|

#### Exchange

| Parameter | Description |
|-----------|-------------|
| `name` | Name of source exchange |
| `type` | Type of source exchange |

#### Queue

| Parameter | Description |
|-----------|-------------|
| `name`      | Name of the queue to connect to |
| `kwargs`    | kwargs to provide the [pika.queue_declare](https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_declare) method |
| `bind_kwargs` | kwargs to provide to the [pika.queue_bind](https://pika.readthedocs.io/en/stable/modules/channel.html#pika.channel.Channel.queue_bind)

### indexer

| Parameter | Description |
|-----------|-------------|
| `queue_consumer_class` | The python path to the consumer class. e.g. rabbit_dbi_elastic_indexer.queue_consumers.DBIQueueConsumer |
| `path_filter` | kwargs for the [rabbit_indexer.utils.PathFilter](rabbit_indexer/utils/path_tools.py#L235)  |

### logging
| Parameter | Description |
|-----------|-------------|
| `log_level` | Set the python logging level |

### moles
| Parameter | Description |
|-----------|-------------|
| `moles_obs_map_url` | URL to download the observation map |

### elasticsearch
| Parameter | Description |
|-----------|-------------|
| `es_api_key` | Elasticsearch API key to allow write access to the indices |

### directory_index
| Parameter | Description |
|-----------|-------------|
| `name` | Name of the Directory index to write to |

### Files Index
| Parameter | Description |
|-----------|-------------|
| `name` | Name of the Files index to write to |
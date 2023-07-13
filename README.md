[![Python CI actions](https://github.com/adsabs/SciXIngestParserPipeline/actions/workflows/python_actions.yml/badge.svg)](https://github.com/adsabs/SciXIngestParserPipeline/actions/workflows/python_actions.yml) [![Coverage Status](https://coveralls.io/repos/github/adsabs/SciXIngestParserPipeline/badge.svg?branch=main)](https://coveralls.io/github/adsabs/SciXIngestParserPipeline?branch=main)

![Parser Pipeline Flowchart](README_assets/Parser_Pipeline_implementation.png?raw=true "Parser Pipeline Flowchart")
# Setting Up a Development Environment
## Installing dependencies and hooks

This project uses `pyproject.toml` to install necessary dependencies and otherwise set up a working development environment. To set up a local working environment, simply run the following:
```bash
virtualenv .venv
source .venv/bin/activate
pip install .[dev]
pip install .
pre-commit install
pre-commit install --hook-type commit-msg
```
To install gRPC, the following variables may need to be set in order for the build to succeed:
```bash
export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
export GRPC_PYTHON_BUILD_SYSTEM_ZLIB=1
```
## Testing with pytest

Tests can be run from the main directory using pytest:
```bash
pytest
```
The pytest command line arguments are already specified in `pyproject.toml`.
## Testing Against Kafka
### The Kafka Environment

In order to set up a full development environment, a kafka instance must be created that contains at least:
- kafka broker
- kafka zookeeper
- kafka schema registry
- postgres
- redis
- minIO (or AWS S3 bucket)

The following can also be helpful:
- kafka-ui
- pgadmin

Next, we need to copy config.py to local_config.py and update the environment variables to point to reflect the values of the local environment. For postgres, we will need a database to store data. We will also need an S3 bucket created either on AWS or locally on a minIO instance. We will also need to create Pipeline input and output topics which can be done either through python or by using the kafka-ui. The relevant AVRO schemas from SciXParser/AVRO_schemas/ must also be added to the schema registry using either python or the UI.

## Launching The Pipeline

All SciX Pipelines require librdkafka be installed. The source can be found here. Installation on most `nix systems can be done by running the following:
```bash
git clone https://github.com/edenhill/librdkafka
cd librdkafka && ./configure && make && make install && ldconfig
```
To launch the pipeline, the following commands need to be run within the SciXParser/ directory
```bash
#Start gRPC API
python3 run.py PARSER_API
#Start Parser pipeline consumer and producer
python3 run.py PARSER_APP
```
# Sending commands to the gRPC API

Currently, there are three methods that have been defined in the API for interacting with the Parser Pipeline.

```bash
#This command tells the server to initialize a job by adding a message to the Parser Topic
python3 API/parser_client.py REPARSE --uuid "<string of space separated uuids>"
#This command asks the server to check on the current status of a record with id <uuid>
python3 API/parser_client.py MONITOR --uuid '<single uuid>'
#This command returns the current parsed record for a given <uuid>
python3 API/parser_client.py VIEW --uuid '<single uuid>'
```

`REPARSE` takes optional arguments:
- `--force` : forces reparsed data to be sent to the Kafka topic regardless of whether the data has changed.
- `--resend-only` : Only resends the current parsed data stored in the database without doing any parsing.

Additionally, calling `REPARSE` or `MONITOR` command with --persistence will open a persistent connection that streams updates for the specificed uuid if only one is provided.

## Maintainers

Taylor Jacovich, Kelly Lockhart, Sergi Blanco-Cuaresma, The ADS team

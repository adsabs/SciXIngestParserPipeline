"""The Python AsyncIO implementation of the GRPC parser server."""

import json
import logging
import sys
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Thread

import grpc
import redis
from confluent_kafka.avro import AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from SciXPipelineUtils import utils
from SciXPipelineUtils.avro_serializer import AvroSerialHelper
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from API.grpc_modules.parser_grpc import (
    ParserInitServicer,
    ParserMonitorServicer,
    ParserViewServicer,
    add_ParserInitServicer_to_server,
    add_ParserMonitorServicer_to_server,
    add_ParserViewServicer_to_server,
)
from parser import db

HERE = Path(__file__).parent
proj_home = str(HERE / "..")
sys.path.append(proj_home)

config = utils.load_config(proj_home=proj_home)


class Logging:
    def __init__(self, logger):
        self.logger = logger


NUMBER_OF_REPLY = 10


class Listener(Thread):
    def __init__(self):
        self.redis = redis.StrictRedis(
            config.get("REDIS_HOST", "localhost"),
            config.get("REDIS_PORT", 6379),
            charset="utf-8",
            decode_responses=True,
        )
        self.subscription = self.redis.pubsub()
        self.end = False

    def subscribe(self, channel_name="PARSER_statuses"):
        self.subscription.subscribe(channel_name)

    def get_status_redis(self, job_id, logger):
        status = None
        logger.debug("DB: Listening for parser status updates")
        logger.debug(str(self.subscription.listen()))
        for message in self.subscription.listen():
            if self.end:
                logger.debug("Ending Listener thread")
                return
            logger.debug("DB: Message from redis: {}".format(message))
            if message is not None and isinstance(message, dict):
                if message.get("data") != 1:
                    logger.debug("DB: data: {}".format(message.get("data")))
                    status_dict = json.loads(message.get("data"))
                    if status_dict["job_id"] == job_id:
                        status = status_dict["status"]
                        logger.debug("DB: status: {}".format(status))
                        yield status


def initialize_parser(gRPC_Servicer=ParserInitServicer):
    class Parser(gRPC_Servicer):
        def __init__(self, producer, req_schema, schema_client, logger):
            self.topic = config.get("PARSER_INPUT_TOPIC")
            self.timestamp = datetime.now().timestamp()
            self.producer = producer
            self.req_schema = req_schema
            self.schema_client = schema_client
            self.engine = create_engine(config.get("SQLALCHEMY_URL"))
            self.Session = sessionmaker(self.engine)
            self.logger = logger

        @contextmanager
        def session_scope(self):
            """Provide a transactional scope for postgres."""
            session = self.Session()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        def persistent_connection(self, job_request, listener):
            record_id = job_request.get("record_id")
            msg = db.get_job_status_by_record_id(self, [str(record_id)]).name
            self.logger.info("PARSER: User requested persitent connection.")
            self.logger.info("PARSER: Latest message is: {}".format(msg))
            job_request["status"] = str(msg)
            yield job_request
            if msg == "Error":
                Done = True
                self.logger.debug("Error = {}".format(Done))
                listener.end = True
            elif msg == "Success":
                Done = True
                self.logger.debug("Done = {}".format(Done))
                listener.end = True
            else:
                Done = False
            old_msg = msg
            while not Done:
                if msg and msg != old_msg:
                    self.logger.info("yielded new status: {}".format(msg))
                    job_request["status"] = str(msg)
                    yield job_request
                    old_msg = msg
                    try:
                        if msg == "Error":
                            Done = True
                            self.logger.debug("Error = {}".format(Done))
                            listener.end = True
                            break
                        elif msg == "Success":
                            Done = True
                            self.logger.debug("Done = {}".format(Done))
                            listener.end = True
                            break

                    except Exception:
                        continue
                    try:
                        msg = next(listener.get_status_redis(record_id, self.logger))
                        self.logger.debug("PARSER: Redis returned: {} for job_id".format(msg))
                    except Exception:
                        msg = ""
                        continue

                else:
                    try:
                        msg = next(listener.get_status_redis(record_id, self.logger))
                        self.logger.debug("PARSER: Redis published status: {}".format(msg))
                    except Exception as e:
                        self.logger.error("failed to read message with error: {}.".format(e))
                        continue
            return

        def initParser(self, request, context: grpc.aio.ServicerContext):
            self.logger.info("Serving initParser request %s", request)
            self.logger.info(json.dumps(request.get("task_args")))
            self.logger.info(
                "Sending {} to Parser Topic".format(
                    b" %s." % json.dumps(request.get("task_args")).encode("utf-8")
                )
            )
            job_request = request
            persistence = job_request.get("persistence", False)
            job_request.pop("persistence")

            self.logger.info(job_request)

            job_request["status"] = "Pending"

            self.producer.produce(
                topic=self.topic, value=job_request, value_schema=self.req_schema
            )

            db.write_job_status(self, job_request)

            yield job_request

            if persistence:
                listener = Listener()
                listener.subscribe()
                yield from self.persistent_connection(job_request, listener)

        def viewParser(self, request, context: grpc.aio.ServicerContext):
            self.logger.info("Serving viewParser request %s", request)
            self.logger.info(json.dumps(request.get("task_args")))
            record_id = request["record_id"]
            record = db.get_parser_record(self, record_id).parsed_data
            return record

        def monitorParser(self, request, context: grpc.aio.ServicerContext):
            self.logger.info("%s", request)
            self.logger.info(json.dumps(request.get("task_args")))

            job_request = request
            persistence = job_request.get("persistence", False)

            record_id = request.get("record_id")

            if record_id:
                if persistence:
                    listener = Listener()
                    listener.subscribe()
                    yield from self.persistent_connection(job_request, listener)
                else:
                    msg = db.get_job_status_by_record_id(self, [str(record_id)]).name
                    job_request["status"] = str(msg)
                    yield job_request
            else:
                msg = "Error"
                job_request["status"] = msg
                yield job_request

    return Parser


async def serve() -> None:
    server = grpc.aio.server()
    app_log = Logging(logging)
    schema_client = SchemaRegistryClient({"url": config.get("SCHEMA_REGISTRY_URL")})
    main_schema = utils.get_schema(app_log, schema_client, config.get("PARSER_INPUT_SCHEMA"))
    data_schema = utils.get_schema(app_log, schema_client, config.get("PARSER_OUTPUT_SCHEMA"))
    mainserialhelper = AvroSerialHelper(ser_schema=main_schema, logger=app_log.logger)
    viewserialhelper = AvroSerialHelper(
        ser_schema=data_schema, des_schema=main_schema, logger=app_log.logger
    )
    producer = AvroProducer(
        {
            "schema.registry.url": config.get("SCHEMA_REGISTRY_URL"),
            "bootstrap.servers": config.get("KAFKA_BROKER"),
        }
    )

    add_ParserInitServicer_to_server(
        initialize_parser(ParserInitServicer)(
            producer, main_schema, schema_client, app_log.logger
        ),
        server,
        mainserialhelper,
    )
    add_ParserViewServicer_to_server(
        initialize_parser(ParserViewServicer)(
            producer, main_schema, schema_client, app_log.logger
        ),
        server,
        viewserialhelper,
    )
    add_ParserMonitorServicer_to_server(
        initialize_parser(ParserMonitorServicer)(
            producer, main_schema, schema_client, app_log.logger
        ),
        server,
        mainserialhelper,
    )
    listen_addr = "[::]:" + str(config.get("GRPC_PORT", 50051))
    server.add_insecure_port(listen_addr)

    app_log.logger.info("Starting server on %s", listen_addr)
    await server.start()
    await server.wait_for_termination()

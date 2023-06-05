import json
import logging
import time
from contextlib import contextmanager

import redis
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from SciXPipelineUtils import utils
from SciXPipelineUtils.s3_methods import load_s3_providers
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from parser import db, parsing_handler


def init_pipeline(proj_home, consumer_topic_name=None, consumer_schema_name=None):
    """
    input:
    proj_home: The home directory for the Pipeline

    Initializes the relevant python methods
    app: The main application class
    schema_client: The Kafka Schema Registry
    schema: The input schema the pipeline uses
    consumer: The kafka consumer for the pipeline
    producer: The kafka producer for the pipeline
    """
    app = PARSER_APP(proj_home)
    if not consumer_schema_name:
        consumer_schema_name = app.config.get("PARSER_INPUT_SCHEMA")
    if not consumer_topic_name:
        consumer_topic_name = app.config.get("PARSER_INPUT_TOPIC")
    app.schema_client = SchemaRegistryClient({"url": app.config.get("SCHEMA_REGISTRY_URL")})
    schema = utils.get_schema(app, app.schema_client, consumer_schema_name)
    consumer = AvroConsumer(
        {
            "bootstrap.servers": app.config.get("KAFKA_BROKER"),
            "schema.registry.url": app.config.get("SCHEMA_REGISTRY_URL"),
            "auto.offset.reset": "latest",
            "group.id": "ParserPipeline1",
        },
        reader_value_schema=schema,
    )
    consumer.subscribe([consumer_topic_name])
    producer = AvroProducer(
        {
            "bootstrap.servers": app.config.get("KAFKA_BROKER"),
            "schema.registry.url": app.config.get("SCHEMA_REGISTRY_URL"),
        }
    )
    app.logger.info("Starting PARSER APP on TOPIC: {}".format(consumer_topic_name))
    app.parser_consumer(consumer, producer)


class PARSER_APP:
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

    def _consume_from_topic(self, consumer):
        self.logger.debug("Consuming from Parser Topic")
        return consumer.poll()

    def _init_logger(self):
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger(__name__)
        self.logger.info("Starting Parser Service Logging")

    def __init__(self, proj_home):
        """
        input:
        proj_home: The home directory for the Pipeline

        The main application for the pipeline
        config: The configuration for the pipeline
        engine: The SQLAlchemy engine
        logger: The logger for the pipeline
        schema_client: The kafka schema registry client
        s3Clients: The S3 providers that the pipeline may interact with
        Session: The SQLAlchemy session
        redis: The redis server configuration
        """
        self.config = utils.load_config(proj_home)
        self.engine = create_engine(self.config.get("SQLALCHEMY_URL"))
        self.logger = None
        self.schema_client = None
        self._init_logger()
        self.s3Clients = load_s3_providers(self.config)
        self.Session = sessionmaker(self.engine)
        self.redis = redis.StrictRedis(
            self.config.get("REDIS_HOST", "localhost"),
            self.config.get("REDIS_PORT", 6379),
            decode_responses=True,
        )

    def parser_consumer(self, consumer, producer):
        """
        Ingests a message from the Pipeline input topic and passes it to the consumer task
        """
        while True:
            msg = self._consume_from_topic(consumer)
            if msg:
                self.parser_task(msg, producer)
            else:
                self.logger.debug("No new messages")
                time.sleep(2)
                continue

    def parser_task(self, msg, producer):
        """
        input:
        msg: The consumed msg from the Pipeline input topic
        producer: The relevant Pipeline output producer

        The main consumer task for the Pipeline
        This task will take any consumed messages and pass them to the relevant subprocesses
        as well as updating postgres and redis.
        """
        self.logger.debug("Received message {}".format(msg.value()))

        job_request = msg.value()
        metadata_uuid = job_request.get("record_id")
        task = job_request.get("task")

        job_request["status"] = "Processing"
        db.write_status_redis(
            self.redis,
            json.dumps({"job_id": str(metadata_uuid), "status": job_request["status"]}),
        )

        if task == "REPARSE":
            db.update_job_status(self, job_request["record_id"], job_request["status"])
            parsing_handler.reparse_handler(self, job_request, producer)

        else:
            db.write_job_status(self, job_request)
            job_request["status"] = parsing_handler.parse_task_selector(
                self, job_request, producer
            )

        db.write_status_redis(
            self.redis,
            json.dumps({"job_id": str(job_request["record_id"]), "status": job_request["status"]}),
        )

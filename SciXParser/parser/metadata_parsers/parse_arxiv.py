import copy
import json
from datetime import datetime

from adsingestp.parsers.arxiv import ArxivParser
from SciXPipelineUtils import utils

from SciXParser.parser import db


def parse_store_arxiv_record(app, job_request, producer, reparse=False):

    record_id = job_request.get("record_id")
    s3_key = job_request.get("s3_path")
    task = job_request.get("task")
    metadata = job_request.get("record_xml")
    force = job_request.get("force", False)
    date = datetime.now()
    arxiv_parser = ArxivParser()
    parser_output_schema = utils.get_schema(
        app, app.schema_client, app.config.get("PARSER_OUTPUT_SCHEMA")
    )

    parsed_record = arxiv_parser.parse(metadata)
    app.logger.debug("Parsed record is: {}".format(parsed_record))

    if reparse:
        app.logger.debug("{}".format(parsed_record))
        with app.session_scope() as session:
            old_record = db.get_parser_record(session, record_id).parsed_data
        if old_record:
            old_record["recordData"].pop("parsedTime")
        test_record = copy.deepcopy(parsed_record)
        test_record["recordData"].pop("parsedTime")

        if old_record == test_record and force is not True:
            record_status = False
            status = "Unchanged"
        else:
            with app.session_scope() as session:
                record_status = db.update_parser_record_metadata(
                    session, record_id, date, parsed_record, app.logger
                )
    else:
        record_status = db.write_parser_record(app, record_id, date, s3_key, parsed_record, task)

    if record_status:
        producer_message = {}
        producer_message["parsed_record"] = parsed_record
        producer_message["record_id"] = str(record_id)

        try:
            producer.produce(
                topic=app.config.get("PARSER_OUTPUT_TOPIC"),
                value=producer_message,
                value_schema=parser_output_schema,
            )
            status = "Success"

        except Exception as e:
            app.logger.exception(
                "Failed to produce {} to Kafka topic: {}".format(
                    record_id, app.config.get("PARSER_OUTPUT_TOPIC")
                )
            )
            status = "Error"
            db.write_status_redis(
                app.redis, json.dumps({"job_id": str(job_request["record_id"]), "status": status})
            )
            db.update_job_status(app, job_request["record_id"], status)
            raise e

    db.write_status_redis(
        app.redis, json.dumps({"job_id": str(job_request["record_id"]), "status": status})
    )
    db.update_job_status(app, job_request["record_id"], status)

    return status

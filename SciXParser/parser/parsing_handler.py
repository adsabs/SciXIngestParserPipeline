import json
from datetime import datetime

from parser import db
from parser.metadata_parsers import parse_arxiv


def reparse_handler(app, job_request, producer):
    """
    Collects S3 information and prepares a compatible request for the task_selector
    """
    metadata_uuid = job_request.get("record_id")
    record_entry = db.get_parser_record(metadata_uuid)
    s3_path = record_entry.s3_path
    date = datetime.now()

    try:
        app.logger.info("Attempting to collect raw metadata from {} S3".format("AWS"))
        metadata = app.s3clients["AWS"].read_s3_object(s3_path)
    except Exception:
        db.collect_metadata_from_secondary_s3(app, s3_path, job_request, metadata_uuid)

    new_job_request = {
        "record_id": metadata_uuid,
        "record_xml": metadata,
        "s3_path": s3_path,
        "task": record_entry.source,
        "datetime": date,
    }

    return parse_task_selector(app, new_job_request, producer, reparse=True)


def parse_task_selector(app, job_request, producer, reparse=False):
    """
    Identifies the correct task and calls the appropriate parser
    """
    task = job_request.get("task")
    if task == "ARXIV":
        print("Record is arxiv")
        parse_arxiv.parse_store_arxiv_record(app, job_request, producer, reparse=reparse)

    else:
        app.logger.error("{} is not a valid data source. Stopping.".format(task))
        status = "Error"
        db.write_status_redis(app.redis, status)
        db.update_job_status(
            app, json.dumps({"job_id": job_request.get("record_id"), "status": status})
        )
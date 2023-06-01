from SciXParser.parser import db
from SciXParser.parser.metadata_parsers import parse_arxiv


def reparse_handler(app, job_request):
    """
    Collects S3 information and prepares a compatible request for the task_selector
    """
    metadata_uuid = job_request.get("record_id")
    record_entry = db.get_parser_record(metadata_uuid)
    s3_path = record_entry.s3_path
    try:
        metadata = app.s3clients["AWS"].read_s3_object(s3_path)
    except Exception:
        try:
            for client in app.s3clients:
                metadata = client.read_s3_object(s3_path)
                break
        except Exception:
            app.logger.error(
                "Unable to find an valid s3 object for {}. Stopping.".format(metadata_uuid)
            )
    return metadata


def parse_task_selector(app, job_request):
    """
    Identifies the correct task and calls the appropriate parser
    """
    task = job_request.get("task")

    if task == "ARXIV":
        parse_arxiv.parse_store_arxiv_record(job_request)

    else:
        app.logger.error("{} is not a valid data source. Stopping.".format(task))

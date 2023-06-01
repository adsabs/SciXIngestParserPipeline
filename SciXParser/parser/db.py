import datetime
import logging as logger

import parser.models as models

logger.basicConfig(level=logger.DEBUG)


def write_status_redis(redis_instance, status):
    logger.debug("Publishing status: {}".format(status))
    redis_instance.publish("PARSER_statuses", status)


def get_job_status_by_job_hash(cls, job_hashes, only_status=None):
    """
    Return all updates with job_hash
    """
    with cls.session_scope() as session:
        status = None
        logger.info("Opening Session")
        for job_hash in job_hashes:
            if only_status:
                record_db = (
                    session.query(models.gRPC_status)
                    .filter(models.gRPC_status.job_hash == job_hash)
                    .filter_by(status=only_status)
                    .first()
                )
            else:
                record_db = (
                    session.query(models.gRPC_status)
                    .filter(models.gRPC_status.job_hash == job_hash)
                    .first()
                )
            if record_db:
                status = record_db.status
                logger.info("{} has status: {}".format(record_db.job_hash, status))

    return status


def _get_job_by_job_hash(session, job_hash, only_status=None):
    """
    Return all updates with job_hash internal function
    """
    logger.info("Opening Session")

    if only_status:
        record_db = (
            session.query(models.gRPC_status)
            .filter(models.gRPC_status.job_hash == job_hash)
            .filter_by(status=only_status)
            .first()
        )
    else:
        record_db = (
            session.query(models.gRPC_status)
            .filter(models.gRPC_status.job_hash == job_hash)
            .first()
        )
    if record_db:
        logger.info("Found record: {}".format(record_db.job_hash))
    return record_db


def write_job_status(cls, job_request, only_status=None):
    """
    Write new status for job to db
    """
    with cls.session_scope() as session:
        job_status = models.gRPC_status()
        job_status.job_hash = job_request.get("hash")
        job_status.job_request = job_request.get("task")
        job_status.status = job_request.get("status")
        job_status.timestamp = datetime.datetime.now()
        session.add(job_status)
        session.commit()
    return True


def update_job_status(cls, job_hash, status=None):
    """
    Update status for job previously written to db
    """
    updated = False
    with cls.session_scope() as session:
        job_status = _get_job_by_job_hash(session, job_hash)
        if job_status:
            job_status.status = status
            job_status.timestamp = datetime.datetime.now()
            session.add(job_status)
            session.commit()
            updated = True
    return updated


def write_parser_record(cls, record_id, date, s3_key, parsed_metadata, source):
    """
    Write harvested record to db.
    """
    success = False
    with cls.session_scope() as session:
        PARSER_record = models.PARSER_record()
        PARSER_record.id = record_id
        PARSER_record.s3_key = s3_key
        PARSER_record.parsed_data = parsed_metadata
        PARSER_record.date_created = date
        PARSER_record.date_modified = date
        PARSER_record.source = source
        session.add(PARSER_record)
        session.commit()
        success = True
    return success


def update_parser_record_metadata(cls, record_id, date, parsed_metadata):
    """
    Write harvested record to db.
    """
    updated = False
    with cls.session_scope() as session:
        record_db = get_parser_record(session, record_id)
        if record_db:
            PARSER_record = models.PARSER_record()
            PARSER_record.id = record_db.record_id
            PARSER_record.s3_key = record_db.s3_key
            PARSER_record.parsed_data = parsed_metadata
            PARSER_record.date_created = record_db.date_created
            PARSER_record.date_modified = date
            PARSER_record.source = record_db.source
            session.add(PARSER_record)
            session.commit()
            updated = True
    return updated


def get_parser_record(session, record_id):
    """
    Return record with UUID: record_id
    """
    record_db = (
        session.query(models.PARSER_record).filter(models.PARSER_record.id == record_id).first()
    )
    return record_db

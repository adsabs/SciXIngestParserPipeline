import enum
import uuid

from sqlalchemy import JSON, Column, DateTime, Enum, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Status(enum.Enum):
    Pending = 1
    Processing = 2
    Error = 3
    Success = 4


class Source(enum.Enum):
    ARXIV = 1
    SYMBOL2 = 2
    SYMBOL3 = 3
    SYMBOL4 = 4


class gRPC_status(Base):
    """
    gRPC table
    table containing the given status of every job passed through the gRPC API
    """

    __tablename__ = "grpc_status"
    record_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_request = Column(String)
    status = Column(Enum(Status))
    date_added = Column(DateTime)
    date_of_last_success = Column(DateTime)
    date_of_last_attempt = Column(DateTime)


class PARSER_record(Base):
    """
    ArXiV records table
    table containing the relevant information for harvested arxiv records.
    """

    __tablename__ = "parser_records"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    s3_key = Column(String)
    date_created = Column(DateTime)
    date_modified = Column(DateTime)
    parsed_data = Column(JSON)
    source = Column(Enum(Source))

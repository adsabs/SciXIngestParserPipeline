"""Client and server classes for gRPC services."""
import grpc


class ParserInitStub(object):
    """The Stub for connecting to the Parser init service."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.initParser = channel.unary_stream(
            "/parseraapi.ParserInit/initParser",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class ParserInitServicer(object):
    """The servicer definition for initiating jobs with the Parser pipeline."""

    def initParser(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ParserInitServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
    rpc_method_handlers = {
        "initParser": grpc.unary_stream_rpc_method_handler(
            servicer.initParser,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "parseraapi.ParserInit", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


class ParserInit(object):
    """The definition of the Parser gRPC API and stream connections."""

    @staticmethod
    def initParser(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/parseraapi.ParserInit/initParser",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class ParserMonitorStub(object):
    """The Stub for connecting to the Parser Monitor service."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.monitorParser = channel.unary_stream(
            "/parseraapi.ParserMonitor/monitorParser",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class ParserMonitorServicer(object):
    """The servicer definition for monitoring jobs already submitted to the Parser pipeline."""

    def monitorParser(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_ParserMonitorServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
    rpc_method_handlers = {
        "monitorParser": grpc.unary_stream_rpc_method_handler(
            servicer.monitorParser,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "parseraapi.ParserMonitor", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


class ParserMonitor(object):
    """The definition of the Monitor gRPC API and stream connections."""

    @staticmethod
    def monitorParser(
        request,
        target,
        options=(),
        channel_credentials=None,
        call_credentials=None,
        insecure=False,
        compression=None,
        wait_for_ready=None,
        timeout=None,
        metadata=None,
    ):
        return grpc.experimental.unary_stream(
            request,
            target,
            "/parseraapi.ParserMonitor/monitorParser",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

"""Client and server classes for gRPC services."""
import grpc


class TemplateInitStub(object):
    """The Stub for connecting to the Template init service."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.initTemplate = channel.unary_stream(
            "/templateaapi.TemplateInit/initTemplate",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class TemplateInitServicer(object):
    """The servicer definition for initiating jobs with the Template pipeline."""

    def initTemplate(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_TemplateInitServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
    rpc_method_handlers = {
        "initTemplate": grpc.unary_stream_rpc_method_handler(
            servicer.initTemplate,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "templateaapi.TemplateInit", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


class TemplateInit(object):
    """The definition of the Template gRPC API and stream connections."""

    @staticmethod
    def initTemplate(
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
            "/templateaapi.TemplateInit/initTemplate",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )


class TemplateMonitorStub(object):
    """The Stub for connecting to the Template Monitor service."""

    def __init__(self, channel, avroserialhelper):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.monitorTemplate = channel.unary_stream(
            "/templateaapi.TemplateMonitor/monitorTemplate",
            request_serializer=avroserialhelper.avro_serializer,
            response_deserializer=avroserialhelper.avro_deserializer,
        )


class TemplateMonitorServicer(object):
    """The servicer definition for monitoring jobs already submitted to the Template pipeline."""

    def monitorTemplate(self, request, context):
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Method not implemented!")
        raise NotImplementedError("Method not implemented!")


def add_TemplateMonitorServicer_to_server(servicer, server, avroserialhelper):
    """The actual methods for sending and receiving RPC calls."""
    rpc_method_handlers = {
        "monitorTemplate": grpc.unary_stream_rpc_method_handler(
            servicer.monitorTemplate,
            request_deserializer=avroserialhelper.avro_deserializer,
            response_serializer=avroserialhelper.avro_serializer,
        ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
        "templateaapi.TemplateMonitor", rpc_method_handlers
    )
    server.add_generic_rpc_handlers((generic_handler,))


class TemplateMonitor(object):
    """The definition of the Monitor gRPC API and stream connections."""

    @staticmethod
    def monitorTemplate(
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
            "/templateaapi.TemplateMonitor/monitorTemplate",
            options,
            channel_credentials,
            insecure,
            call_credentials,
            compression,
            wait_for_ready,
            timeout,
            metadata,
        )

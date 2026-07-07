from queue import Queue
from types import SimpleNamespace
from unittest import TestCase

from thingsboard_gateway.connectors.request.request_connector import RequestConnector


class RequestConnectorSseTest(TestCase):
    def setUp(self):
        self.captured = []
        self.connector = object.__new__(RequestConnector)
        self.connector._RequestConnector__stopped = False
        self.connector._RequestConnector__convert_queue = Queue()
        self.connector._RequestConnector__convert_data = self.captured.append
        self.logger = SimpleNamespace(debug=lambda *args, **kwargs: None)
        self.request = {"converter": object()}

    def test_converts_each_sse_event_when_blank_line_is_received(self):
        response = _SseResponse(
            [
                "event: job-changed",
                'data: {"type":"job-changed","job_id":"abc"}',
                "",
                ": keepalive",
                "event: install-progress",
                'data: {"progress":42.0}',
                "",
            ]
        )

        RequestConnector._RequestConnector__process_sse_response(
            self.connector, response, "http://example/events", self.request, self.logger
        )

        self.assertEqual(2, len(self.captured))
        self.assertEqual(
            {"type": "job-changed", "job_id": "abc", "event": "job-changed"},
            self.captured[0][2],
        )
        self.assertEqual(
            {"progress": 42.0, "event": "install-progress"},
            self.captured[1][2],
        )

    def test_wraps_non_json_sse_data_as_value(self):
        RequestConnector._RequestConnector__convert_sse_event(
            self.connector,
            "http://example/events",
            self.request,
            "job-output",
            ["plain text"],
            self.logger,
        )

        self.assertEqual(
            {"value": "plain text", "event": "job-output"},
            self.captured[0][2],
        )

    def test_decodes_byte_lines_from_response(self):
        response = _SseResponse(
            [
                b"event: upload-progress",
                b'data: {"bytes":128}',
                b"",
            ]
        )

        RequestConnector._RequestConnector__process_sse_response(
            self.connector, response, "http://example/events", self.request, self.logger
        )

        self.assertEqual({"bytes": 128, "event": "upload-progress"}, self.captured[0][2])


class _SseResponse:
    encoding = "utf-8"

    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self, decode_unicode=True):
        yield from self._lines

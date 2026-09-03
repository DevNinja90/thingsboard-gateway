from queue import Queue
from threading import Event
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
        self.connector._RequestConnector__active_sse_streams = {}
        self.connector._log = SimpleNamespace(info=lambda *a, **k: None, error=lambda *a, **k: None)
        self.connector._connector_type = "request"
        self.connector._converter_log = SimpleNamespace()
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

    def test_server_closed_stream_stops_without_reconnect(self):
        response = _SseResponse(
            [
                "event: job-changed",
                'data: {"job_id":"abc"}',
                "",
                "event: install-progress",
                'data: {"progress":99.0}',
            ]
        )

        should_reconnect = RequestConnector._RequestConnector__process_sse_response(
            self.connector, response, "http://example/events", self.request, self.logger
        )

        self.assertFalse(should_reconnect)
        self.assertEqual(1, len(self.captured))
        self.assertEqual({"job_id": "abc", "event": "job-changed"}, self.captured[0][2])

    def test_parses_fields_according_to_event_stream_rules(self):
        response = _SseResponse(
            [
                "data",
                "",
                "data:test",
                "",
                "data: test",
                "",
                "data:  test",
                "",
            ]
        )

        RequestConnector._RequestConnector__process_sse_response(
            self.connector, response, "http://example/events", self.request, self.logger
        )

        self.assertEqual({"value": "", "event": "message"}, self.captured[0][2])
        self.assertEqual({"value": "test", "event": "message"}, self.captured[1][2])
        self.assertEqual({"value": "test", "event": "message"}, self.captured[2][2])
        self.assertEqual({"value": " test", "event": "message"}, self.captured[3][2])

    def test_tracks_last_event_id_and_retry_fields(self):
        response = _SseResponse(
            [
                "id: 42",
                "retry: 2500",
                "data: payload",
                "",
            ]
        )

        RequestConnector._RequestConnector__process_sse_response(
            self.connector, response, "http://example/events", self.request, self.logger
        )

        self.assertEqual("42", self.request["last_event_id"])
        self.assertEqual(2.5, self.request["sse_reconnect_period"])
        self.assertEqual({"value": "payload", "event": "message"}, self.captured[0][2])

    def test_stream_stopped_checks_stop_event_independent_of_connector_stopped(self):
        request = {"stop_event": Event()}

        self.assertFalse(RequestConnector._RequestConnector__stream_sse_stopped(self.connector, request))

        request["stop_event"].set()

        self.assertTrue(RequestConnector._RequestConnector__stream_sse_stopped(self.connector, request))
        self.assertFalse(self.connector._RequestConnector__stopped)

    def test_start_sse_stream_rpc_requires_url(self):
        result = RequestConnector._RequestConnector__start_sse_stream_rpc(self.connector, {})

        self.assertFalse(result["success"])
        self.assertIn("url", result["error"])

    def test_start_and_stop_sse_stream_rpc_lifecycle(self):
        started = Event()
        proceed = Event()

        def fake_stream(request, logger):
            started.set()
            proceed.wait(timeout=2)

        self.connector._RequestConnector__stream_sse_request = fake_stream

        result = RequestConnector._RequestConnector__start_sse_stream_rpc(
            self.connector, {"url": "http://example/events", "streamId": "custom-id"}
        )

        self.assertTrue(result["success"])
        self.assertEqual("custom-id", result["result"]["streamId"])
        self.assertTrue(started.wait(timeout=2))
        self.assertIn("custom-id", self.connector._RequestConnector__active_sse_streams)

        duplicate = RequestConnector._RequestConnector__start_sse_stream_rpc(
            self.connector, {"url": "http://example/events", "streamId": "custom-id"}
        )
        self.assertFalse(duplicate["success"])

        stream = self.connector._RequestConnector__active_sse_streams["custom-id"]
        stop_result = RequestConnector._RequestConnector__stop_sse_stream_rpc(
            self.connector, {"streamId": "custom-id"}
        )

        self.assertTrue(stop_result["success"])
        self.assertTrue(stream["stop_event"].is_set())

        proceed.set()
        stream["thread"].join(timeout=2)

        self.assertNotIn("custom-id", self.connector._RequestConnector__active_sse_streams)

    def test_stop_sse_stream_rpc_reports_unknown_stream(self):
        result = RequestConnector._RequestConnector__stop_sse_stream_rpc(self.connector, {"streamId": "missing"})

        self.assertFalse(result["success"])


class _SseResponse:
    encoding = "utf-8"

    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self, decode_unicode=True):
        yield from self._lines

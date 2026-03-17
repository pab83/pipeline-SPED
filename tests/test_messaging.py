"""Tests for messaging/redis_client.py."""

import json
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, date

from messaging.redis_client import RedisQueueClient


class TestRedisQueueClientPublish:
    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_publish(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_redis_cls.return_value = mock_r

        client = RedisQueueClient(host="localhost", port=6379)
        msg = {"key": "value"}
        client.publish("test_queue", msg)

        mock_r.lpush.assert_called_once()
        args = mock_r.lpush.call_args
        assert args[0][0] == "test_queue"
        parsed = json.loads(args[0][1])
        assert parsed["key"] == "value"

    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_publish_datetime_serialization(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_redis_cls.return_value = mock_r

        client = RedisQueueClient(host="localhost", port=6379)
        dt = datetime(2024, 6, 15, 12, 30, 0)
        msg = {"timestamp": dt}
        client.publish("test_queue", msg)

        args = mock_r.lpush.call_args
        parsed = json.loads(args[0][1])
        assert parsed["timestamp"] == "2024-06-15T12:30:00"

    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_publish_error(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_r.lpush.side_effect = Exception("Connection refused")
        mock_redis_cls.return_value = mock_r

        client = RedisQueueClient(host="localhost", port=6379)
        # Should not propagate the exception
        client.publish("test_queue", {"key": "value"})


class TestRedisQueueClientConsume:
    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_consume_single_message(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_redis_cls.return_value = mock_r

        msg_data = {"message_id": "123", "result": "ok"}
        # First call returns data, second call raises to stop the loop
        mock_r.brpop.side_effect = [
            (b"queue", json.dumps(msg_data).encode()),
            Exception("stop"),
        ]

        client = RedisQueueClient(host="localhost", port=6379)
        callback = MagicMock()

        # consume runs an infinite loop; the second brpop raises Exception
        # which is caught internally, then we need another way to stop.
        # Let's make the third call raise KeyboardInterrupt via side_effect
        mock_r.brpop.side_effect = [
            (b"queue", json.dumps(msg_data).encode()),
            KeyboardInterrupt("stop"),
        ]

        with pytest.raises(KeyboardInterrupt):
            client.consume("test_queue", callback)

        callback.assert_called_once_with(msg_data)


class TestJsonSerializer:
    @patch("messaging.redis_client.redis.Redis")
    def test_json_serializer_datetime(self, mock_redis_cls):
        mock_redis_cls.return_value = MagicMock()
        client = RedisQueueClient(host="localhost", port=6379)
        dt = datetime(2024, 1, 1, 0, 0, 0)
        assert client._json_serializer(dt) == "2024-01-01T00:00:00"

    @patch("messaging.redis_client.redis.Redis")
    def test_json_serializer_date(self, mock_redis_cls):
        mock_redis_cls.return_value = MagicMock()
        client = RedisQueueClient(host="localhost", port=6379)
        d = date(2024, 6, 15)
        assert client._json_serializer(d) == "2024-06-15"

    @patch("messaging.redis_client.redis.Redis")
    def test_json_serializer_unsupported(self, mock_redis_cls):
        mock_redis_cls.return_value = MagicMock()
        client = RedisQueueClient(host="localhost", port=6379)
        with pytest.raises(TypeError):
            client._json_serializer(42)


class TestRedisQueueClientDLQ:
    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_send_to_dlq(self, mock_redis_cls):
        mock_r = MagicMock()
        mock_redis_cls.return_value = mock_r

        client = RedisQueueClient(host="localhost", port=6379)
        msg = {"error": "failed"}
        client.send_to_dlq("dlq_test", msg)

        mock_r.lpush.assert_called_once()
        args = mock_r.lpush.call_args
        assert args[0][0] == "dlq_test"


class TestRedisQueueClientAck:
    @patch("messaging.redis_client.redis.Redis")
    def test_redis_client_ack_noop(self, mock_redis_cls):
        mock_redis_cls.return_value = MagicMock()
        client = RedisQueueClient(host="localhost", port=6379)
        # ack is a no-op, should not raise
        client.ack("some-message-id")

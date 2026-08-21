import asyncio

import pytest

from astrbot.api.event import MessageChain
from astrbot.api.message_components import Plain
from astrbot.core.platform.sources.webchat import webchat_event
from astrbot.core.platform.sources.webchat.webchat_queue_mgr import WebChatQueueMgr
from astrbot.dashboard.services.chat_service import poll_webchat_stream_result


class _QueueThatRaises:
    def __init__(self, exc: BaseException):
        self._exc = exc

    async def get(self):
        raise self._exc


class _QueueWithResult:
    def __init__(self, result):
        self._result = result

    async def get(self):
        return self._result


@pytest.mark.asyncio
async def test_poll_webchat_stream_result_breaks_on_cancelled_error():
    result, should_break = await poll_webchat_stream_result(
        _QueueThatRaises(asyncio.CancelledError()),
        "alice",
    )

    assert result is None
    assert should_break is True


@pytest.mark.asyncio
async def test_poll_webchat_stream_result_continues_on_generic_exception():
    result, should_break = await poll_webchat_stream_result(
        _QueueThatRaises(RuntimeError("boom")),
        "alice",
    )

    assert result is None
    assert should_break is False


@pytest.mark.asyncio
async def test_poll_webchat_stream_result_returns_queue_payload():
    payload = {"type": "end", "data": ""}

    result, should_break = await poll_webchat_stream_result(
        _QueueWithResult(payload),
        "alice",
    )

    assert result == payload
    assert should_break is False


@pytest.mark.asyncio
async def test_removing_back_queue_unblocks_waiting_producer():
    manager = WebChatQueueMgr(back_queue_maxsize=1)
    queue = manager.get_or_create_back_queue("request", "conversation")
    await queue.put({"data": "first"})
    blocked_put = asyncio.create_task(queue.put({"data": "second"}))
    await asyncio.sleep(0)

    assert not blocked_put.done()

    manager.remove_back_queue("request")
    await asyncio.wait_for(blocked_put, timeout=1)

    assert queue.empty()
    assert manager.get_back_queue("request") is None


@pytest.mark.asyncio
async def test_webchat_event_drops_output_after_client_disconnect(monkeypatch):
    manager = WebChatQueueMgr(back_queue_maxsize=1)
    manager.get_or_create_back_queue("request", "conversation")
    manager.remove_back_queue("request")
    monkeypatch.setattr(webchat_event, "webchat_queue_mgr", manager)

    result = await webchat_event.WebChatMessageEvent._send(
        "request",
        MessageChain([Plain("late output")]),
        "webchat!alice!conversation",
        streaming=True,
    )

    assert result is None
    assert manager.get_back_queue("request") is None

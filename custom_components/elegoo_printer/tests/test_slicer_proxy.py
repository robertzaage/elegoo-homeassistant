"""
Tests for the V1 (MQTT) slicer proxy.

The bridge tests run two real instances of the embedded broker: one plays
Home Assistant's broker (the printer is connected to it), the other plays
the slicer's own broker that a slicer opens before sending M66666.
"""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

import aiomqtt
import pytest
from aiohttp import ClientSession, web

from custom_components.elegoo_printer.mqtt.server import ElegooMQTTBroker
from custom_components.elegoo_printer.mqtt.slicer_proxy import (
    SLICER_NAME_SUFFIX,
    FileRelay,
    SlicerBroker,
    SlicerIdentity,
    SlicerProxy,
    _preview,
    alias_id,
    parse_connect_message,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

MAINBOARD_ID = "428fa2e552d20100"
CONNECTION_ID = "f25273b12b094c5a8b9513a30ca60049"
ALIAS_ID = alias_id(MAINBOARD_ID)
PRINTER_IP = "192.0.2.10"
PROXY_IP = "192.0.2.20"
SLICER_IP = "192.0.2.30"


def _topic(kind: str, mainboard_id: str = MAINBOARD_ID) -> str:
    return f"/sdcp/{kind}/{mainboard_id}"


def _request(
    cmd: int,
    request_id: str,
    data: dict[str, Any] | None = None,
    mainboard_id: str = ALIAS_ID,
) -> str:
    """Build a request frame as a slicer addresses it (to the proxy's identity)."""
    return json.dumps(
        {
            "Id": alias_id(CONNECTION_ID),
            "Data": {
                "Cmd": cmd,
                "Data": data or {},
                "RequestID": request_id,
                "MainboardID": mainboard_id,
                "TimeStamp": 0,
                "From": 1,
            },
            "Topic": f"sdcp/request/{mainboard_id}",
        }
    )


# ----------------------------------------------------------------------
# Parsing helpers
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("M66666 192.0.2.30 1883", SlicerBroker("192.0.2.30", 1883)),
        (
            "M66666 192.0.2.30 1883 user secret",
            SlicerBroker("192.0.2.30", 1883, "user", "secret"),
        ),
        ("M66666 1883", SlicerBroker(SLICER_IP, 1883)),
        ("M66666", None),
        ("M66666 host notaport", None),
        ("M66666 host 70000", None),
        ("M99999", None),
    ],
)
def test_parse_connect_message(message: str, expected: SlicerBroker | None) -> None:
    """Both the current and the legacy M66666 forms are understood."""
    assert parse_connect_message(message, SLICER_IP) == expected


def test_alias_id_is_stable_distinct_and_shaped_like_the_original() -> None:
    """Slicers remember printers by ID, so the stand-in must not change."""
    assert alias_id(MAINBOARD_ID) == alias_id(MAINBOARD_ID)
    assert alias_id(MAINBOARD_ID) != MAINBOARD_ID
    assert len(alias_id(MAINBOARD_ID)) == len(MAINBOARD_ID)
    assert len(alias_id("a" * 100)) == 100
    int(alias_id(CONNECTION_ID), 16)  # hex, like the printer's IDs


def _printer_discovery_answer() -> dict[str, Any]:
    return {
        "Id": CONNECTION_ID,
        "Data": {
            "Attributes": {
                "Name": "Mars 4 Ultra",
                "MachineName": "ELEGOO Mars 4 Ultra",
                "MainboardIP": PRINTER_IP,
                "MainboardID": MAINBOARD_ID,
                "ProtocolVersion": "V1.0.0",
                "SDCPStatus": 1,
                "LocalSDCPAddress": f"tcp://{PROXY_IP}:18830",
            },
            "Status": {"CurrentStatus": [0]},
        },
    }


def test_discovery_answer_presents_a_separate_printer() -> None:
    """Own IDs and name, Home Assistant's address; everything else unchanged."""
    proxy = _proxy()
    answer = json.loads(
        proxy.discovery_answer(json.dumps(_printer_discovery_answer()).encode())
    )
    attributes = answer["Data"]["Attributes"]
    assert answer["Id"] == alias_id(CONNECTION_ID)
    assert attributes["MainboardID"] == ALIAS_ID
    assert attributes["MainboardIP"] == PROXY_IP
    assert attributes["Name"] == f"Mars 4 Ultra{SLICER_NAME_SUFFIX}"
    assert attributes["MachineName"] == "ELEGOO Mars 4 Ultra"
    assert answer["Data"]["Status"] == {"CurrentStatus": [0]}
    # Free for a slicer, although the printer itself is linked to Home Assistant.
    assert attributes["SDCPStatus"] == 0
    assert attributes["LocalSDCPAddress"] == ""


def test_discovery_answer_stays_free_while_bridged() -> None:
    """A slicer retrying with a new broker port must not see a busy printer."""
    proxy = _proxy()
    proxy.identity.slicer_link = SlicerBroker(SLICER_IP, 49610)
    answer = json.loads(
        proxy.discovery_answer(json.dumps(_printer_discovery_answer()).encode())
    )
    attributes = answer["Data"]["Attributes"]
    assert attributes["SDCPStatus"] == 0
    assert attributes["LocalSDCPAddress"] == ""


def test_bridged_attributes_report_the_slicer_as_client() -> None:
    """Inside the bridge the slicer sees itself as the printer's client."""
    identity = SlicerIdentity(MAINBOARD_ID, PROXY_IP)
    identity.slicer_link = SlicerBroker(SLICER_IP, 49610)
    frame = {
        "Data": {
            "Attributes": {"SDCPStatus": 1, "LocalSDCPAddress": "tcp://ha:18830"},
        }
    }
    attributes = json.loads(identity.for_slicer(json.dumps(frame)))["Data"][
        "Attributes"
    ]
    assert attributes["SDCPStatus"] == 1
    assert attributes["LocalSDCPAddress"] == f"tcp://{SLICER_IP}:49610"


@pytest.mark.parametrize(
    "raw", [b"not json", b'{"Data": {}}', b'{"Data": {"Attributes": 1}}']
)
def test_discovery_answer_rejects_other_payloads(raw: bytes) -> None:
    """Anything that is not a printer's discovery answer is not relayed."""
    assert _proxy().discovery_answer(raw) is None


def test_identity_round_trip() -> None:
    """IDs are translated both ways, also inside longer strings."""
    identity = SlicerIdentity(MAINBOARD_ID, PROXY_IP)
    identity.learn(CONNECTION_ID)
    printer_frame = {
        "Id": CONNECTION_ID,
        "Data": {"MainboardID": MAINBOARD_ID, "Status": {"CurrentStatus": [1]}},
        "Topic": f"sdcp/status/{MAINBOARD_ID}",
    }
    for_slicer = json.loads(identity.for_slicer(json.dumps(printer_frame)))
    assert MAINBOARD_ID not in json.dumps(for_slicer)
    assert CONNECTION_ID not in json.dumps(for_slicer)
    assert for_slicer["Topic"] == f"sdcp/status/{ALIAS_ID}"
    assert json.loads(identity.for_printer(json.dumps(for_slicer))) == printer_frame


def test_preview_masks_credentials() -> None:
    """Credential-like values never reach the debug log."""
    text = _preview('{"Password": "hunter2", "AccessKey": "k", "Name": "x"}')
    assert "hunter2" not in text
    assert '"AccessKey": "***"' in text
    assert '"Name": "x"' in text


# ----------------------------------------------------------------------
# UDP handling
# ----------------------------------------------------------------------


class _FakeTransport:
    def __init__(self) -> None:
        self.sent: list[tuple[bytes, tuple[str, int]]] = []

    def sendto(self, data: bytes, addr: tuple[str, int]) -> None:
        self.sent.append((data, addr))

    def close(self) -> None:
        pass


def _proxy(broker_port: int = 0) -> SlicerProxy:
    return SlicerProxy(
        printer_ip=PRINTER_IP,
        mainboard_id=MAINBOARD_ID,
        proxy_ip=PROXY_IP,
        broker_port=broker_port,
    )


async def _settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


async def test_discovery_relays_printer_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A slicer's M99999 gets the printer's own answer, pointing at the proxy."""
    proxy = _proxy()
    transport = _FakeTransport()
    proxy._transport = transport  # type: ignore[assignment]

    async def _query() -> bytes:
        return json.dumps(_printer_discovery_answer()).encode()

    monkeypatch.setattr(proxy, "_query_printer", _query)
    proxy._on_datagram(b"M99999", (SLICER_IP, 50000))
    await _settle()

    assert len(transport.sent) == 1
    data, addr = transport.sent[0]
    assert addr == (SLICER_IP, 50000)
    attributes = json.loads(data)["Data"]["Attributes"]
    assert attributes["MainboardIP"] == PROXY_IP
    assert attributes["MainboardID"] == ALIAS_ID


async def test_discovery_uses_cached_answer_when_printer_is_silent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A slow printer does not hide it from the slicer once it answered before."""
    proxy = _proxy()
    transport = _FakeTransport()
    proxy._transport = transport  # type: ignore[assignment]
    proxy._cached_discovery = json.dumps(
        {"Data": {"Attributes": {"MainboardIP": PRINTER_IP}}}
    ).encode()

    async def _silent() -> None:
        return None

    monkeypatch.setattr(proxy, "_query_printer", _silent)
    proxy._on_datagram(b"M99999", (SLICER_IP, 50000))
    await _settle()

    assert len(transport.sent) == 1


@pytest.mark.parametrize("source", [PRINTER_IP, PROXY_IP])
async def test_own_and_printer_datagrams_are_ignored(source: str) -> None:
    """Home Assistant's own discovery broadcast must not be answered by the proxy."""
    proxy = _proxy()
    transport = _FakeTransport()
    proxy._transport = transport  # type: ignore[assignment]
    proxy._on_datagram(b"M99999", (source, 3000))
    proxy._on_datagram(b"M66666 192.0.2.30 1883", (source, 3000))
    await _settle()

    assert transport.sent == []
    assert proxy._bridge_task is None


# ----------------------------------------------------------------------
# MQTT bridge
# ----------------------------------------------------------------------


@pytest.fixture
async def brokers():
    """Start Home Assistant's and the slicer's broker on free ports."""
    ha_broker = ElegooMQTTBroker(host="127.0.0.1", port=0)
    slicer_broker = ElegooMQTTBroker(host="127.0.0.1", port=0)
    await ha_broker.start()
    await slicer_broker.start()
    yield ha_broker, slicer_broker
    await ha_broker.stop()
    await slicer_broker.stop()


async def _next(client: aiomqtt.Client, wait: float = 3.0) -> tuple[str, dict]:
    message = await asyncio.wait_for(anext(aiter(client.messages)), wait)
    return str(message.topic), json.loads(message.payload)


async def _skip_connect_requests(printer: aiomqtt.Client) -> None:
    """Consume the attribute and status requests the bridge sends on connect."""
    for _ in range(2):
        await _next(printer)


async def _wait_for(predicate: Callable[[], bool], wait: float = 3.0) -> None:
    step = 0.02
    for _ in range(int(wait / step)):
        if predicate():
            return
        await asyncio.sleep(step)
    msg = "condition not reached"
    raise TimeoutError(msg)


async def test_bridge_passes_traffic_both_ways(brokers) -> None:
    """Requests reach the printer, printer messages reach the slicer."""
    ha_broker, slicer_broker = brokers
    proxy = _proxy(ha_broker.port)

    async with (
        aiomqtt.Client("127.0.0.1", ha_broker.port, identifier="printer") as printer,
        aiomqtt.Client("127.0.0.1", slicer_broker.port, identifier="slicer") as slicer,
    ):
        await printer.subscribe(_topic("request"))
        await slicer.subscribe(_topic("status", ALIAS_ID))
        await slicer.subscribe(_topic("response", ALIAS_ID))

        proxy._start_bridge(SlicerBroker("127.0.0.1", slicer_broker.port))
        await _wait_for(lambda: proxy.slicer_connected)
        await _skip_connect_requests(printer)

        # Slicer → printer
        await slicer.publish(_topic("request", ALIAS_ID), _request(1, "req-attr"))
        topic, frame = await _next(printer)
        assert topic == _topic("request")
        assert frame["Data"]["RequestID"] == "req-attr"
        assert frame["Data"]["MainboardID"] == MAINBOARD_ID
        assert frame["Topic"] == f"sdcp/request/{MAINBOARD_ID}"

        # Printer → slicer
        status = {
            "Id": "s",
            "Data": {"Status": {"CurrentStatus": [0]}, "MainboardID": MAINBOARD_ID},
        }
        await printer.publish(_topic("status"), json.dumps(status))
        topic, frame = await _next(slicer)
        assert topic == _topic("status", ALIAS_ID)
        assert frame["Data"]["MainboardID"] == ALIAS_ID
        assert frame["Data"]["Status"] == status["Data"]["Status"]

        await proxy.stop()
        assert not proxy.slicer_connected


async def test_bridge_answers_period_and_disconnect_itself(brokers) -> None:
    """512 and 64 never reach the printer; the slicer still gets its ack."""
    ha_broker, slicer_broker = brokers
    proxy = _proxy(ha_broker.port)

    async with (
        aiomqtt.Client("127.0.0.1", ha_broker.port, identifier="printer") as printer,
        aiomqtt.Client("127.0.0.1", slicer_broker.port, identifier="slicer") as slicer,
    ):
        await printer.subscribe(_topic("request"))
        await slicer.subscribe(_topic("response", ALIAS_ID))

        proxy._start_bridge(SlicerBroker("127.0.0.1", slicer_broker.port))
        await _wait_for(lambda: proxy.slicer_connected)
        await _skip_connect_requests(printer)

        await slicer.publish(
            _topic("request", ALIAS_ID),
            _request(512, "req-period", {"TimePeriod": 5000}),
        )
        topic, frame = await _next(slicer)
        assert topic == _topic("response", ALIAS_ID)
        assert frame["Data"]["MainboardID"] == ALIAS_ID
        assert frame["Data"]["Cmd"] == 512
        assert frame["Data"]["RequestID"] == "req-period"
        assert frame["Data"]["Data"] == {"Ack": 0}

        await slicer.publish(_topic("request", ALIAS_ID), _request(64, "req-bye"))
        topic, frame = await _next(slicer)
        assert frame["Data"]["Cmd"] == 64
        await _wait_for(lambda: not proxy.slicer_connected)

        # Neither request reached the printer.
        with pytest.raises(TimeoutError):
            await _next(printer, wait=0.3)

        await proxy.stop()


async def test_bridge_replays_latest_state_to_a_new_slicer(brokers) -> None:
    """A slicer gets attributes and status right away, as from a real printer."""
    ha_broker, slicer_broker = brokers
    proxy = _proxy(ha_broker.port)
    proxy._latest = {
        "attributes": json.dumps({"Data": {"Attributes": {"Name": "Mars"}}}),
        "status": json.dumps({"Data": {"Status": {"CurrentStatus": [0]}}}),
    }

    async with aiomqtt.Client(
        "127.0.0.1", slicer_broker.port, identifier="slicer"
    ) as slicer:
        await slicer.subscribe(_topic("attributes", ALIAS_ID))
        await slicer.subscribe(_topic("status", ALIAS_ID))
        proxy._start_bridge(SlicerBroker("127.0.0.1", slicer_broker.port))

        received = dict([await _next(slicer), await _next(slicer)])
        assert set(received) == {
            _topic("attributes", ALIAS_ID),
            _topic("status", ALIAS_ID),
        }
        name = received[_topic("attributes", ALIAS_ID)]["Data"]["Attributes"]["Name"]
        assert name == f"Mars{SLICER_NAME_SUFFIX}"

        await proxy.stop()


async def test_bridge_asks_the_printer_for_attributes_on_connect(brokers) -> None:
    """Slicers wait for attributes; a real printer sends them on connect."""
    ha_broker, slicer_broker = brokers
    proxy = _proxy(ha_broker.port)

    async with (
        aiomqtt.Client("127.0.0.1", ha_broker.port, identifier="printer") as printer,
        aiomqtt.Client("127.0.0.1", slicer_broker.port, identifier="slicer") as slicer,
    ):
        await printer.subscribe(_topic("request"))
        await slicer.subscribe(_topic("attributes", ALIAS_ID))
        proxy._start_bridge(SlicerBroker("127.0.0.1", slicer_broker.port))

        commands = [(await _next(printer))[1]["Data"]["Cmd"] for _ in range(2)]
        assert commands == [1, 0]

        attributes = {"Data": {"Attributes": {"Name": "Mars"}}}
        await printer.publish(_topic("attributes"), json.dumps(attributes))
        topic, frame = await _next(slicer)
        assert topic == _topic("attributes", ALIAS_ID)
        assert frame["Data"]["Attributes"]["Name"] == f"Mars{SLICER_NAME_SUFFIX}"

        await proxy.stop()


# ----------------------------------------------------------------------
# File relay
# ----------------------------------------------------------------------

FILE_BODY = bytes(range(256)) * 512  # 128 KiB, more than one relay chunk


@pytest.fixture
async def slicer_file_server(tmp_path: Path):
    """Play a slicer's file server: serves FILE_BODY at /SlicerFile/model.ctb."""
    model = tmp_path / "model.ctb"
    model.write_bytes(FILE_BODY)

    async def _file(_request: web.Request) -> web.FileResponse:
        return web.FileResponse(model)  # supports Range, like a slicer's file server

    app = web.Application()
    app.router.add_get("/SlicerFile/model.ctb", _file)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    yield runner.addresses[0][1]
    await runner.cleanup()


def test_relay_leaves_urls_alone_until_started() -> None:
    """Without a listening relay the printer keeps the slicer's URL."""
    relay = FileRelay("127.0.0.1")
    url = "http://${ipaddr}:8165/SlicerFile/model.ctb"
    assert relay.rewrite_url(url) == url
    assert relay.rewrite_url(None) is None


async def test_relay_streams_announced_files_only(slicer_file_server: int) -> None:
    """The printer gets the slicer's file through Home Assistant, nothing else."""
    relay = FileRelay("127.0.0.1")
    await relay.start()
    try:
        url = f"http://${{ipaddr}}:{slicer_file_server}/SlicerFile/model.ctb"
        relayed = relay.rewrite_url(url)
        assert relayed == (
            f"http://${{ipaddr}}:{relay.port}/{slicer_file_server}/SlicerFile/model.ctb"
        )
        # The plain URL from a slicer with its own address is not touched.
        assert relay.rewrite_url("http://192.0.2.30:8165/a.ctb") == (
            "http://192.0.2.30:8165/a.ctb"
        )

        as_printer = relayed.replace("${ipaddr}", "127.0.0.1")
        async with ClientSession() as session:
            async with session.get(as_printer) as response:
                assert response.status == 200
                assert await response.read() == FILE_BODY
            async with session.get(as_printer, headers={"Range": "bytes=10-19"}) as r:
                assert r.status == 206
                assert await r.read() == FILE_BODY[10:20]
            other = f"http://127.0.0.1:{relay.port}/{slicer_file_server}/etc/passwd"
            async with session.get(other) as response:
                assert response.status == 404
    finally:
        await relay.stop()


async def test_bridge_sends_uploads_through_the_relay(brokers) -> None:
    """Cmd 256 reaches the printer with a URL on Home Assistant's relay."""
    ha_broker, slicer_broker = brokers
    proxy = _proxy(ha_broker.port)

    async with (
        aiomqtt.Client("127.0.0.1", ha_broker.port, identifier="printer") as printer,
        aiomqtt.Client("127.0.0.1", slicer_broker.port, identifier="slicer") as slicer,
    ):
        await printer.subscribe(_topic("request"))
        proxy._start_bridge(SlicerBroker("127.0.0.1", slicer_broker.port))
        await _wait_for(lambda: proxy.slicer_connected)
        await _skip_connect_requests(printer)
        await _wait_for(lambda: proxy._relay is not None and proxy._relay.port > 0)

        upload = {
            "Filename": "model.ctb",
            "FileSize": 1,
            "URL": "http://${ipaddr}:8165/SlicerFile/model.ctb",
        }
        await slicer.publish(_topic("request", ALIAS_ID), _request(256, "up", upload))
        _, frame = await _next(printer)
        relay_port = proxy._relay.port
        assert frame["Data"]["Data"]["URL"] == (
            f"http://${{ipaddr}}:{relay_port}/8165/SlicerFile/model.ctb"
        )
        assert frame["Data"]["MainboardID"] == MAINBOARD_ID

        await proxy.stop()
        assert proxy._relay is None


async def test_upload_keeps_the_files_preview_as_cover(tmp_path: Path) -> None:
    """The proxy reads the preview of every file a slicer sends."""
    from custom_components.elegoo_printer.tests.test_ctb_preview import (  # noqa: PLC0415
        _encrypted_ctb,
        _png_pixels,
    )

    (tmp_path / "model.ctb").write_bytes(_encrypted_ctb())
    app = web.Application()
    app.router.add_static("/SlicerFile", tmp_path)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]

    proxy = _proxy()
    proxy._relay = FileRelay("127.0.0.1")
    await proxy._relay.start()
    try:
        upload = _request(
            256,
            "up",
            {
                "Filename": "model.ctb",
                "URL": f"http://${{ipaddr}}:{port}/SlicerFile/model.ctb",
            },
        )
        proxy._relay_upload(proxy.identity.for_printer(upload))
        await _wait_for(lambda: proxy.cover is not None)
        assert proxy.cover.filename == "model.ctb"
        assert _png_pixels(proxy.cover.png)[:2] == (6, 4)
    finally:
        await proxy.stop()
        await proxy._relay.stop()
        await runner.cleanup()

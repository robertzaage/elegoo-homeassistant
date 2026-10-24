"""
Slicer proxy for V1 (MQTT) printers.

A V1 printer (e.g. Mars 4 Ultra, Saturn 3/4 Ultra) holds exactly one MQTT
broker connection. Home Assistant's embedded broker keeps it, so a slicer such
as ChiTuBox that asks the printer to connect to its own broker (M66666) is
refused with "connection limit is reached".

The proxy offers the printer to slicers a second time, under its own identity:

1. Discovery: a slicer broadcasts ``M99999`` to UDP port 3000. The proxy asks
   the real printer the same question and relays its answer as a separate
   printer: own ``MainboardID`` and connection ``Id`` (stable stand-ins derived
   from the real ones), the name suffixed with "(Home Assistant)" and
   ``MainboardIP`` pointing at Home Assistant. With the printer's own IDs,
   ChiTuBox merges both answers into one entry and keeps the real printer.
2. Connect: the slicer sends ``M66666 <host> <port> [user] [password]`` to that
   address. The proxy then connects to the slicer's broker as the printer
   would.
3. Bridge: requests from the slicer go to the printer through the embedded
   broker; everything the printer publishes reaches Home Assistant and the
   slicer alike. Identifiers are translated on the way, so the slicer only
   ever sees the stand-ins and the printer only its real IDs. File transfers
   need no help: the printer downloads the file from the URL the slicer sends.

Two requests are answered by the proxy instead of the printer, because they
would change the printer's link for Home Assistant too: ``Disconnect`` (64)
would drop Home Assistant's connection, and ``Set status update period`` (512)
would change the push interval Home Assistant relies on.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aiomqtt

from custom_components.elegoo_printer.const import DISCOVERY_MESSAGE, DISCOVERY_PORT
from custom_components.elegoo_printer.sdcp.const import (
    CMD_DISCONNECT,
    CMD_SET_STATUS_UPDATE_PERIOD,
)

from .const import (
    MQTT_KEEPALIVE,
    TOPIC_ATTRIBUTES,
    TOPIC_ERROR,
    TOPIC_NOTICE,
    TOPIC_PREFIX,
    TOPIC_REQUEST,
    TOPIC_RESPONSE,
    TOPIC_STATUS,
)

if TYPE_CHECKING:
    from collections.abc import Callable

_LOGGER = logging.getLogger(__name__)

CONNECT_MESSAGE = "M66666"
SLICER_NAME_SUFFIX = " (Home Assistant)"
PRINTER_QUERY_TIMEOUT = 2.0
DISCOVERY_RATE_LIMIT_SECONDS = 1.0
LOG_PREVIEW_LENGTH = 600
PRINTER_TOPICS = (
    TOPIC_STATUS,
    TOPIC_ATTRIBUTES,
    TOPIC_RESPONSE,
    TOPIC_NOTICE,
    TOPIC_ERROR,
)
# Commands answered by the proxy instead of being passed to the printer.
LOCALLY_ANSWERED_CMDS = (CMD_DISCONNECT, CMD_SET_STATUS_UPDATE_PERIOD)

_SENSITIVE_KEY = re.compile(
    r'("[^"]*(?:pass|key|token|secret)[^"]*"\s*:\s*)"[^"]*"', re.IGNORECASE
)


def _preview(payload: str) -> str:
    """Shorten a payload for the debug log and mask credential-like values."""
    masked = _SENSITIVE_KEY.sub(r'\1"***"', payload)
    if len(masked) > LOG_PREVIEW_LENGTH:
        return f"{masked[:LOG_PREVIEW_LENGTH]}… ({len(masked)} chars)"
    return masked


def alias_id(value: str) -> str:
    """
    Return the stand-in the slicer sees for a printer identifier.

    Stable across restarts (slicers remember printers by ID), same length and
    hex alphabet as the printer's IDs, and never equal to the original.
    """
    digest = ""
    seed = f"elegoo-slicer-proxy:{value}"
    while len(digest) < len(value):
        seed = hashlib.sha256(seed.encode()).hexdigest()
        digest += seed
    return digest[: len(value)]


@dataclass(frozen=True)
class SlicerBroker:
    """Where the slicer asked the printer to connect (from M66666)."""

    host: str
    port: int
    username: str | None = None
    password: str | None = None


def parse_connect_message(message: str, source_ip: str) -> SlicerBroker | None:
    """
    Parse ``M66666 <host> <port> [user] [password]`` or legacy ``M66666 <port>``.

    The legacy form has no host; the printer uses the sender's address.
    """
    parts = message.split()
    if not parts or parts[0] != CONNECT_MESSAGE or len(parts) < 2:  # noqa: PLR2004
        return None
    if len(parts) == 2:  # noqa: PLR2004
        host, port_raw, extra = source_ip, parts[1], []
    else:
        host, port_raw, extra = parts[1], parts[2], parts[3:]
    try:
        port = int(port_raw)
    except ValueError:
        return None
    if not 1 <= port <= 65535:  # noqa: PLR2004
        return None
    return SlicerBroker(
        host=host,
        port=port,
        username=extra[0] if extra else None,
        password=extra[1] if len(extra) > 1 else None,
    )


class SlicerIdentity:
    """Translates between the printer's identity and the one slicers see."""

    def __init__(self, mainboard_id: str, proxy_ip: str) -> None:
        """Start with the MainboardID; the connection Id is learned later."""
        self.mainboard_id = mainboard_id
        self.alias_mainboard_id = alias_id(mainboard_id)
        self.proxy_ip = proxy_ip
        self._to_alias: dict[str, str] = {mainboard_id: self.alias_mainboard_id}
        self._to_real: dict[str, str] = {self.alias_mainboard_id: mainboard_id}

    def learn(self, value: Any) -> None:
        """Add another printer identifier (the connection ``Id``)."""
        if isinstance(value, str) and value and value not in self._to_alias:
            alias = alias_id(value)
            self._to_alias[value] = alias
            self._to_real[alias] = value

    def for_slicer(self, payload: str) -> str:
        """Rewrite a printer message the way the slicer must see it."""
        return self._translate(payload, to_slicer=True)

    def for_printer(self, payload: str) -> str:
        """Rewrite a slicer message the way the printer must see it."""
        return self._translate(payload, to_slicer=False)

    def _translate(self, payload: str, *, to_slicer: bool) -> str:
        try:
            frame = json.loads(payload)
        except ValueError:
            mapping = self._to_alias if to_slicer else self._to_real
            for old, new in mapping.items():
                payload = payload.replace(old, new)
            return payload
        return json.dumps(self._walk(frame, to_slicer=to_slicer, parent=""))

    def _walk(self, value: Any, *, to_slicer: bool, parent: str) -> Any:
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                if to_slicer and key == "MainboardIP":
                    result[key] = self.proxy_ip
                elif (
                    to_slicer
                    and key == "Name"
                    and parent == "Attributes"
                    and isinstance(item, str)
                    and not item.endswith(SLICER_NAME_SUFFIX)
                ):
                    result[key] = f"{item}{SLICER_NAME_SUFFIX}"
                else:
                    result[key] = self._walk(item, to_slicer=to_slicer, parent=key)
            return result
        if isinstance(value, list):
            return [
                self._walk(item, to_slicer=to_slicer, parent=parent) for item in value
            ]
        if isinstance(value, str):
            # IDs also appear inside strings ("Topic": "sdcp/request/<id>").
            # They are long random hex strings, so replacing substrings is safe.
            mapping = self._to_alias if to_slicer else self._to_real
            for old, new in mapping.items():
                value = value.replace(old, new)
            return value
        return value


class _UdpProtocol(asyncio.DatagramProtocol):
    """Hands datagrams to a callback."""

    def __init__(self, on_datagram: Callable[[bytes, tuple[str, int]], None]) -> None:
        self._on_datagram = on_datagram
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self._on_datagram(data, addr)


class SlicerProxy:
    """Offers a V1 MQTT printer to slicers while Home Assistant stays connected."""

    def __init__(  # noqa: PLR0913
        self,
        *,
        printer_ip: str,
        mainboard_id: str,
        proxy_ip: str,
        broker_port: int,
        broker_host: str = "127.0.0.1",
        logger: logging.Logger = _LOGGER,
    ) -> None:
        """
        Create the proxy; call ``start`` to listen.

        Arguments:
            printer_ip: The real printer's address.
            mainboard_id: The printer's MainboardID (topic suffix).
            proxy_ip: The LAN address slicers should use for the printer
                (Home Assistant's address, as given to the printer in M66666).
            broker_port: Port of the embedded broker the printer is on.
            broker_host: Address of that broker as seen from Home Assistant.
            logger: Logger to use.

        """
        self.printer_ip = printer_ip
        self.mainboard_id = mainboard_id
        self.proxy_ip = proxy_ip
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.logger = logger
        self.identity = SlicerIdentity(mainboard_id, proxy_ip)
        self._transport: asyncio.DatagramTransport | None = None
        self._bridge_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[Any]] = set()
        self._last_discovery: dict[str, float] = {}
        self._cached_discovery: bytes | None = None
        self._latest: dict[str, str] = {}
        self.slicer: SlicerBroker | None = None

    @property
    def slicer_connected(self) -> bool:
        """Return True while a slicer is bridged to the printer."""
        return self.slicer is not None

    def _printer_topic(self, kind: str) -> str:
        # Leading slash: matches the printer's own topics.
        return f"/{TOPIC_PREFIX}/{kind}/{self.mainboard_id}"

    def _slicer_topic(self, kind: str) -> str:
        return f"/{TOPIC_PREFIX}/{kind}/{self.identity.alias_mainboard_id}"

    async def start(self) -> bool:
        """Listen for slicers on UDP 3000; False if the port is taken."""
        loop = asyncio.get_running_loop()
        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _UdpProtocol(self._on_datagram),
                local_addr=("0.0.0.0", DISCOVERY_PORT),  # noqa: S104
                allow_broadcast=True,
            )
        except OSError as err:
            self.logger.error(  # noqa: TRY400  # the cause is the message
                "Slicer proxy cannot listen on UDP port %s (%s); slicers will"
                " not find the printer through Home Assistant",
                DISCOVERY_PORT,
                err,
            )
            return False
        self._transport = transport  # type: ignore[assignment]
        self.logger.info(
            "Slicer proxy for printer %s listening on UDP %s (printer at %s,"
            " offered to slicers as %s with ID %s)",
            self.mainboard_id,
            DISCOVERY_PORT,
            self.printer_ip,
            self.proxy_ip,
            self.identity.alias_mainboard_id,
        )
        return True

    async def stop(self) -> None:
        """Stop listening and close any slicer bridge."""
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        tasks = [*self._tasks, *([self._bridge_task] if self._bridge_task else [])]
        for task in tasks:
            task.cancel()
        for task in tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        self._bridge_task = None
        self.slicer = None

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ------------------------------------------------------------------
    # UDP: discovery and connect requests
    # ------------------------------------------------------------------

    def _on_datagram(self, data: bytes, addr: tuple[str, int]) -> None:
        source_ip = addr[0]
        # Our own and the printer's broadcasts arrive here too.
        if source_ip in (self.printer_ip, self.proxy_ip):
            return
        try:
            message = data.decode().strip()
        except UnicodeDecodeError:
            return
        if message == DISCOVERY_MESSAGE:
            now = time.monotonic()
            last = self._last_discovery.get(source_ip, 0.0)
            if now - last < DISCOVERY_RATE_LIMIT_SECONDS:
                return
            self._last_discovery[source_ip] = now
            self.logger.debug("Slicer proxy: discovery from %s", source_ip)
            self._spawn(self._answer_discovery(addr))
        elif message.startswith(CONNECT_MESSAGE):
            broker = parse_connect_message(message, source_ip)
            if broker is None:
                self.logger.warning(
                    "Slicer proxy: ignoring malformed connect request from %s",
                    source_ip,
                )
                return
            self.logger.info(
                "Slicer proxy: %s asks the printer to connect to its broker %s:%s",
                source_ip,
                broker.host,
                broker.port,
            )
            self._start_bridge(broker)
        else:
            self.logger.debug(
                "Slicer proxy: unhandled datagram from %s: %s",
                source_ip,
                _preview(message),
            )

    def discovery_answer(self, reply: bytes) -> bytes | None:
        """Turn the printer's M99999 answer into the proxy's; None if invalid."""
        try:
            payload = json.loads(reply)
            attributes = payload["Data"]["Attributes"]
        except (ValueError, KeyError, TypeError):
            return None
        if not isinstance(attributes, dict):
            return None
        self.identity.learn(payload.get("Id"))
        return self.identity.for_slicer(json.dumps(payload)).encode()

    async def _answer_discovery(self, addr: tuple[str, int]) -> None:
        reply = await self._query_printer()
        if reply is not None:
            self._cached_discovery = reply
        elif self._cached_discovery is not None:
            self.logger.debug("Slicer proxy: printer silent, using cached answer")
            reply = self._cached_discovery
        else:
            self.logger.debug("Slicer proxy: printer silent, no answer to relay")
            return
        answer = self.discovery_answer(reply)
        if answer is None or self._transport is None:
            return
        self._transport.sendto(answer, addr)
        self.logger.debug(
            "Slicer proxy: answered discovery from %s: %s",
            addr[0],
            _preview(answer.decode()),
        )

    async def _query_printer(self) -> bytes | None:
        """Ask the real printer M99999 directly and return its raw answer."""
        loop = asyncio.get_running_loop()
        answer: asyncio.Future[bytes] = loop.create_future()

        def _on_answer(data: bytes, addr: tuple[str, int]) -> None:
            if addr[0] == self.printer_ip and not answer.done():
                answer.set_result(data)

        try:
            transport, _ = await loop.create_datagram_endpoint(
                lambda: _UdpProtocol(_on_answer),
                local_addr=("0.0.0.0", 0),  # noqa: S104
            )
        except OSError as err:
            self.logger.debug("Slicer proxy: cannot query printer: %s", err)
            return None
        try:
            transport.sendto(
                DISCOVERY_MESSAGE.encode(), (self.printer_ip, DISCOVERY_PORT)
            )
            return await asyncio.wait_for(answer, PRINTER_QUERY_TIMEOUT)
        except TimeoutError:
            return None
        finally:
            transport.close()

    # ------------------------------------------------------------------
    # MQTT bridge
    # ------------------------------------------------------------------

    def _start_bridge(self, broker: SlicerBroker) -> None:
        previous = self._bridge_task
        self._bridge_task = asyncio.create_task(self._run_bridge(broker, previous))

    async def _run_bridge(
        self, broker: SlicerBroker, previous: asyncio.Task[None] | None
    ) -> None:
        if previous is not None and not previous.done():
            self.logger.info("Slicer proxy: replacing the previous slicer bridge")
            previous.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await previous
        try:
            async with (
                aiomqtt.Client(
                    hostname=self.broker_host,
                    port=self.broker_port,
                    identifier=f"slicer-proxy-{self.mainboard_id}",
                    keepalive=MQTT_KEEPALIVE,
                ) as printer_side,
                aiomqtt.Client(
                    hostname=broker.host,
                    port=broker.port,
                    username=broker.username,
                    password=broker.password,
                    identifier=self.identity.alias_mainboard_id,
                    keepalive=MQTT_KEEPALIVE,
                ) as slicer_side,
            ):
                self.slicer = broker
                self.logger.info(
                    "Slicer proxy: bridging slicer broker %s:%s to printer %s",
                    broker.host,
                    broker.port,
                    self.mainboard_id,
                )
                for kind in PRINTER_TOPICS:
                    await printer_side.subscribe(self._printer_topic(kind))
                await slicer_side.subscribe(self._slicer_topic(TOPIC_REQUEST))
                # A printer publishes its attributes and status on connect.
                for kind in (TOPIC_ATTRIBUTES, TOPIC_STATUS):
                    if kind in self._latest:
                        await slicer_side.publish(
                            self._slicer_topic(kind),
                            self.identity.for_slicer(self._latest[kind]),
                        )
                await self._pump(printer_side, slicer_side)
        except aiomqtt.MqttError as err:
            self.logger.info("Slicer proxy: slicer bridge closed: %s", err)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Slicer proxy: slicer bridge failed")
        finally:
            self.slicer = None
            self.logger.info("Slicer proxy: slicer disconnected")

    async def _pump(
        self, printer_side: aiomqtt.Client, slicer_side: aiomqtt.Client
    ) -> None:
        """Run both directions until one side ends or the slicer says goodbye."""
        to_slicer = asyncio.create_task(
            self._printer_to_slicer(printer_side, slicer_side)
        )
        to_printer = asyncio.create_task(
            self._slicer_to_printer(slicer_side, printer_side)
        )
        try:
            done, _ = await asyncio.wait(
                {to_slicer, to_printer}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()  # surface MqttError from either side
        finally:
            for task in (to_slicer, to_printer):
                task.cancel()
            for task in (to_slicer, to_printer):
                with contextlib.suppress(asyncio.CancelledError, aiomqtt.MqttError):
                    await task

    async def _printer_to_slicer(
        self, printer_side: aiomqtt.Client, slicer_side: aiomqtt.Client
    ) -> None:
        async for message in printer_side.messages:
            topic = str(message.topic)
            payload = _as_text(message.payload)
            kind = topic.split("/")[2] if topic.count("/") >= 3 else ""  # noqa: PLR2004
            if kind not in PRINTER_TOPICS:
                continue
            if kind in (TOPIC_ATTRIBUTES, TOPIC_STATUS):
                self._latest[kind] = payload
            translated = self.identity.for_slicer(payload)
            self.logger.debug(
                "Slicer proxy: printer → slicer %s %s", kind, _preview(translated)
            )
            await slicer_side.publish(self._slicer_topic(kind), translated)

    async def _slicer_to_printer(
        self, slicer_side: aiomqtt.Client, printer_side: aiomqtt.Client
    ) -> None:
        async for message in slicer_side.messages:
            payload = _as_text(message.payload)
            self.logger.debug(
                "Slicer proxy: slicer → printer %s %s",
                message.topic,
                _preview(payload),
            )
            request = _parse_request(payload)
            cmd = request.get("Cmd") if request else None
            if cmd in LOCALLY_ANSWERED_CMDS and request is not None:
                await slicer_side.publish(
                    self._slicer_topic(TOPIC_RESPONSE), self._ack(payload, request)
                )
                self.logger.debug("Slicer proxy: answered command %s locally", cmd)
                if cmd == CMD_DISCONNECT:
                    return
                continue
            await printer_side.publish(
                self._printer_topic(TOPIC_REQUEST), self.identity.for_printer(payload)
            )

    def _ack(self, raw: str, request: dict[str, Any]) -> str:
        """Build the success response the printer would send, as the slicer sees it."""
        try:
            frame_id = json.loads(raw).get("Id", "")
        except ValueError:
            frame_id = ""
        return json.dumps(
            {
                "Id": frame_id,
                "Data": {
                    "Cmd": request.get("Cmd"),
                    "Data": {"Ack": 0},
                    "RequestID": request.get("RequestID", ""),
                    "MainboardID": self.identity.alias_mainboard_id,
                    "TimeStamp": int(time.time()),
                },
            }
        )


def _as_text(payload: Any) -> str:
    if isinstance(payload, (bytes, bytearray)):
        return payload.decode(errors="replace")
    return str(payload)


def _parse_request(payload: str) -> dict[str, Any] | None:
    """Return the ``Data`` part of a request frame, or None."""
    try:
        data = json.loads(payload).get("Data")
    except (ValueError, AttributeError):
        return None
    return data if isinstance(data, dict) else None

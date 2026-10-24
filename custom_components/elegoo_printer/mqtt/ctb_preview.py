"""
Preview image of a sliced ChiTuBox file (CTB/CBDDLP), as PNG.

V1 (MQTT) printers send no thumbnail, but every job sent through the slicer
proxy passes Home Assistant as a URL. The preview sits near the start of the
file, so a few ranged requests are enough; the file is never downloaded.

Two layouts exist (field positions as documented by UVtools,
https://github.com/sn4k3/UVtools):

* CTB/CBDDLP: the header holds the large preview's offset at byte 60.
* Encrypted CTB (current ChiTuBox): the slicer settings are encrypted, but
  the previews are not. They follow the settings block directly, whose
  offset and size are in the plain 48-byte file header. No key is needed.

At that offset a preview header (width, height, data offset, data length)
precedes the image, a 16-bit RGB555 run-length encoding.
"""

from __future__ import annotations

import struct
import zlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aiohttp import ClientSession

MAGIC_CBDDLP = 0x12FD0019
MAGIC_CTB = 0x12FD0086
MAGIC_CTB_V4 = 0x12FD0106
MAGIC_CTB_ENCRYPTED = 0x12FD0107
PLAIN_MAGICS = (MAGIC_CBDDLP, MAGIC_CTB, MAGIC_CTB_V4)

HEADER_READ = 80  # covers both header layouts
PLAIN_PREVIEW_OFFSET_AT = 60
PREVIEW_HEADER_SIZE = 16
MAX_PREVIEW_SIDE = 2048
MAX_PREVIEW_BYTES = 4 * 1024 * 1024
_REPEAT_FLAG = 0x20


class PreviewError(ValueError):
    """The file has no preview this module can read."""


def preview_header_offset(header: bytes) -> int:
    """Return where the large preview's header starts, from the file header."""
    if len(header) < HEADER_READ:
        msg = "file header too short"
        raise PreviewError(msg)
    magic = struct.unpack_from("<I", header, 0)[0]
    if magic == MAGIC_CTB_ENCRYPTED:
        settings_size, settings_offset = struct.unpack_from("<II", header, 4)
        return settings_offset + settings_size
    if magic in PLAIN_MAGICS:
        return struct.unpack_from("<I", header, PLAIN_PREVIEW_OFFSET_AT)[0]
    msg = f"unknown file magic 0x{magic:08X}"
    raise PreviewError(msg)


def parse_preview_header(data: bytes, at: int) -> tuple[int, int, int, int]:
    """Return width, height, data offset and data length; checked for sanity."""
    if len(data) < PREVIEW_HEADER_SIZE:
        msg = "preview header too short"
        raise PreviewError(msg)
    width, height, offset, length = struct.unpack_from("<IIII", data, 0)
    if not (0 < width <= MAX_PREVIEW_SIDE and 0 < height <= MAX_PREVIEW_SIDE):
        msg = f"implausible preview size {width}x{height}"
        raise PreviewError(msg)
    if not 0 < length <= MAX_PREVIEW_BYTES or offset < at + PREVIEW_HEADER_SIZE:
        msg = f"implausible preview data at {offset} ({length} bytes)"
        raise PreviewError(msg)
    return width, height, offset, length


def decode_rgb15_rle(data: bytes, width: int, height: int) -> bytes:
    """
    Decode ChiTuBox's preview encoding to 8-bit RGB.

    Each pixel is a little-endian 16-bit word: 5 bits each of red, green and
    blue (bits 11-15, 6-10, 0-4). Bit 5 marks a run: the next two bytes add a
    12-bit repeat count.
    """
    size = width * height * 3
    out = bytearray(size)
    pos = 0
    i = 0
    end = len(data) - 1
    while i < end:
        dot = data[i] | data[i + 1] << 8
        i += 2
        repeat = 1
        if dot & _REPEAT_FLAG:
            if i + 1 >= len(data):
                break
            repeat += data[i] | (data[i + 1] & 0x0F) << 8
            i += 2
        pixel = bytes(
            (((dot >> 11) & 0x1F) << 3, ((dot >> 6) & 0x1F) << 3, (dot & 0x1F) << 3)
        )
        count = min(repeat, (size - pos) // 3)
        out[pos : pos + count * 3] = pixel * count
        pos += count * 3
        if pos >= size:
            break
    return bytes(out)


def encode_png(width: int, height: int, rgb: bytes) -> bytes:
    """Encode 8-bit RGB as PNG with the standard library."""

    def chunk(kind: bytes, body: bytes) -> bytes:
        return (
            struct.pack(">I", len(body))
            + kind
            + body
            + struct.pack(">I", zlib.crc32(kind + body) & 0xFFFFFFFF)
        )

    stride = width * 3
    raw = b"".join(
        b"\x00" + rgb[row * stride : (row + 1) * stride] for row in range(height)
    )
    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw, 6))
        + chunk(b"IEND", b"")
    )


async def fetch_preview_png(session: ClientSession, url: str) -> bytes:
    """Read the large preview of the file at ``url`` with ranged requests."""

    async def read(start: int, length: int) -> bytes:
        headers = {"Range": f"bytes={start}-{start + length - 1}"}
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            body = await response.read()
        if len(body) > length:  # server ignored the range
            body = body[start : start + length]
        return body

    at = preview_header_offset(await read(0, HEADER_READ))
    width, height, offset, length = parse_preview_header(
        await read(at, PREVIEW_HEADER_SIZE), at
    )
    rgb = decode_rgb15_rle(await read(offset, length), width, height)
    return encode_png(width, height, rgb)

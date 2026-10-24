"""
Tests for reading preview images from sliced ChiTuBox files.

The test files follow the layouts UVtools writes (and documents): an
encrypted CTB, whose previews follow the encrypted settings block, and a
plain CTB, whose header points at the preview.
"""

from __future__ import annotations

import struct
import zlib
from typing import TYPE_CHECKING

import pytest
from aiohttp import ClientSession, web

from custom_components.elegoo_printer.mqtt.ctb_preview import (
    MAGIC_CTB,
    MAGIC_CTB_ENCRYPTED,
    PreviewError,
    decode_rgb15_rle,
    encode_png,
    fetch_preview_png,
    parse_preview_header,
    preview_header_offset,
)

if TYPE_CHECKING:
    from pathlib import Path

WIDTH, HEIGHT = 6, 4
# Two colour bands, so the encoding needs runs: 12 red, then 12 blue pixels.
RED, BLUE = (248, 0, 0), (0, 0, 248)
PIXELS = [RED] * 12 + [BLUE] * 12


def _encode_rle(pixels: list[tuple[int, int, int]]) -> bytes:
    """Encode like ChiTuBox: RGB555 words, runs flagged with bit 5."""
    out = bytearray()
    i = 0
    while i < len(pixels):
        run = 1
        while i + run < len(pixels) and pixels[i + run] == pixels[i] and run < 4096:
            run += 1
        r, g, b = (c >> 3 for c in pixels[i])
        dot = r << 11 | g << 6 | b
        if run > 1:
            out += struct.pack("<H", dot | 0x20)
            out += bytes(((run - 1) & 0xFF, (run - 1) >> 8 & 0x0F))
        else:
            out += struct.pack("<H", dot)
        i += run
    return bytes(out)


def _preview_block(at: int) -> bytes:
    data = _encode_rle(PIXELS)
    return struct.pack("<IIII", WIDTH, HEIGHT, at + 16, len(data)) + data


def _encrypted_ctb() -> bytes:
    settings_offset, settings_size = 48, 300  # settings: opaque to us
    header = struct.pack(
        "<IIIIIIII", MAGIC_CTB_ENCRYPTED, settings_size, settings_offset, 0, 5, 0, 0, 0
    ) + bytes(16)
    at = settings_offset + settings_size
    return header + bytes(settings_size) + _preview_block(at) + bytes(64)


def _plain_ctb() -> bytes:
    at = 112
    header = bytearray(at)
    struct.pack_into("<I", header, 0, MAGIC_CTB)
    struct.pack_into("<I", header, 60, at)
    return bytes(header) + _preview_block(at) + bytes(16)


def _png_pixels(png: bytes) -> tuple[int, int, bytes]:
    """Read width, height and RGB rows back from our own PNG."""
    assert png.startswith(b"\x89PNG\r\n\x1a\n")
    width, height = struct.unpack(">II", png[16:24])
    idat_len = struct.unpack(">I", png[33:37])[0]
    raw = zlib.decompress(png[41 : 41 + idat_len])
    stride = width * 3 + 1
    rows = b"".join(raw[r * stride + 1 : (r + 1) * stride] for r in range(height))
    return width, height, rows


EXPECTED_RGB = b"".join(bytes(p) for p in PIXELS)


def test_decode_matches_the_encoded_image() -> None:
    """Runs and single pixels decode to the original colours."""
    assert decode_rgb15_rle(_encode_rle(PIXELS), WIDTH, HEIGHT) == EXPECTED_RGB


def test_decode_never_writes_past_the_image() -> None:
    """A run longer than the image is cut off, not an overflow."""
    too_long = _encode_rle([RED] * 100)
    assert decode_rgb15_rle(too_long, WIDTH, HEIGHT) == bytes(RED) * 24


def test_png_round_trip() -> None:
    """The PNG holds exactly the decoded pixels."""
    assert _png_pixels(encode_png(WIDTH, HEIGHT, EXPECTED_RGB)) == (
        WIDTH,
        HEIGHT,
        EXPECTED_RGB,
    )


@pytest.mark.parametrize(
    ("file", "expected"), [(_encrypted_ctb(), 348), (_plain_ctb(), 112)]
)
def test_preview_offset_for_both_layouts(file: bytes, expected: int) -> None:
    """Encrypted files need no key: the offset comes from the plain header."""
    assert preview_header_offset(file[:80]) == expected


def test_unknown_files_are_refused() -> None:
    """Another format is reported, not misread."""
    with pytest.raises(PreviewError, match="unknown file magic"):
        preview_header_offset(b"PK\x03\x04" + bytes(76))


@pytest.mark.parametrize(
    "header",
    [
        struct.pack("<IIII", 0, 4, 116, 10),  # no width
        struct.pack("<IIII", 5000, 4, 116, 10),  # absurd width
        struct.pack("<IIII", 6, 4, 50, 10),  # data before the header
        struct.pack("<IIII", 6, 4, 116, 0),  # no data
    ],
)
def test_implausible_preview_headers_are_refused(header: bytes) -> None:
    """Garbage at the computed offset is not decoded."""
    with pytest.raises(PreviewError):
        parse_preview_header(header, 100)


@pytest.fixture
async def file_server(tmp_path: Path):
    """Serve both test files with Range support, like a slicer's file server."""
    (tmp_path / "enc.ctb").write_bytes(_encrypted_ctb())
    (tmp_path / "plain.ctb").write_bytes(_plain_ctb())
    app = web.Application()
    app.router.add_static("/SlicerFile", tmp_path)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    yield f"http://127.0.0.1:{runner.addresses[0][1]}/SlicerFile"
    await runner.cleanup()


@pytest.mark.parametrize("name", ["enc.ctb", "plain.ctb"])
async def test_fetch_preview_png(file_server: str, name: str) -> None:
    """Three ranged requests turn a sliced file into its preview PNG."""
    async with ClientSession() as session:
        png = await fetch_preview_png(session, f"{file_server}/{name}")
    assert _png_pixels(png) == (WIDTH, HEIGHT, EXPECTED_RGB)

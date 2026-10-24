"""
Tests for job details of V1 (MQTT) printers.

V1 printers keep no task history, so the current job is built from the
status, and the file transfer they report is exposed as its own sensors.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from custom_components.elegoo_printer.definitions import (
    PRINTER_STATUS_V1,
    V1_UNSUPPORTED_SENSOR_KEYS,
    _get_begin_time,
)
from custom_components.elegoo_printer.mqtt.client import ElegooMQTTClient
from custom_components.elegoo_printer.sdcp.models.enums import (
    PrinterType,
    ProtocolVersion,
)
from custom_components.elegoo_printer.sdcp.models.status import (
    FileTransferInfo,
    PrinterStatus,
)
from custom_components.elegoo_printer.tests.test_sensor_registration import (
    _registered_keys,
)

FILE = "model.ctb"
V1_KEYS = {desc.key for desc in PRINTER_STATUS_V1}


def _status(
    *,
    current_status: int = 1,
    filename: str = FILE,
    ticks: int = 60_000,
    total: int = 600_000,
    transfer: dict | None = None,
) -> PrinterStatus:
    """Build a status as a Mars 4 Ultra sends it (CurrentStatus 1 = printing)."""
    return PrinterStatus(
        {
            "Status": {
                "CurrentStatus": current_status,
                "PrintInfo": {
                    "Status": 3,
                    "CurrentLayer": 10,
                    "TotalLayer": 100,
                    "CurrentTicks": ticks,
                    "TotalTicks": total,
                    "Filename": filename,
                },
                "FileTransferInfo": transfer or {},
            }
        },
        PrinterType.RESIN,
    )


def _client() -> ElegooMQTTClient:
    return ElegooMQTTClient(mqtt_host="127.0.0.1", logger=MagicMock())


async def _job(client: ElegooMQTTClient, status: PrinterStatus):
    client.printer_data.status = status
    return await client.async_get_printer_current_task()


# ----------------------------------------------------------------------
# File transfer
# ----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("data", "state", "progress"),
    [
        ({}, "idle", None),
        (
            {"Status": 0, "DownloadOffset": 0, "FileTotalSize": 0, "Filename": ""},
            "idle",
            None,
        ),
        (
            {
                "Status": 0,
                "DownloadOffset": 1000,
                "FileTotalSize": 4000,
                "Filename": FILE,
            },
            "transferring",
            25.0,
        ),
        (
            {
                "Status": 2,
                "DownloadOffset": 4000,
                "FileTotalSize": 4000,
                "Filename": FILE,
            },
            "completed",
            100.0,
        ),
        (
            {"Status": 3, "DownloadOffset": 0, "FileTotalSize": 4000, "Filename": FILE},
            "failed",
            0.0,
        ),
    ],
)
def test_file_transfer_info(data: dict, state: str, progress: float | None) -> None:
    """States as observed on a Mars 4 Ultra (Status stays 0 while downloading)."""
    info = FileTransferInfo(data)
    assert info.state == state
    assert info.progress == progress


def test_status_parses_file_transfer_info() -> None:
    """The status message's FileTransferInfo reaches the model."""
    status = _status(
        transfer={
            "Status": 2,
            "DownloadOffset": 5,
            "FileTotalSize": 5,
            "Filename": FILE,
        }
    )
    assert status.file_transfer_info.state == "completed"


# ----------------------------------------------------------------------
# Current job from the status
# ----------------------------------------------------------------------


async def test_running_print_becomes_the_current_job() -> None:
    """A print without TaskId still gets a job, named after the file."""
    job = await _job(_client(), _status())
    assert job is not None
    assert job.task_name == FILE
    assert job.begin_time is None  # derived from the elapsed time by the sensor


async def test_job_survives_progress_and_completion() -> None:
    """The same print keeps its job, also after it finished."""
    client = _client()
    first = await _job(client, _status(ticks=60_000))
    later = await _job(client, _status(ticks=120_000))
    done = await _job(client, _status(current_status=0, ticks=600_000))
    assert first is later is done


@pytest.mark.parametrize(
    "next_status",
    [
        _status(filename="other.ctb", ticks=5_000),
        _status(ticks=1_000),  # the same file printed again
    ],
)
async def test_new_print_starts_a_new_job(next_status: PrinterStatus) -> None:
    """Another file, or the elapsed time going back, means a new job."""
    client = _client()
    first = await _job(client, _status(ticks=60_000))
    second = await _job(client, next_status)
    assert second is not None
    assert second is not first
    assert second.task_id != first.task_id


async def test_finished_print_after_restart_gets_no_job() -> None:
    """A print that ended before Home Assistant started has no knowable begin."""
    assert await _job(_client(), _status(current_status=0, ticks=600_000)) is None


async def test_begin_and_end_time_are_derived() -> None:
    """Begin = now - elapsed, end = now + remaining, rounded to the minute."""
    client = _client()
    data = client.printer_data
    data.current_job = await _job(client, _status(ticks=60_000, total=600_000))
    data.calculate_current_job_end_time()

    now = datetime.now(UTC)
    begin = _get_begin_time(data)
    assert begin is not None
    assert abs(begin - (now - timedelta(minutes=1))) <= timedelta(minutes=1)
    assert abs(data.current_job.end_time - (now + timedelta(minutes=9))) <= (
        timedelta(minutes=1)
    )


# ----------------------------------------------------------------------
# Sensor registration
# ----------------------------------------------------------------------


def test_v1_printers_get_file_transfer_and_no_task_history_sensors() -> None:
    """V1 gets the transfer sensors; sensors needing task history are left out."""
    keys = _registered_keys(PrinterType.RESIN, ProtocolVersion.V1, open_centauri=False)
    assert keys >= V1_KEYS
    assert not keys & V1_UNSUPPORTED_SENSOR_KEYS
    assert {"begin_time", "end_time"} <= keys


def test_v3_printers_are_unchanged() -> None:
    """WebSocket printers keep their sensors and get no transfer sensors."""
    keys = _registered_keys(PrinterType.RESIN, ProtocolVersion.V3, open_centauri=False)
    assert not keys & V1_KEYS
    assert keys >= V1_UNSUPPORTED_SENSOR_KEYS

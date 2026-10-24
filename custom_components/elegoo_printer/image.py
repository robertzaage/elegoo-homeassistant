"""Image platform."""

from __future__ import annotations

from typing import TYPE_CHECKING

from homeassistant.components.image import Image, ImageEntity

from custom_components.elegoo_printer.entity import ElegooPrinterEntity
from custom_components.elegoo_printer.sdcp.models.enums import ProtocolVersion

from .const import LOGGER
from .definitions import PRINTER_IMAGES

if TYPE_CHECKING:
    from datetime import datetime

    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback

    from custom_components.elegoo_printer.coordinator import ElegooDataUpdateCoordinator
    from custom_components.elegoo_printer.definitions import (
        ElegooPrinterSensorEntityDescription,
    )


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """
    Asynchronously sets up Elegoo Printer image entities for a Home Assistant config entry.

    Cover thumbnails come from the task history of V3 (WebSocket/SDCP) and CC2
    printers. V1 (MQTT) printers have none; with the slicer proxy on, their
    cover is the preview image of the file a slicer sent through it.
    """  # noqa: E501
    coordinator: ElegooDataUpdateCoordinator = config_entry.runtime_data.coordinator
    protocol_version = (
        coordinator.config_entry.runtime_data.api.printer.protocol_version
    )

    if (
        protocol_version == ProtocolVersion.V1
        and coordinator.config_entry.runtime_data.api.slicer_proxy is not None
    ):
        async_add_entities(
            [SlicerCoverImage(hass, coordinator, image) for image in PRINTER_IMAGES]
        )
        return

    # V1 (MQTT) printers don't have async_get_task or async_get_thumbnail_image
    if protocol_version not in (ProtocolVersion.V3, ProtocolVersion.CC2):
        LOGGER.debug(
            "Skipping image entities for non-V3/CC2 printer (protocol: %s)",
            protocol_version,
        )
        return

    LOGGER.debug(f"Adding {len(PRINTER_IMAGES)} image entities")
    for image in PRINTER_IMAGES:
        async_add_entities(
            [CoverImage(hass, coordinator, image)],
            update_before_add=True,
        )


class CoverImage(ElegooPrinterEntity, ImageEntity):
    """Representation of an image entity."""

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: ElegooDataUpdateCoordinator,
        description: ElegooPrinterSensorEntityDescription,
    ) -> None:
        """
        Initialize a CoverImage entity for the Elegoo Printer.

        Sets up the entity with the provided Home Assistant instance, data coordinator, and entity description.
        Assigns a unique ID, sets the content type to PNG, and records the initial image update timestamp.
        """  # noqa: E501
        super().__init__(coordinator)
        ImageEntity.__init__(self, hass=hass)
        self.coordinator = coordinator
        self._image_filename = None
        self.image_url = None
        self._cached_task_id: str | None = (
            None  # Track which task the cached image is for
        )
        self._cached_image: Image | None = None
        self.entity_description = description
        unique_id = coordinator.generate_unique_id(self.entity_description.key)
        self._attr_unique_id = unique_id
        self._attr_image_last_updated: datetime | None = None
        self.api = coordinator.config_entry.runtime_data.api

    async def async_image(self) -> bytes | None:
        """Return bytes of an image."""
        task = await self.api.async_get_task(include_last_task=False)

        # Check if we need to fetch a new thumbnail
        # V3 printers reuse same URL (/thumb.jpg) but file changes per print
        # Compare task_id to detect new prints even with identical URLs
        if task and (
            task.task_id != self._cached_task_id or task.thumbnail != self.image_url
        ):
            if thumbnail_image := await self.api.async_get_thumbnail_image(task=task):
                self._attr_image_last_updated = thumbnail_image.get_last_update_time()
                self._cached_image = thumbnail_image.get_image()
                self.image_url = task.thumbnail
                self._cached_task_id = (
                    task.task_id
                )  # Cache task ID to detect new prints
                self._attr_content_type = thumbnail_image.get_content_type()
                return thumbnail_image.get_bytes()
            # Fetch failed but we have a cached image - return it instead of None
            # Better to show stale image briefly than nothing during transient errors
            if self._cached_image:
                return self._cached_image.content

        elif self._cached_image:
            return self._cached_image.content

        return None


class SlicerCoverImage(ElegooPrinterEntity, ImageEntity):
    """
    Cover image of a V1 printer: the preview of the file a slicer sent.

    Shown while the printer's current file is the one that came through the
    slicer proxy; the proxy keeps it in memory, so it is gone after a restart.
    """

    _attr_content_type = "image/png"

    def __init__(
        self,
        hass: HomeAssistant,
        coordinator: ElegooDataUpdateCoordinator,
        description: ElegooPrinterSensorEntityDescription,
    ) -> None:
        """Initialize the cover image of a V1 printer."""
        super().__init__(coordinator)
        ImageEntity.__init__(self, hass=hass)
        self.entity_description = description
        self._attr_unique_id = coordinator.generate_unique_id(description.key)
        self._proxy = coordinator.config_entry.runtime_data.api.slicer_proxy
        self._attr_image_last_updated = self._cover_time()

    def _current_cover(self) -> bytes | None:
        cover = self._proxy.cover if self._proxy else None
        data = self.coordinator.data
        filename = data.status.print_info.filename if data and data.status else None
        if cover is None or not filename or cover.filename != filename:
            return None
        return cover.png

    def _cover_time(self) -> datetime | None:
        cover = self._proxy.cover if self._proxy else None
        return cover.received if cover and self._current_cover() else None

    def _handle_coordinator_update(self) -> None:
        """Tell the frontend to reload when another file's preview applies."""
        self._attr_image_last_updated = self._cover_time()
        super()._handle_coordinator_update()

    async def async_image(self) -> bytes | None:
        """Return the PNG preview of the current file, if a slicer sent it."""
        return self._current_cover()

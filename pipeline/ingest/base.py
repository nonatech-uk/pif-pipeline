"""Base class for source watchers and shared envelope-building utilities."""

from __future__ import annotations

import hashlib
import io
import logging
import mimetypes
from collections.abc import AsyncGenerator
from datetime import datetime, UTC
from pathlib import Path

import magic
import piexif

from pipeline.models import Envelope, ExifData

log = logging.getLogger(__name__)

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except ImportError:
    pass


class SourceWatcher:
    """Abstract base for all source watchers.

    Subclasses implement watch() as an async generator that yields Envelopes.
    """

    source_type: str = "unknown"

    async def watch(self) -> AsyncGenerator[Envelope, None]:
        raise NotImplementedError
        yield  # make it a generator

    @staticmethod
    def _build_envelope(
        raw_bytes: bytes,
        *,
        source_type: str,
        source_path: str | None = None,
        file_name: str | None = None,
        source_email_from: str | None = None,
        source_email_subject: str | None = None,
    ) -> Envelope:
        """Build a normalised Envelope from raw file bytes."""
        sha256 = hashlib.sha256(raw_bytes).hexdigest()

        # MIME detection — python-magic on the bytes
        media_type = magic.from_buffer(raw_bytes, mime=True)

        # Convert HEIC/HEIF to JPEG so all classifiers can process it
        if media_type in ("image/heic", "image/heif"):
            try:
                from PIL import Image
                img = Image.open(io.BytesIO(raw_bytes))
                buf = io.BytesIO()
                img.convert("RGB").save(buf, format="JPEG", quality=90, exif=img.info.get("exif", b""))
                raw_bytes = buf.getvalue()
                media_type = "image/jpeg"
                log.info("Converted HEIC to JPEG (%d bytes)", len(raw_bytes))
            except Exception:
                log.exception("HEIC to JPEG conversion failed")

        # Fallback MIME from filename extension
        if media_type == "application/octet-stream" and file_name:
            guessed, _ = mimetypes.guess_type(file_name)
            if guessed:
                media_type = guessed

        # EXIF extraction for images
        exif = _extract_exif(raw_bytes, media_type)

        return Envelope(
            source_type=source_type,
            source_path=source_path,
            media_type=media_type,
            file_sha256=sha256,
            file_size=len(raw_bytes),
            file_name=file_name,
            raw_bytes=raw_bytes,
            exif=exif,
            source_email_from=source_email_from,
            source_email_subject=source_email_subject,
        )


def _extract_exif(raw_bytes: bytes, media_type: str | None) -> ExifData | None:
    """Extract EXIF data from image bytes if present."""
    if not media_type or not media_type.startswith("image/"):
        return None

    try:
        exif_dict = piexif.load(raw_bytes)
    except Exception:
        return None

    gps = exif_dict.get("GPS", {})
    exif_ifd = exif_dict.get("Exif", {})
    zeroth = exif_dict.get("0th", {})

    lat = _gps_to_decimal(gps.get(piexif.GPSIFD.GPSLatitude), gps.get(piexif.GPSIFD.GPSLatitudeRef))
    lng = _gps_to_decimal(gps.get(piexif.GPSIFD.GPSLongitude), gps.get(piexif.GPSIFD.GPSLongitudeRef))

    taken_at = _parse_exif_datetime(exif_ifd.get(piexif.ExifIFD.DateTimeOriginal))

    make = zeroth.get(piexif.ImageIFD.Make)
    model = zeroth.get(piexif.ImageIFD.Model)

    if lat is None and lng is None and taken_at is None and make is None:
        return None

    return ExifData(
        gps_lat=lat,
        gps_lng=lng,
        taken_at=taken_at,
        year=taken_at.year if taken_at else None,
        camera_make=make.decode(errors="replace").strip() if isinstance(make, bytes) else make,
        camera_model=model.decode(errors="replace").strip() if isinstance(model, bytes) else model,
    )


def _gps_to_decimal(
    dms: tuple | None, ref: bytes | None
) -> float | None:
    """Convert EXIF GPS DMS tuple to decimal degrees."""
    if not dms or not ref:
        return None
    try:
        degrees = dms[0][0] / dms[0][1]
        minutes = dms[1][0] / dms[1][1]
        seconds = dms[2][0] / dms[2][1]
        decimal = degrees + minutes / 60 + seconds / 3600
        if ref in (b"S", b"W"):
            decimal = -decimal
        return round(decimal, 6)
    except (ZeroDivisionError, TypeError, IndexError):
        return None


def _parse_exif_datetime(raw: bytes | str | None) -> datetime | None:
    """Parse EXIF datetime string like b'2024:01:15 14:30:00'."""
    if not raw:
        return None
    s = raw.decode(errors="replace") if isinstance(raw, bytes) else raw
    try:
        return datetime.strptime(s.strip(), "%Y:%m:%d %H:%M:%S").replace(tzinfo=UTC)
    except ValueError:
        return None

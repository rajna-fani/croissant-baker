"""Image file handler for datasets containing images."""

import logging
from pathlib import Path
from typing import Dict, List

import mlcroissant as mlc

from croissant_baker.handlers.base_handler import FileTypeHandler
from croissant_baker.handlers.utils import compute_file_hash

logger = logging.getLogger(__name__)

# Standard image extensions that Pillow handles natively.
_PILLOW_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".bmp",
    ".webp",
    ".ico",
}

# Multi-band / scientific TIFF extensions that may need tifffile.
_TIFF_EXTENSIONS = {".tiff", ".tif"}

# All supported image extensions.
SUPPORTED_EXTENSIONS = _PILLOW_EXTENSIONS | _TIFF_EXTENSIONS

# MIME types for common image formats.
_MIME_TYPES: Dict[str, str] = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
    ".webp": "image/webp",
    ".ico": "image/x-icon",
    ".tiff": "image/tiff",
    ".tif": "image/tiff",
}


def _read_with_pillow(file_path: Path) -> Dict:
    """Read image metadata using Pillow (standard RGB/grayscale images)."""
    from PIL import Image

    with Image.open(file_path) as img:
        width, height = img.size
        # mode → number of bands: L=1, LA=2, RGB=3, RGBA=4, CMYK=4, etc.
        num_bands = len(img.getbands())
        return {
            "width": width,
            "height": height,
            "num_bands": num_bands,
            "image_format": img.format or file_path.suffix.lstrip(".").upper(),
        }


def _read_with_tifffile(file_path: Path) -> Dict:
    """Read image metadata using tifffile (multi-band / scientific TIFFs)."""
    import tifffile

    with tifffile.TiffFile(str(file_path)) as tif:
        page = tif.pages[0]
        # Prefer the TIFF tags, which describe the logical image dimensions
        # directly and are not affected by planar storage order.
        width = getattr(page, "imagewidth", None)
        height = getattr(page, "imagelength", None)
        num_bands = getattr(page, "samplesperpixel", None)

        if width is None or height is None or num_bands is None:
            # Some TIFF variants expose dimensions more reliably through axes.
            shape = getattr(page, "shape", ())
            axes = getattr(page, "axes", "") or ""
            shape_map = dict(zip(axes, shape)) if axes else {}

            if width is None:
                width = shape_map.get("X")
            if height is None:
                height = shape_map.get("Y")
            if num_bands is None:
                num_bands = shape_map.get("S")

        if width is None or height is None:
            raise ValueError(f"Unable to determine TIFF dimensions for {file_path}")

        if num_bands is None:
            num_bands = 1

        return {
            "width": int(width),
            "height": int(height),
            "num_bands": int(num_bands),
            "image_format": "TIFF",
        }


def _read_image_metadata(file_path: Path) -> Dict:
    """
    Read image dimensions and band count, choosing the right backend.

    Tries Pillow first for standard formats. Falls back to tifffile for
    multi-band TIFFs that Pillow cannot decode (e.g., 12-band Sentinel-2).
    """
    suffix = file_path.suffix.lower()

    if suffix in _PILLOW_EXTENSIONS:
        return _read_with_pillow(file_path)

    # For TIFF files, try Pillow first (works for standard 1/3/4-band TIFFs),
    # then fall back to tifffile for multi-band scientific imagery.
    if suffix in _TIFF_EXTENSIONS:
        try:
            return _read_with_pillow(file_path)
        except Exception:
            return _read_with_tifffile(file_path)

    return _read_with_pillow(file_path)


class ImageHandler(FileTypeHandler):
    """
    Handler for image files (JPEG, PNG, TIFF, GIF, BMP, WebP).

    - Extracts dimensions (width, height), band count, and format
    - Uses Pillow for standard formats, tifffile for multi-band TIFFs
    - Computes SHA256 for reproducibility
    - Returns metadata with ``image_properties`` key for the builder
    """

    def can_handle(self, file_path: Path) -> bool:
        return file_path.suffix.lower() in SUPPORTED_EXTENSIONS

    def extract_metadata(self, file_path: Path, **kwargs) -> dict:
        if not file_path.exists():
            raise FileNotFoundError(f"Image file not found: {file_path}")

        try:
            img_meta = _read_image_metadata(file_path)
        except Exception as e:
            raise ValueError(f"Failed to read image {file_path}: {e}") from e

        suffix = file_path.suffix.lower()
        mime_type = _MIME_TYPES.get(suffix, "application/octet-stream")

        file_size = file_path.stat().st_size
        sha256 = compute_file_hash(file_path)

        return {
            "file_path": str(file_path),
            "file_name": file_path.name,
            "file_size": file_size,
            "sha256": sha256,
            "encoding_format": mime_type,
            "image_properties": {
                "width": img_meta["width"],
                "height": img_meta["height"],
                "num_bands": img_meta["num_bands"],
                "image_format": img_meta["image_format"],
            },
        }

    def build_croissant(self, file_metas: list, file_ids: list) -> tuple:
        summary = collect_image_summary(file_metas)
        w_lo, w_hi = summary["width_range"]
        h_lo, h_hi = summary["height_range"]
        b_lo, b_hi = summary["num_bands_range"]
        formats_str = ", ".join(
            f"{fmt} ({cnt})" for fmt, cnt in summary["format_counts"].items()
        )

        if w_lo == w_hi and h_lo == h_hi:
            dims = f"{w_lo}x{h_lo}"
        else:
            dims = f"{w_lo}-{w_hi}x{h_lo}-{h_hi}"

        bands_note = f", {b_lo}-{b_hi} bands" if b_hi > 4 else ""

        extensions = set()
        mime_types = set()
        for meta in file_metas:
            ext = Path(meta["file_name"]).suffix.lower()
            extensions.add(ext)
            mime_types.add(meta["encoding_format"])

        includes = [f"**/*{ext}" for ext in sorted(extensions)]

        fileset_id = "image-files"
        image_fileset = mlc.FileSet(
            id=fileset_id,
            name="Image files",
            description=f"{summary['num_images']} image files ({formats_str})",
            encoding_formats=sorted(mime_types),
            includes=includes,
        )

        image_fields = [
            mlc.Field(
                id="images/image_content",
                name="image",
                description=f"Image content ({summary['num_images']} files, {formats_str})",
                data_types=["sc:ImageObject"],
                source=mlc.Source(
                    file_set=fileset_id,
                    extract=mlc.Extract(file_property="content"),
                ),
            ),
        ]

        image_record_set = mlc.RecordSet(
            id="images",
            name="images",
            description=f"{summary['num_images']} images ({dims}{bands_note}): {formats_str}",
            fields=image_fields,
        )

        return [image_fileset], [image_record_set]


def collect_image_summary(image_metadata_list: List[Dict]) -> Dict:
    """
    Summarize a collection of image file metadata into aggregate stats.

    Used by the metadata generator to describe an image dataset at the
    RecordSet level (e.g., total images, dimension ranges, formats).

    Args:
        image_metadata_list: List of metadata dicts from ImageHandler.

    Returns:
        Summary dict with counts, dimension ranges, and format breakdown.
    """
    if not image_metadata_list:
        return {}

    widths = []
    heights = []
    bands = []
    formats: Dict[str, int] = {}

    processed_count = 0
    for i, meta in enumerate(image_metadata_list):
        props = meta.get("image_properties")
        if not props:
            logger.warning(
                "Skipping image entry %d: missing or incomplete image_properties", i
            )
            continue

        processed_count += 1
        width = props.get("width")
        height = props.get("height")
        num_bands = props.get("num_bands")
        fmt = props.get("image_format")

        if width is not None:
            widths.append(width)
        if height is not None:
            heights.append(height)
        if num_bands is not None:
            bands.append(num_bands)
        if fmt is not None:
            formats[fmt] = formats.get(fmt, 0) + 1

    return {
        "num_images": processed_count,
        "width_range": (min(widths), max(widths)) if widths else (0, 0),
        "height_range": (min(heights), max(heights)) if heights else (0, 0),
        "num_bands_range": (min(bands), max(bands)) if bands else (0, 0),
        "format_counts": formats,
    }

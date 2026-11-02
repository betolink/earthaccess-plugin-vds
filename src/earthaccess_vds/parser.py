"""Parser resolution and URL generation for VirtualiZarr-backed virtual datasets.

Ported from ``earthaccess.virtual._parser``. This module is intentionally free
of I/O side-effects: it resolves parser names/instances and maps granule data
links to the URLs the chosen parser needs.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    from collections.abc import Sequence

logger = logging.getLogger(__name__)

ParserType = (
    Literal[
        "DMRPPParser",
        "HDFParser",
        "NetCDF3Parser",
        "KerchunkJSONParser",
        "KerchunkParquetParser",
        "IcechunkParser",
    ]
    | Any
)
"""A VirtualiZarr parser: a canonical class-name string or a pre-built instance."""

SUPPORTED_PARSERS: frozenset[str] = frozenset(
    {
        "DMRPPParser",
        "HDFParser",
        "NetCDF3Parser",
        "KerchunkJSONParser",
        "KerchunkParquetParser",
        "IcechunkParser",
    }
)

# Maps user-facing lowercase aliases → canonical class name.
_ALIASES: dict[str, str] = {
    "dmrpp": "DMRPPParser",
    "hdf": "HDFParser",
    "hdf5": "HDFParser",
    "netcdf3": "NetCDF3Parser",
    "kerchunk": "KerchunkJSONParser",
    "kerchunk-parquet": "KerchunkParquetParser",
    "icechunk": "IcechunkParser",
}


def resolve_parser(
    parser: ParserType,
    group: str | None = None,
) -> Any:
    """Resolve a parser name or instance to a VirtualiZarr parser object.

    If ``parser`` is already an instance (i.e. not a ``str``) it is returned
    unchanged. String values are matched case-sensitively against
    ``SUPPORTED_PARSERS`` first, then against the lowercase aliases table.

    Raises:
        ValueError: If ``parser`` is a string not in ``SUPPORTED_PARSERS`` and
            not a recognised alias.
        ImportError: If ``earthaccess-plugin-vds``'s virtualizarr dependency is
            not installed.
    """
    if not isinstance(parser, str):
        return parser

    try:
        from virtualizarr.parsers import (  # noqa: PLC0415
            DMRPPParser,
            HDFParser,
            IcechunkParser,
            KerchunkJSONParser,
            KerchunkParquetParser,
            NetCDF3Parser,
        )
    except ImportError:
        msg = "earthaccess_vds requires `virtualizarr>=2.8` to be installed"
        raise ImportError(msg) from None

    canonical = _ALIASES.get(parser, parser)

    if canonical not in SUPPORTED_PARSERS:
        msg = (
            f"Unknown parser {parser!r}. "
            f"Supported parsers: {sorted(SUPPORTED_PARSERS)}. "
            "You can also pass a pre-built parser instance directly."
        )
        raise ValueError(msg)

    return {
        "DMRPPParser": lambda: DMRPPParser(group=group),
        "HDFParser": lambda: HDFParser(group=group),
        "NetCDF3Parser": lambda: NetCDF3Parser(),
        "KerchunkJSONParser": lambda: KerchunkJSONParser(group=group),
        "KerchunkParquetParser": lambda: KerchunkParquetParser(group=group),
        # keyword-only constructor: branch/tag/snapshot pinning stays available
        # through a pre-built instance.
        "IcechunkParser": lambda: IcechunkParser(group=group),
    }[canonical]()


def get_urls_for_parser(
    granules: Sequence[Any],
    parser: Any,
    access: str,
) -> list[str]:
    """Return one data URL per granule, formatted for the given parser.

    ``DMRPPParser`` expects the NASA DMR++ sidecar files, which live at the
    data URL with a ``.dmrpp`` suffix appended. All other parsers receive the
    raw data URLs as-is.
    """
    is_dmrpp = type(parser).__name__ == "DMRPPParser"

    urls: list[str] = []
    for granule in granules:
        url = granule.data_links(access=access)[0]
        if is_dmrpp:
            url = url + ".dmrpp"
        urls.append(url)
    return urls

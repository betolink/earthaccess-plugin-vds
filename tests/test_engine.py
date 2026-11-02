"""Unit tests for ``earthaccess_vds.engine`` and ``earthaccess_vds.parser``.

Ported from earthaccess's ``tests/unit/test_virtual.py`` (the core and _parser
sections). All external I/O is mocked so the suite runs without network access.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from earthaccess_vds.engine import virtualize
from earthaccess_vds.parser import (
    SUPPORTED_PARSERS,
    get_urls_for_parser,
    resolve_parser,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_granules(n: int = 1, base_url: str = "s3://bucket/file") -> list[Any]:
    granules: list[Any] = []
    for i in range(n):
        g = MagicMock()
        g.data_links.return_value = [f"{base_url}_{i}.nc"]
        g.__getitem__ = MagicMock(
            side_effect=lambda key, i=i: {
                "meta": {
                    "collection-concept-id": f"C{i}-PODAAC",
                    "provider-id": "PODAAC",
                },
            }[key],
        )
        granules.append(g)
    return cast("list[Any]", granules)


def _patch_internals(mock_vds: Any | None = None) -> tuple[Any, ...]:
    """Return patches for the registry and all virtual openers."""
    if mock_vds is None:
        mock_vds = MagicMock()
    return (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_dataset_single",
            return_value=mock_vds,
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_mfdataset",
            return_value=mock_vds,
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_datatree",
            return_value=mock_vds,
        ),
    )


# ---------------------------------------------------------------------------
# engine — input validation
# ---------------------------------------------------------------------------


def test_virtualize_empty_granules_raises() -> None:
    """virtualize() raises ValueError when granules list is empty."""
    with pytest.raises(ValueError, match=r"[Nn]o granules"):
        virtualize([])


def test_virtualize_multi_granule_no_concat_dim_raises() -> None:
    """virtualize() raises ValueError for >1 granule without concat_dim."""
    with pytest.raises(ValueError, match="concat_dim"):
        virtualize(_make_granules(2))


def test_virtualize_by_coords_with_concat_dim_raises() -> None:
    """combine='by_coords' rejects a concat_dim."""
    with pytest.raises(ValueError, match="concat_dim"):
        virtualize(_make_granules(2), combine="by_coords", concat_dim="time")


def test_virtualize_tree_multi_granule_raises() -> None:
    """tree=True requires exactly one granule."""
    with pytest.raises(ValueError, match="tree"):
        virtualize(_make_granules(2), tree=True)


def test_virtualize_tree_with_load_raises() -> None:
    """tree=True is incompatible with load=True."""
    with pytest.raises(ValueError, match="tree"):
        virtualize(_make_granules(1), tree=True, load=True)


def test_virtualize_invalid_parser_string_raises() -> None:
    """virtualize() raises ValueError for an unrecognised parser string."""
    with pytest.raises(ValueError, match="BadParser"):
        virtualize(_make_granules(1), parser="BadParser")


# ---------------------------------------------------------------------------
# engine — happy paths
# ---------------------------------------------------------------------------


def test_virtualize_load_false_returns_virtual_dataset() -> None:
    """virtualize(load=False) returns the raw virtual dataset without kerchunk."""
    mock_vds = MagicMock()
    reg_patch, single_patch, mf_patch, tree_patch = _patch_internals(mock_vds)
    with (
        reg_patch,
        single_patch,
        mf_patch,
        tree_patch,
        patch("earthaccess_vds.engine._load_via_kerchunk") as mock_load,
    ):
        result = virtualize(_make_granules(1), load=False)

    assert result is mock_vds
    mock_load.assert_not_called()


def test_virtualize_load_true_delegates_to_kerchunk(tmp_path: Any) -> None:
    """virtualize(load=True) calls _load_via_kerchunk and returns its result."""
    import xarray as xr

    expected_ds = MagicMock()
    reg_patch, single_patch, mf_patch, tree_patch = _patch_internals(xr.Dataset())
    with (
        reg_patch,
        single_patch,
        mf_patch,
        tree_patch,
        patch(
            "earthaccess_vds.engine._load_via_kerchunk",
            return_value=expected_ds,
        ) as mock_load,
    ):
        result = virtualize(
            _make_granules(1),
            load=True,
            reference_dir=str(tmp_path),
        )

    mock_load.assert_called_once()
    assert result is expected_ds


# ---------------------------------------------------------------------------
# engine — dispatch behaviour
# ---------------------------------------------------------------------------


def test_virtualize_single_granule_uses_open_virtual_dataset() -> None:
    """A single granule is opened directly, not through the mfdataset path."""
    mock_vds = MagicMock()
    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_dataset_single",
            return_value=mock_vds,
        ) as single,
        patch("earthaccess_vds.engine._open_virtual_mfdataset") as mf,
    ):
        result = virtualize(_make_granules(1))

    assert result is mock_vds
    single.assert_called_once()
    mf.assert_not_called()


def test_virtualize_single_granule_forwards_loadable_and_drop_variables() -> None:
    """loadable_variables / drop_variables are forwarded to the single-file opener."""
    mock_vds = MagicMock()
    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_dataset_single",
            return_value=mock_vds,
        ) as single,
    ):
        virtualize(
            _make_granules(1),
            loadable_variables=["time"],
            drop_variables=["qc"],
        )

    kwargs = single.call_args.kwargs
    assert kwargs["loadable_variables"] == ["time"]
    assert kwargs["drop_variables"] == ["qc"]


def test_virtualize_multi_granule_uses_open_virtual_mfdataset() -> None:
    """Multiple granules are combined through open_virtual_mfdataset."""
    mock_vds = MagicMock()
    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_mfdataset",
            return_value=mock_vds,
        ) as mf,
        patch("earthaccess_vds.engine._open_virtual_dataset_single") as single,
    ):
        result = virtualize(_make_granules(2), concat_dim="time")

    assert result is mock_vds
    mf.assert_called_once()
    single.assert_not_called()


def test_virtualize_by_coords_forwards_combine_and_join() -> None:
    """combine='by_coords' does not require concat_dim and forwards join."""
    mock_vds = MagicMock()
    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_mfdataset",
            return_value=mock_vds,
        ) as mf,
    ):
        result = virtualize(_make_granules(2), combine="by_coords", join="inner")

    assert result is mock_vds
    kwargs = mf.call_args.kwargs
    assert kwargs["combine"] == "by_coords"
    assert kwargs["join"] == "inner"


def test_virtualize_tree_returns_datatree() -> None:
    """tree=True dispatches to open_virtual_datatree."""
    mock_tree = MagicMock()
    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_datatree",
            return_value=mock_tree,
        ) as tree,
    ):
        result = virtualize(_make_granules(1), tree=True)

    assert result is mock_tree
    tree.assert_called_once()


# ---------------------------------------------------------------------------
# engine — DMR++ fallback behaviour
# ---------------------------------------------------------------------------


def test_virtualize_dmrpp_fallback_emits_user_warning() -> None:
    """When DMR++ sidecars are missing, virtualize() warns and retries with HDFParser."""
    mock_vds_hdf = MagicMock()
    call_count = {"n": 0}

    def side_effect(*args: Any, **kwargs: Any) -> Any:  # noqa: ARG001
        call_count["n"] += 1
        if call_count["n"] == 1:
            msg = "no .dmrpp sidecar"
            raise FileNotFoundError(msg)
        return mock_vds_hdf

    with (
        patch(
            "earthaccess_vds.engine.build_obstore_registry",
            return_value=MagicMock(),
        ),
        patch(
            "earthaccess_vds.engine._open_virtual_dataset_single",
            side_effect=side_effect,
        ),
        pytest.warns(UserWarning, match="HDFParser"),
    ):
        result = virtualize(_make_granules(1), parser="DMRPPParser")

    assert result is mock_vds_hdf
    assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# parser — SUPPORTED_PARSERS / resolve_parser / get_urls_for_parser
# ---------------------------------------------------------------------------


def test_supported_parsers_contains_canonical_names() -> None:
    """SUPPORTED_PARSERS includes the canonical parser names."""
    assert isinstance(SUPPORTED_PARSERS, frozenset)
    assert {
        "DMRPPParser",
        "HDFParser",
        "NetCDF3Parser",
        "KerchunkJSONParser",
        "KerchunkParquetParser",
    } <= SUPPORTED_PARSERS


def test_resolve_parser_dmrpp_returns_instance() -> None:
    """resolve_parser('DMRPPParser') returns a DMRPPParser instance."""
    assert type(resolve_parser("DMRPPParser")).__name__ == "DMRPPParser"


def test_resolve_parser_icechunk_returns_instance() -> None:
    """resolve_parser('IcechunkParser') opens icechunk stores (store-to-store merge)."""
    assert type(resolve_parser("IcechunkParser")).__name__ == "IcechunkParser"


def test_resolve_parser_invalid_string_raises() -> None:
    """resolve_parser raises ValueError and lists valid names in the message."""
    with pytest.raises(ValueError, match="DMRPPParser"):
        resolve_parser("UnknownParser")


def test_get_urls_dmrpp_appends_dmrpp_suffix() -> None:
    """get_urls_for_parser with DMRPPParser appends '.dmrpp' to each URL."""
    granule = cast("Any", MagicMock())
    granule.data_links.return_value = ["s3://bucket/file.nc"]  # type: ignore[attr-defined]
    urls = get_urls_for_parser(
        [granule],
        resolve_parser("DMRPPParser"),
        access="direct",
    )
    assert urls == ["s3://bucket/file.nc.dmrpp"]


def test_get_urls_passes_access_to_data_links() -> None:
    """get_urls_for_parser forwards the access argument to granule.data_links."""
    mock = MagicMock()
    mock.data_links.return_value = ["https://example.com/file.h5"]
    granule = cast("Any", mock)
    get_urls_for_parser([granule], resolve_parser("HDFParser"), access="indirect")
    mock.data_links.assert_called_once_with(access="indirect")

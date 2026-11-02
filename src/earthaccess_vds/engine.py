"""Core virtualization engine: ``virtualize()`` and its dispatch helpers.

Ported from ``earthaccess.virtual.core`` (the virtualize half). Differences
from the original:

- credentials come from an :class:`~earthaccess_plugin_spec.AuthHandle`
  (``auth=``) instead of reading ``earthaccess.__auth__`` directly;
- the ``load=True`` round-trip writes **parquet** references only — no JSON
  output anywhere (the refs "file" is a directory named ``*.parquet``).
"""

from __future__ import annotations

import tempfile
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, Any

from earthaccess_vds.credentials import build_obstore_registry
from earthaccess_vds.parser import get_urls_for_parser, resolve_parser
from earthaccess_vds.types import (
    AccessType,
    CombineAttrsType,
    CombineType,
    CompatType,
    DataVarsType,
    JoinType,
    ParallelType,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    import xarray as xr
    from earthaccess_plugin_spec import AuthHandle

    from earthaccess_vds.parser import ParserType

__all__ = ["virtualize"]


def virtualize(  # noqa: PLR0913, C901
    granules: Sequence[Any],
    *,
    access: AccessType = "direct",
    load: bool = False,
    group: str = "/",
    concat_dim: str | None = None,
    combine: CombineType = "nested",
    preprocess: Callable[[xr.Dataset], xr.Dataset] | None = None,
    data_vars: DataVarsType = "all",
    coords: str = "different",
    compat: CompatType = "no_conflicts",
    combine_attrs: CombineAttrsType = "drop_conflicts",
    join: JoinType = "outer",
    loadable_variables: list[str] | None = None,
    drop_variables: list[str] | None = None,
    parallel: ParallelType = "dask",
    parser: ParserType = "DMRPPParser",
    reference_dir: str | None = None,
    tree: bool = False,
    auth: AuthHandle | None = None,
    **xr_combine_kwargs: Any,
) -> xr.Dataset | xr.DataTree:
    """Create a virtual xarray Dataset from NASA Earthdata granules.

    Uses VirtualiZarr to open granules as virtual datasets backed by cloud
    object storage without downloading data. By default returns a virtual
    dataset (``load=False``); set ``load=True`` to return a concrete
    lazily-loaded xarray Dataset via a kerchunk parquet round-trip.

    The ``parser`` controls which VirtualiZarr backend reads the files. The
    default ``"DMRPPParser"`` uses NASA pre-computed DMR++ sidecar files; when
    those sidecars are absent the engine automatically falls back to
    ``"HDFParser"`` and emits a ``UserWarning``.
    """
    if len(granules) == 0:
        msg = "No granules provided. At least one granule is required."
        raise ValueError(msg)

    if tree:
        if len(granules) != 1:
            msg = (
                "tree=True requires exactly one granule. "
                "open_virtual_datatree only opens a single data source."
            )
            raise ValueError(msg)
        if load:
            msg = "tree=True is only supported with load=False."
            raise ValueError(msg)
    elif combine == "nested":
        if len(granules) > 1 and concat_dim is None:
            msg = (
                "concat_dim is required when virtualizing more than one granule "
                "with combine='nested'. Pass concat_dim='<dimension_name>' to "
                "specify how to concatenate, or use combine='by_coords'."
            )
            raise ValueError(msg)
    elif concat_dim is not None:
        msg = (
            "concat_dim is not valid with combine='by_coords'. Align granules "
            "on their coordinates instead, or pass combine='nested'."
        )
        raise ValueError(msg)

    # Validate / resolve parser early so callers get a clear error before any
    # network activity.
    resolved_parser = resolve_parser(parser, group=group if group != "/" else None)

    registry = build_obstore_registry(granules, access=access, auth=auth)

    def _open_once(parser_obj: Any, obj_registry: Any) -> xr.Dataset | xr.DataTree:
        return _open_virtual_dispatch(
            granules=granules,
            parser=parser_obj,
            registry=obj_registry,
            access=access,
            concat_dim=concat_dim,
            combine=combine,
            join=join,
            preprocess=preprocess,
            loadable_variables=loadable_variables,
            drop_variables=drop_variables,
            parallel=parallel,
            data_vars=data_vars,
            coords=coords,
            compat=compat,
            combine_attrs=combine_attrs,
            tree=tree,
            **xr_combine_kwargs,
        )

    # Attempt to open with the requested parser; fall back to HDFParser if
    # DMR++ sidecars are not present.
    try:
        vds = _open_once(resolved_parser, registry)
    except FileNotFoundError:
        if type(resolved_parser).__name__ != "DMRPPParser":
            raise
        warnings.warn(
            "DMR++ sidecar files were not found for one or more granules. "
            "Falling back to HDFParser. "
            "Set parser='HDFParser' to silence this warning.",
            UserWarning,
            stacklevel=2,
        )
        resolved_parser = resolve_parser(
            "HDFParser",
            group=group if group != "/" else None,
        )
        registry = build_obstore_registry(granules, access=access, auth=auth)
        vds = _open_once(resolved_parser, registry)

    if not load:
        return vds

    import xarray as xr  # noqa: PLC0415

    if not isinstance(vds, xr.Dataset):
        msg = "load=True requires a Dataset; open with tree=False."
        raise TypeError(msg)

    return _load_via_kerchunk(
        vds=vds,
        granules=granules,
        group=group,
        access=access,
        reference_dir=reference_dir,
    )


# ---------------------------------------------------------------------------
# Internal helpers — separated to make mocking clean in tests
# ---------------------------------------------------------------------------


def _open_virtual_dispatch(  # noqa: PLR0913
    granules: Sequence[Any],
    parser: Any,
    registry: Any,
    access: AccessType,
    concat_dim: str | None,
    combine: CombineType,
    join: JoinType,
    preprocess: Callable | None,
    loadable_variables: list[str] | None,
    drop_variables: list[str] | None,
    parallel: ParallelType,
    data_vars: DataVarsType,
    coords: str,
    compat: CompatType,
    combine_attrs: CombineAttrsType,
    *,
    tree: bool,
    **xr_combine_kwargs: Any,
) -> xr.Dataset | xr.DataTree:
    """Route granules to the correct VirtualiZarr opener.

    ``tree=True`` uses ``open_virtual_datatree`` (single granule only), a
    single granule uses ``open_virtual_dataset`` directly, and everything else
    goes through ``open_virtual_mfdataset``.
    """
    if tree:
        url = get_urls_for_parser(granules, parser, access=access)[0]
        return _open_virtual_datatree(
            url,
            parser=parser,
            registry=registry,
            loadable_variables=loadable_variables,
            **xr_combine_kwargs,
        )

    if len(granules) == 1:
        url = get_urls_for_parser(granules, parser, access=access)[0]
        return _open_virtual_dataset_single(
            url,
            parser=parser,
            registry=registry,
            preprocess=preprocess,
            drop_variables=drop_variables,
            loadable_variables=loadable_variables,
            **xr_combine_kwargs,
        )

    return _open_virtual_mfdataset(
        granules=granules,
        parser=parser,
        registry=registry,
        access=access,
        concat_dim=concat_dim,
        combine=combine,
        join=join,
        preprocess=preprocess,
        loadable_variables=loadable_variables,
        drop_variables=drop_variables,
        parallel=parallel,
        data_vars=data_vars,
        coords=coords,
        compat=compat,
        combine_attrs=combine_attrs,
        **xr_combine_kwargs,
    )


def _open_virtual_dataset_single(  # noqa: PLR0913
    url: str,
    parser: Any,
    registry: Any,
    *,
    preprocess: Callable | None,
    drop_variables: list[str] | None,
    loadable_variables: list[str] | None,
    **kwargs: Any,
) -> xr.Dataset:
    """Open a single URL with ``vz.open_virtual_dataset``, then apply preprocess."""
    import virtualizarr as vz  # noqa: PLC0415

    vds = vz.open_virtual_dataset(
        url,
        registry=registry,
        parser=parser,
        drop_variables=drop_variables,
        loadable_variables=loadable_variables,
        **kwargs,
    )
    return preprocess(vds) if preprocess else vds


def _open_virtual_datatree(
    url: str,
    parser: Any,
    registry: Any,
    *,
    loadable_variables: list[str] | None,
    **kwargs: Any,
) -> xr.DataTree:
    """Open a single URL with ``vz.open_virtual_datatree`` (returns a DataTree)."""
    import virtualizarr as vz  # noqa: PLC0415

    return vz.open_virtual_datatree(
        url,
        registry=registry,
        parser=parser,
        loadable_variables=loadable_variables,
        **kwargs,
    )


def _open_virtual_mfdataset(  # noqa: PLR0913
    granules: Sequence[Any],
    parser: Any,
    registry: Any,
    access: AccessType,
    concat_dim: str | None,
    combine: CombineType,
    join: JoinType,
    preprocess: Callable | None,
    loadable_variables: list[str] | None,
    drop_variables: list[str] | None,
    parallel: ParallelType,
    data_vars: DataVarsType,
    coords: str,
    compat: CompatType,
    combine_attrs: CombineAttrsType,
    **xr_combine_kwargs: Any,
) -> xr.Dataset:
    """Thin wrapper around ``vz.open_virtual_mfdataset`` for testability."""
    import virtualizarr as vz  # noqa: PLC0415

    urls = get_urls_for_parser(granules, parser, access=access)

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="Numcodecs codecs*",
            category=UserWarning,
        )
        return vz.open_virtual_mfdataset(
            urls=urls,
            registry=registry,
            parser=parser,
            preprocess=preprocess,
            parallel=parallel,
            combine=combine,
            concat_dim=concat_dim,
            join=join,
            data_vars=data_vars,
            coords=coords,
            compat=compat,
            combine_attrs=combine_attrs,
            loadable_variables=loadable_variables,
            drop_variables=drop_variables,
            **xr_combine_kwargs,
        )


def _load_via_kerchunk(  # noqa: PLR0913
    vds: xr.Dataset,
    granules: Sequence[Any],
    group: str,
    access: AccessType,
    reference_dir: str | None,
) -> xr.Dataset:
    """Materialize a virtual dataset's coordinates via a kerchunk round-trip.

    Writes **parquet** references (a directory named ``*.parquet``) and reopens
    them with the kerchunk engine. Needed until
    https://github.com/zarr-developers/VirtualiZarr/issues/360 is resolved.
    """
    import earthaccess  # noqa: PLC0415
    import xarray as xr  # noqa: PLC0415

    fs = earthaccess.get_fsspec_https_session()

    if reference_dir is None:
        ref_dir = Path(tempfile.gettempdir())
    else:
        ref_dir = Path(reference_dir)
        ref_dir.mkdir(exist_ok=True, parents=True)

    collection_id = granules[0]["meta"]["collection-concept-id"]

    if group in (None, "/"):
        group_name = "root"
    else:
        group_name = group.replace("/", "_").replace(" ", "_").lstrip("_")

    # refs_to_dataframe writes a directory of parquet files; the .parquet
    # suffix on the name is the convention open_virtual/ea.open sniff on.
    ref_path = ref_dir / f"{collection_id}-{group_name}.parquet"

    vds.virtualize.to_kerchunk(str(ref_path), format="parquet")

    storage_options = {
        "remote_protocol": "s3" if access == "direct" else "https",
        "remote_options": fs.storage_options,
    }
    return xr.open_dataset(
        str(ref_path),
        engine="kerchunk",
        storage_options=storage_options,
    )

"""VDSPlugin: virtualize NASA granules into icechunk / kerchunk-parquet stores."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from earthaccess_plugin_spec import Asset, Capabilities, Capability, PluginBase
from earthaccess_plugin_spec.granules import peek

__all__ = ["VDSPlugin"]

_OPERATIONS = frozenset({Capability.VIRTUALIZE, Capability.APPEND})
_FORMATS = frozenset({"icechunk", "kerchunk"})

_EXAMPLES = {
    Capability.VIRTUALIZE: (
        'ea.plugins.vds.request(results, concat_dim="time").results() '
        "# manifest-form Dataset"
    ),
    Capability.APPEND: (
        'ea.plugins.vds.request(new_results, store="mur_sst", '
        'concat_dim="time").results()  # store exists -> append'
    ),
}


class VDSPlugin(PluginBase):
    name = "vds"
    needs = frozenset({"umm"})
    auth = "edl"
    output = "assets"
    requires_results = False
    """``source=`` can build a store without new search results."""

    @dataclass
    class Options:
        # --- persistence (store= triggers write/append) ---
        store: Any = None
        """Target store: local path / ``s3://`` URI / obstore store / Storage."""

        prefix: str | None = None
        """Repository prefix when ``store`` is an obstore ``S3Store``."""

        format: Literal["icechunk", "kerchunk"] = "icechunk"
        source: Any = None
        """An existing store (href/Asset) to merge into ``store``."""

        assets: list[Any] | None = None
        """File assets (Asset or URL) to virtualize instead of search results.

        Use this to virtualize the output of another plugin (for example a
        Harmony subset). OPeNDAP assets are rejected: they are responses, not
        byte-range-readable files.
        """

        append_dim: str | None = None
        """Defaults to ``concat_dim`` when the target store already exists."""

        replace: bool = False
        """Kerchunk only: rewrite the existing references in place."""

        commit: bool = True
        """Icechunk only: commit the write to ``main``."""

        # --- virtualize ---
        access: Literal["direct", "indirect"] = "indirect"
        parser: Any = "DMRPPParser"
        group: str = "/"
        concat_dim: str | None = None
        combine: Literal["nested", "by_coords"] = "nested"
        preprocess: Any = None
        loadable_variables: list[str] | None = None
        drop_variables: list[str] | None = None
        load: bool = False
        tree: bool = False
        reference_dir: str | None = None
        combine_kwargs: dict[str, Any] = field(default_factory=dict)

        # --- execution ---
        client: Any = None
        """An existing ``distributed.Client``; per-granule opens run there."""

        show_progress: bool | None = None

    def _supports(self, results: Any) -> Capabilities:
        if peek(results) is None:
            return Capabilities(
                None, reason="supply search results to virtualize these granules"
            )
        seen = {_has_data_links(granule) for granule in results}
        if True in seen:
            return Capabilities(
                True,
                operations=_OPERATIONS,
                formats=_FORMATS,
                examples=_EXAMPLES,
            )
        if seen == {False}:
            return Capabilities(False, reason="none of these granules has a data link")
        return Capabilities(None, reason="could not confirm the granules' data links")

    def _request(self, results: Any, options: VDSPlugin.Options) -> Any:
        granules = list(results) if results is not None else []
        output = _validate(granules, options)

        from .progress import ProgressExecutor, ProgressState, VirtualJob

        state = ProgressState()
        count = len(granules) if granules else len(options.assets or [])
        if count:
            state.start("virtualize", total=count)
        if options.client is not None:
            state.dashboard = getattr(options.client, "dashboard_link", None)
        executor = ProgressExecutor(
            state,
            show_progress=options.show_progress,
            client=options.client,
        )

        def work() -> Any:
            try:
                return _run(self._ctx.auth, granules, options, state, executor)
            finally:
                executor.shutdown(wait=False)

        return VirtualJob(
            work,
            plugin=self.name,
            output=output,
            state=state,
            executor=executor,
        )


_NON_VIRTUAL_FORMATS = frozenset({"zarr", "geotiff", "geoparquet", "csv"})


@dataclass(frozen=True)
class _AssetGranule:
    """A granule-shaped view over a plain file URL."""

    href: str

    def data_links(self, access: str = "indirect") -> list[str]:
        return [self.href]

    def __getitem__(self, key: str) -> Any:
        if key == "meta":
            return {"concept-id": self.href, "collection-concept-id": "assets"}
        raise KeyError(key)


def _asset_granules(assets: list[Any]) -> list[_AssetGranule]:
    return [_AssetGranule(str(getattr(asset, "href", asset))) for asset in assets]


def _validate_assets(assets: list[Any]) -> None:
    if not assets:
        msg = "assets= was given but is empty"
        raise ValueError(msg)
    for asset in assets:
        href = str(getattr(asset, "href", asset))
        if not href:
            msg = "every asset needs a non-empty href"
            raise ValueError(msg)
        if isinstance(asset, str):
            continue
        if getattr(asset, "access", None) == "opendap":
            msg = (
                "OPeNDAP assets are responses, not byte-range-readable files; "
                "virtualize the original search results instead of the "
                "constraint-expressed OPeNDAP URLs"
            )
            raise ValueError(msg)
        fmt = getattr(asset, "format", None)
        if fmt in _NON_VIRTUAL_FORMATS:
            msg = (
                f"{fmt!r} assets cannot be virtualized: the parsers read "
                "netCDF/HDF5 files (Harmony netCDF output works; Zarr/GeoTIFF "
                "do not)"
            )
            raise ValueError(msg)


def _has_data_links(granule: Any) -> bool | None:
    """Whether a granule exposes data links (``None`` when it cannot tell)."""

    links = getattr(granule, "data_links", None)
    if links is None:
        return None
    try:
        return bool(links())
    except Exception:  # noqa: BLE001 - a capability check must never crash
        return None


# ---------------------------------------------------------------------------
# request orchestration
# ---------------------------------------------------------------------------


def _validate(granules: list[Any], options: VDSPlugin.Options) -> str:
    """Validate the request eagerly; return the job's output kind."""

    if options.client is not None and not hasattr(options.client, "submit"):
        msg = (
            "client= must be a distributed.Client (or any object with submit()); "
            f"got {type(options.client).__name__}"
        )
        raise TypeError(msg)
    if options.format not in _FORMATS:
        msg = f"Unsupported format {options.format!r}: use 'icechunk' or 'kerchunk'"
        raise ValueError(msg)
    if options.load and options.store is not None:
        msg = (
            "load=True is only valid without store=; the job returns the Asset, "
            "and materializing is earthaccess.open's job: "
            "ds = ea.open(asset.href, load=True)"
        )
        raise ValueError(msg)
    if options.replace and options.format != "kerchunk":
        msg = "replace=True only applies to format='kerchunk'"
        raise ValueError(msg)
    if options.assets is not None and granules:
        msg = "pass search results or assets=, not both"
        raise ValueError(msg)
    if options.assets is not None and options.source is not None:
        msg = "assets= (raw files) and source= (virtual store) cannot be combined"
        raise ValueError(msg)
    if options.assets is not None:
        _validate_assets(options.assets)
    if options.source is not None and options.store is None:
        msg = "source= merges into a target store; pass store= as well"
        raise ValueError(msg)
    if options.tree and options.store is not None:
        msg = "tree=True cannot be persisted; open a single granule with tree=True only"
        raise ValueError(msg)
    if not granules and not options.assets and options.source is None:
        msg = (
            "no input provided: pass search results, assets= (file URLs), or "
            "source= (a virtual store to merge)"
        )
        raise ValueError(msg)
    if options.store is not None:
        return "assets"
    return "xarray" if options.load else "virtual"


def _run(
    auth: Any,
    granules: list[Any],
    options: VDSPlugin.Options,
    state: Any,
    executor: Any,
) -> Any:
    """Virtualize, optionally merge, and either return a dataset or write a store."""

    from .engine import (
        combine_virtual,
        materialize_coords,
        open_refs_store,
        virtualize,
    )
    from .writer import store_exists, write_kerchunk

    registry = None
    parser = options.parser
    if options.assets is not None:
        source_granules = _asset_granules(options.assets)
        from .credentials import build_registry_for_urls

        registry = build_registry_for_urls([g.href for g in source_granules], auth=auth)
        # Derived files have no DMR++ sidecars; default to the HDF reader.
        if parser == "DMRPPParser":
            parser = "HDFParser"
    else:
        source_granules = None

    delta = None
    if granules or source_granules:
        delta = virtualize(
            list(granules) + (source_granules or []),
            access=options.access,
            load=options.load,
            group=options.group,
            concat_dim=options.concat_dim,
            combine=options.combine,
            preprocess=options.preprocess,
            loadable_variables=options.loadable_variables,
            drop_variables=options.drop_variables,
            parser=parser,
            registry=registry,
            reference_dir=options.reference_dir,
            tree=options.tree,
            auth=auth,
            parallel=executor,
            progress=state,
            **options.combine_kwargs,
        )

    if options.store is None:
        return delta

    import xarray as xr

    if not isinstance(delta, xr.Dataset):
        msg = "tree=True cannot be persisted; store= needs a single Dataset"
        raise ValueError(msg)

    state.set_phase("combine")

    if options.format == "icechunk":
        state.set_phase("write")
        return [
            _write_icechunk_sequence(delta=delta, options=options, state=state)
        ]

    target_exists = store_exists(options.store, format="kerchunk")
    dim = options.append_dim or options.concat_dim
    if target_exists and dim is None:
        msg = (
            f"the kerchunk store at {options.store!r} already exists; pass "
            "append_dim=... (or concat_dim=...) to append along a dimension"
        )
        raise ValueError(msg)

    pieces: list[Any] = []
    if options.source is not None:
        pieces.append(open_refs_store(options.source, access=options.access))
    if target_exists:
        pieces.append(open_refs_store(options.store, access=options.access))
    if delta is not None:
        pieces.append(materialize_coords(delta) if pieces else delta)

    vds = combine_virtual(pieces, concat_dim=dim)
    state.set_phase("write")
    return [write_kerchunk(vds, options.store, replace=options.replace)]


def _write_icechunk_sequence(
    *, delta: Any, options: VDSPlugin.Options, state: Any
) -> Asset:
    """Write source (if any) then the delta; icechunk appends natively."""

    from .engine import open_refs_store
    from .writer import write_icechunk

    asset: Asset | None = None
    if options.source is not None:
        source_vds = open_refs_store(options.source, access=options.access)
        asset = write_icechunk(
            source_vds,
            options.store,
            append_dim=options.append_dim,
            concat_dim=options.concat_dim,
            commit=options.commit,
        )
    if delta is not None:
        asset = write_icechunk(
            delta,
            options.store,
            append_dim=options.append_dim,
            concat_dim=options.concat_dim,
            commit=options.commit,
        )
    if asset is None:  # pragma: no cover - validation requires source or granules
        msg = "nothing to write"
        raise ValueError(msg)
    return asset

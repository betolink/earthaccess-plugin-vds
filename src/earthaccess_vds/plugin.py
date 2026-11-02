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

        # --- execution (progress lands with the distributed work) ---
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
        return self.job(
            lambda: _run(self._ctx.auth, granules, options),
            output=output,
        )


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
    if options.source is not None and options.store is None:
        msg = "source= merges into a target store; pass store= as well"
        raise ValueError(msg)
    if options.tree and options.store is not None:
        msg = "tree=True cannot be persisted; open a single granule with tree=True only"
        raise ValueError(msg)
    if not granules and options.source is None:
        msg = "no granules provided: pass search results or source= to merge a store"
        raise ValueError(msg)
    if options.store is not None:
        return "assets"
    return "xarray" if options.load else "virtual"


def _run(auth: Any, granules: list[Any], options: VDSPlugin.Options) -> Any:
    """Virtualize, optionally merge, and either return a dataset or write a store."""

    from .engine import (
        combine_virtual,
        materialize_coords,
        open_refs_store,
        virtualize,
    )
    from .writer import store_exists, write_kerchunk

    delta = None
    if granules:
        delta = virtualize(
            granules,
            access=options.access,
            load=options.load,
            group=options.group,
            concat_dim=options.concat_dim,
            combine=options.combine,
            preprocess=options.preprocess,
            loadable_variables=options.loadable_variables,
            drop_variables=options.drop_variables,
            parser=options.parser,
            reference_dir=options.reference_dir,
            tree=options.tree,
            auth=auth,
            **options.combine_kwargs,
        )

    if options.store is None:
        return delta

    import xarray as xr

    if not isinstance(delta, xr.Dataset):
        msg = "tree=True cannot be persisted; store= needs a single Dataset"
        raise ValueError(msg)

    if options.format == "icechunk":
        return [_write_icechunk_sequence(delta=delta, options=options)]

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
    return [write_kerchunk(vds, options.store, replace=options.replace)]


def _write_icechunk_sequence(*, delta: Any, options: VDSPlugin.Options) -> Asset:
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

"""Writers for virtual stores: icechunk (default) and kerchunk-parquet.

Both writers take a manifest-form dataset (``load=False``) and return the
:class:`~earthaccess_plugin_spec.Asset` that identifies the written store, so
``earthaccess.open(asset.href)`` can route it. No credentials are serialized:
icechunk resolves them from the environment at the write site, and kerchunk
writes through fsspec's own env-based credentials.

Icechunk appends are O(delta): the delta is written straight into the existing
repository. Kerchunk has no append operation, so its append is a
read-combine-rewrite handled by the caller (see ``engine.open_refs_store``);
these writers only ever write one complete reference set.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Literal

from earthaccess_plugin_spec import Asset

from .storage import (
    resolve_icechunk_target,
    resolve_kerchunk_target,
)

__all__ = [
    "derive_vccs_from_vds",
    "store_exists",
    "write_icechunk",
    "write_kerchunk",
    "write_virtual_store",
]

StoreFormat = Literal["icechunk", "kerchunk"]


def derive_vccs_from_vds(vds: Any) -> dict[str, Any]:
    """Derive the virtual chunk containers needed to write *vds*.

    Walks every ``ManifestArray`` in the dataset and collects one container per
    unique ``scheme://host/`` prefix in its chunk references. ``s3://``
    containers use the default NASA cloud region; ``http(s)://`` containers
    need no credentials at write time.
    """

    import icechunk

    try:
        from virtualizarr.manifests import ManifestArray
    except ImportError as exc:  # pragma: no cover - hard dependency
        msg = "earthaccess_vds requires `virtualizarr>=2.8`"
        raise ImportError(msg) from exc

    prefixes: dict[str, str] = {}
    for variable in vds.variables.values():
        data = variable.data
        if not isinstance(data, ManifestArray):
            continue
        for path in data.manifest.iter_nonempty_paths():
            from urllib.parse import urlparse

            parsed = urlparse(path)
            if parsed.scheme in ("s3", "https", "http"):
                prefixes[f"{parsed.scheme}://{parsed.netloc}/"] = parsed.scheme

    if not prefixes:
        msg = (
            "writing a virtual store requires a virtual dataset (load=False) "
            "backed by ManifestArray variables; no virtual chunk references "
            "were found"
        )
        raise ValueError(msg)

    vccs: dict[str, Any] = {}
    for prefix, scheme in prefixes.items():
        if scheme == "s3":
            vccs[prefix] = icechunk.s3_store(region="us-west-2")
        else:
            vccs[prefix] = icechunk.http_store()
    return vccs


def store_exists(target: Any, *, format: StoreFormat = "icechunk") -> bool:
    """Return whether a store already exists at *target*."""

    if format == "icechunk":
        import icechunk

        resolved = resolve_icechunk_target(target)
        return bool(icechunk.Repository.exists(resolved.storage))
    if format == "kerchunk":
        resolved = resolve_kerchunk_target(target)
        fs, _ = _fsspec_for(resolved.href)
        return bool(fs.exists(resolved.href))
    msg = f"Unsupported format {format!r}: expected 'icechunk' or 'kerchunk'"
    raise ValueError(msg)


def write_virtual_store(
    vds: Any,
    target: Any,
    *,
    format: StoreFormat = "icechunk",
    append_dim: str | None = None,
    concat_dim: str | None = None,
    commit: bool = True,
    replace: bool = False,
) -> Asset:
    """Write *vds* to *target* and return the result Asset."""

    if format == "icechunk":
        return write_icechunk(
            vds,
            target,
            append_dim=append_dim,
            concat_dim=concat_dim,
            commit=commit,
        )
    if format == "kerchunk":
        return write_kerchunk(vds, target, replace=replace)
    msg = f"Unsupported format {format!r}: expected 'icechunk' or 'kerchunk'"
    raise ValueError(msg)


def write_icechunk(
    vds: Any,
    target: Any,
    *,
    append_dim: str | None = None,
    concat_dim: str | None = None,
    commit: bool = True,
) -> Asset:
    """Create or append to an icechunk repository at *target*.

    Create-vs-append is decided by the repository's existence. On append the
    dataset is written along ``append_dim`` (defaulting to ``concat_dim``), and
    any virtual chunk container missing from the stored configuration is merged
    in explicitly — a config passed to ``open_or_create`` is ignored for an
    existing repository.
    """

    import icechunk

    resolved = resolve_icechunk_target(target)
    vccs = derive_vccs_from_vds(vds)

    existed = icechunk.Repository.exists(resolved.storage)
    if existed:
        append_dim = append_dim or concat_dim
        if append_dim is None:
            msg = (
                f"the icechunk store at {resolved.href!r} already exists; pass "
                "append_dim=... (or concat_dim=... to append along the "
                "combine dimension)"
            )
            raise ValueError(msg)
        config = icechunk.Repository.fetch_config(resolved.storage)
        if config is None:  # pragma: no cover - exists() said otherwise
            config = icechunk.RepositoryConfig.default()
        added = _merge_vccs(config, vccs)
    else:
        config = icechunk.RepositoryConfig.default()
        for prefix, store_config in vccs.items():
            config.set_virtual_chunk_container(
                icechunk.VirtualChunkContainer(prefix, store_config),
            )
        added = False

    repo = icechunk.Repository.open_or_create(resolved.storage, config=config)
    if existed and added:
        # The stored config wins on open; persist the merged containers so
        # future readers can authorize every host this store references.
        repo.save_config()

    session = repo.writable_session("main")
    vds.virtualize.to_icechunk(session.store, append_dim=append_dim)
    if commit:
        session.commit("vds append" if append_dim else "vds create")

    return Asset(
        href=resolved.href,
        access=resolved.access,
        auth="none",
        format="icechunk",
    )


def write_kerchunk(
    vds: Any,
    target: Any,
    *,
    replace: bool = False,
    storage_options: dict[str, Any] | None = None,
) -> Asset:
    """Write *vds* as a kerchunk-parquet references directory.

    The target must be named ``*.parquet`` — the references are a *directory*
    whose name carries the suffix, which is what URI sniffing matches. Writing
    over an existing store creates a versioned sibling (``refs.parquet`` →
    ``refs_v2.parquet``) unless ``replace=True``, in which case the existing
    references are removed and rewritten in place (stable href).
    """

    from kerchunk.df import refs_to_dataframe

    resolved = resolve_kerchunk_target(target)
    if not resolved.href.endswith(".parquet"):
        msg = (
            f"a kerchunk-parquet store target must end in '.parquet' "
            f"(got {resolved.href!r}); the references are written as a "
            "directory named *.parquet"
        )
        raise ValueError(msg)

    fs, _ = _fsspec_for(resolved.href, storage_options=storage_options)
    href = resolved.href
    exists = fs.exists(href)
    if exists and replace:
        fs.rm(href, recursive=True)
    elif exists:
        href = _next_version(href)

    refs = vds.virtualize.to_kerchunk(format="dict")
    refs_to_dataframe(refs, href, storage_options=storage_options)

    return Asset(href=href, access=resolved.access, auth="none", format="kerchunk")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _merge_vccs(config: Any, vccs: dict[str, Any]) -> bool:
    """Add missing containers to *config*; return whether anything changed."""

    import icechunk

    existing = config.virtual_chunk_containers or {}
    missing = {prefix: store for prefix, store in vccs.items() if prefix not in existing}
    for prefix, store_config in missing.items():
        config.set_virtual_chunk_container(
            icechunk.VirtualChunkContainer(prefix, store_config),
        )
    return bool(missing)


def _fsspec_for(
    href: str,
    *,
    storage_options: dict[str, Any] | None = None,
) -> tuple[Any, str]:
    import fsspec

    fs, _, paths = fsspec.get_fs_token_paths(href, **(storage_options or {}))
    return fs, paths[0] if paths else href


_VERSIONED = re.compile(r"^(?P<base>.*?)(?:_v(?P<version>\d+))?(?P<suffix>\.parquet)$")


def _next_version(href: str) -> str:
    """``refs.parquet`` → ``refs_v2.parquet``; ``refs_v2.parquet`` → ``refs_v3.parquet``."""

    match = _VERSIONED.match(href)
    if match is None:  # pragma: no cover - validated by the caller
        return href
    version = int(match.group("version") or 1) + 1
    return f"{match.group('base')}_v{version}{match.group('suffix')}"


def _as_href(target: Any) -> str:
    if isinstance(target, Path):
        return str(target)
    return str(target)

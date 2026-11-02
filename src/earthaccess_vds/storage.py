"""Resolve user-supplied store targets to icechunk ``Storage`` / Asset hrefs.

The plugin accepts four flavours of target:

- a local path or ``file://`` URI,
- an ``s3://bucket/prefix`` URI,
- an obstore ``S3Store`` / ``LocalStore`` (a preconfigured session — the
  client-side escape hatch; never serialized),
- an already-built :class:`icechunk.Storage` (used with an explicit ``href=``).

Only the *location* and *credentials source* are derived here. Credentials are
always resolved from the environment where the code runs — nothing in this
module is pickled with secrets attached.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

__all__ = [
    "StoreTarget",
    "is_icechunk_uri",
    "resolve_icechunk_target",
    "resolve_kerchunk_target",
]

StoreAccess = Literal["file", "s3"]
"""``Asset.access`` for a store target (``https`` is not used for writes)."""


@dataclass(frozen=True)
class StoreTarget:
    """A resolved store location: what to write to, and how to report it."""

    href: str
    """Canonical location (local path or ``s3://`` URI) for the result Asset."""

    access: StoreAccess
    """``"file"`` for local paths, ``"s3"`` for S3 targets."""

    storage: Any = None
    """An :class:`icechunk.Storage` (only for icechunk targets)."""


def is_icechunk_uri(uri: str) -> bool:
    """Return ``True`` when *uri* looks like an icechunk repository.

    Recognizes ``icechunk://`` URIs, paths ending in ``.icechunk``, and local
    directories carrying icechunk's first-snapshot marker.
    """

    if uri.startswith("icechunk://") or uri.rstrip("/").endswith(".icechunk"):
        return True
    parsed = urlparse(uri)
    if parsed.scheme not in ("", "file"):
        return False
    path = Path(parsed.path) if parsed.scheme == "file" else Path(uri)
    return (path / "snapshots" / "1CECHNKREP0F1RSTCMT0").is_file()


def resolve_icechunk_target(
    target: Any,
    *,
    href: str | None = None,
    prefix: str | None = None,
) -> StoreTarget:
    """Resolve *target* to an icechunk :class:`StoreTarget`.

    ``href`` overrides the reported location (required for a bare
    :class:`icechunk.Storage`, which carries no readable path). ``prefix``
    overrides an ``S3Store``'s prefix, so one preconfigured store can back
    several repositories.
    """

    import icechunk
    from obstore.store import LocalStore, S3Store

    if isinstance(target, icechunk.Storage):
        if href is None:
            msg = (
                "an icechunk.Storage target carries no readable location; "
                "pass href=... so the returned Asset can be reopened"
            )
            raise ValueError(msg)
        return StoreTarget(href=href, access=_access_for(href), storage=target)

    if isinstance(target, Path):
        return _local_target(str(target), href=href)

    if isinstance(target, str):
        parsed = urlparse(target)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            prefix = parsed.path.lstrip("/")
            storage = icechunk.Storage.new_s3_object_store(
                icechunk.S3Options(),
                bucket,
                prefix,
                credentials=icechunk.S3Credentials.FromEnv(),
            )
            return StoreTarget(
                href=href or _s3_href(bucket, prefix),
                access="s3",
                storage=storage,
            )
        if parsed.scheme in ("", "file"):
            path = parsed.path if parsed.scheme == "file" else target
            return _local_target(path, href=href)
        msg = (
            f"Unsupported store target {target!r}: use a local path, an "
            "s3:// URI, an obstore store, or an icechunk Storage"
        )
        raise ValueError(msg)

    if isinstance(target, S3Store):
        return _from_s3_store(target, href=href, prefix=prefix)

    if isinstance(target, LocalStore):
        path = str(target.prefix)
        return _local_target(path, href=href)

    msg = (
        f"Unsupported store target {type(target).__name__}: use a local path, "
        "an s3:// URI, an obstore S3Store/LocalStore, or an icechunk Storage"
    )
    raise TypeError(msg)


def resolve_kerchunk_target(
    target: Any,
    *,
    href: str | None = None,
) -> StoreTarget:
    """Resolve a kerchunk-parquet target (a references *directory*).

    Kerchunk stores are plain URIs; obstore stores and icechunk ``Storage``
    objects are rejected because kerchunk writes through fsspec, not obspec.
    """

    from obstore.store import LocalStore, S3Store

    if isinstance(target, (S3Store, LocalStore)) or _is_icechunk_storage(target):
        msg = (
            "kerchunk stores are written through fsspec; pass the target as a "
            "path or URI, not an obstore store or icechunk Storage"
        )
        raise TypeError(msg)

    if isinstance(target, Path):
        return _local_target(str(target), href=href)

    if isinstance(target, str):
        parsed = urlparse(target)
        if parsed.scheme == "s3":
            return StoreTarget(
                href=href or _s3_href(parsed.netloc, parsed.path.lstrip("/")),
                access="s3",
            )
        if parsed.scheme in ("", "file"):
            path = parsed.path if parsed.scheme == "file" else target
            return _local_target(path, href=href)
        msg = f"Unsupported kerchunk target {target!r}: use a local path or s3:// URI"
        raise ValueError(msg)

    msg = (
        f"Unsupported kerchunk target {type(target).__name__}: use a local path "
        "or an s3:// URI"
    )
    raise TypeError(msg)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _local_target(path: str, *, href: str | None) -> StoreTarget:
    return StoreTarget(
        href=href or path,
        access="file",
        storage=_new_local_storage(path),
    )


def _new_local_storage(path: str) -> Any:
    import icechunk

    return icechunk.Storage.new_local_filesystem(path)


def _from_s3_store(
    store: Any,
    *,
    href: str | None,
    prefix: str | None,
) -> StoreTarget:
    """Bridge a configured obstore ``S3Store`` to an icechunk Storage.

    The store's credential provider (for example NASA Earthdata's refreshable
    S3 credentials) is wrapped as an icechunk refreshable credential, so the
    provider — not its cached keys — is what crosses into icechunk.
    """

    import icechunk

    config = store.config or {}
    client_options = getattr(store, "client_options", None) or {}
    bucket = config["bucket"]
    resolved_prefix = prefix if prefix is not None else store.prefix
    allow_http = str(client_options.get("allow_http", "")).lower() == "true"
    skip_signature = str(config.get("skip_signature", "")).lower() == "true"

    options = icechunk.S3Options(
        region=config.get("region"),
        endpoint_url=config.get("endpoint"),
        allow_http=allow_http,
        anonymous=skip_signature,
        force_path_style=str(
            config.get("virtual_hosted_style_request", "")
        ).lower()
        == "false",
    )

    provider = getattr(store, "credential_provider", None)
    credentials: Any
    if provider is not None:
        credentials = icechunk.s3_credentials(get_credentials=_provider_bridge(provider))
    elif skip_signature:
        credentials = icechunk.s3_anonymous_credentials()
    else:
        credentials = icechunk.S3Credentials.FromEnv()

    storage = icechunk.Storage.new_s3_object_store(
        options,
        bucket,
        resolved_prefix or "",
        credentials=credentials,
    )
    return StoreTarget(
        href=href or _s3_href(bucket, resolved_prefix or ""),
        access="s3",
        storage=storage,
    )


def _provider_bridge(provider: Any) -> Any:
    """Adapt an obstore credential provider to icechunk's callback shape."""

    import icechunk

    def get_credentials() -> Any:
        credential = provider()
        return icechunk.S3StaticCredentials(
            access_key_id=credential["access_key_id"],
            secret_access_key=credential["secret_access_key"],
            session_token=credential.get("token"),
            expires_after=credential.get("expires_at"),
        )

    return get_credentials


def _s3_href(bucket: str, prefix: str) -> str:
    prefix = prefix.strip("/")
    return f"s3://{bucket}/{prefix}" if prefix else f"s3://{bucket}"


def _access_for(href: str) -> StoreAccess:
    return "s3" if urlparse(href).scheme == "s3" else "file"


def _is_icechunk_storage(target: Any) -> bool:
    try:
        import icechunk
    except ImportError:  # pragma: no cover - icechunk is a hard dependency
        return False
    return isinstance(target, icechunk.Storage)

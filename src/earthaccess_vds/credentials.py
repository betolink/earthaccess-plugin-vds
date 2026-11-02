"""Credential resolution and obstore registry construction.

Ported from ``earthaccess.virtual._credentials``. The registry may be built
from an :class:`~earthaccess_plugin_spec.AuthHandle` (preferred: the plugin
context, which logs in lazily) or by falling back to earthaccess's own login
state. The serializable-config/worker-side variant lands with the distributed
work: nothing in this module should ever be pickled with credentials embedded
by the engine itself.
"""

from __future__ import annotations

import logging
import os
import threading
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import earthaccess

from .types import AccessType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from earthaccess_plugin_spec import AuthHandle

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RegistryConfig:
    """Credential-free description of the registry a task needs.

    Shipped to Dask workers instead of an obstore registry; the worker resolves
    the token from its own environment and builds the stores there.
    """

    access: str
    scheme: str
    host: str
    credentials_endpoint: str | None = None
    region: str = "us-west-2"
    needs_token: bool = True

    def to_payload(self) -> dict[str, Any]:
        return asdict(self)

    def build(self, token: str) -> Any:
        from obspec_utils.registry import ObjectStoreRegistry
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider
        from obstore.store import HTTPStore, S3Store

        if self.scheme == "s3":
            provider = None
            if self.credentials_endpoint is not None:
                provider = NasaEarthdataCredentialProvider(
                    self.credentials_endpoint,
                    auth=token,
                )
            store = S3Store(
                bucket=self.host,
                region=self.region,
                credential_provider=provider,
                virtual_hosted_style_request=False,
                client_options={"allow_http": True},
            )
            return ObjectStoreRegistry({f"s3://{self.host}": store})

        options: Any = {}
        if self.needs_token and token:
            options["default_headers"] = {"Authorization": f"Bearer {token}"}
        store = HTTPStore.from_url(f"https://{self.host}", client_options=options)
        return ObjectStoreRegistry({f"https://{self.host}": store})


class LazyObjectStoreRegistry:
    """A registry that rebuilds its credentialed stores after pickling.

    Client-side it holds the token (so the first resolve is immediate); its
    ``__reduce__`` ships only :class:`RegistryConfig`, so a Dask worker rebuilds
    the registry from its own environment. Reading credentials never leave the
    process that holds them.
    """

    def __init__(self, config: RegistryConfig, *, token: str | None = None) -> None:
        self._config = config
        self._token = token
        self._real: Any = None
        self._lock = threading.Lock()

    @property
    def config(self) -> RegistryConfig:
        return self._config

    def _ensure(self) -> Any:
        if self._real is None:
            with self._lock:
                if self._real is None:
                    token = self._token
                    if token is None and self._config.needs_token:
                        token = _worker_token()
                    self._real = self._config.build(token or "")
        return self._real

    def resolve(self, url: str) -> Any:
        return self._ensure().resolve(url)

    def register(self, url: str, store: Any) -> None:
        self._ensure().register(url, store)

    def __reduce__(self) -> Any:
        return (_rebuild_lazy_registry, (self._config,))


def _rebuild_lazy_registry(config: RegistryConfig) -> LazyObjectStoreRegistry:
    return LazyObjectStoreRegistry(config)


def _worker_token() -> str:
    """Resolve an EDL token where the task runs (env, then netrc)."""

    token = os.environ.get("EARTHDATA_TOKEN")
    if token:
        return token

    import earthaccess

    for strategy in ("environment", "netrc"):
        try:
            earthaccess.login(strategy=strategy)
            break
        except Exception:  # noqa: BLE001, S112 - try the next strategy
            continue
    auth = getattr(earthaccess, "__auth__", None)
    resolved = getattr(auth, "token", None)
    if isinstance(resolved, dict) and resolved.get("access_token"):
        return str(resolved["access_token"])
    if isinstance(resolved, str):
        return resolved
    msg = (
        "this worker has no Earthdata credentials: set EARTHDATA_TOKEN (or "
        "EARTHDATA_USERNAME/EARTHDATA_PASSWORD), provide ~/.netrc, or call "
        "earthaccess.distribute_credentials(...) from the client"
    )
    raise ValueError(msg)


def get_granule_credentials_endpoint_and_region(granule: Any) -> tuple[str, str]:
    """Return the S3 credentials endpoint and region for a granule.

    The endpoint is read from the granule's UMM-G record first. If absent, a
    CMR collection query is performed and the information is taken from the
    UMM-C record.
    """
    credentials_endpoint = granule.get_s3_credentials_endpoint()
    region = "us-west-2"

    if credentials_endpoint is None:
        collection_results = earthaccess.search_datasets(
            count=1,
            concept_id=granule["meta"]["collection-concept-id"],
        )
        collection = cast(
            "earthaccess.DataCollection",
            collection_results[0],
        )
        collection_s3_bucket = collection.s3_bucket
        credentials_endpoint = collection_s3_bucket.get("S3CredentialsAPIEndpoint")
        region = collection_s3_bucket.get("Region", "us-west-2")

    if credentials_endpoint is None:
        msg = (
            "The collection did not provide an S3CredentialsAPIEndpoint. "
            "Direct S3 access is not available for this granule."
        )
        raise ValueError(msg)

    return credentials_endpoint, region


def validate_granules(granules: Sequence[Any]) -> None:
    """Validate that the given granules are suitable for virtualizing."""
    if not granules or len(granules) == 0:
        msg = "No valid granules provided."
        raise ValueError(msg)
    for granule in granules:
        if not granule.data_links():
            msg = f"Granule {granule['meta']['concept-id']} has no data links."
            raise ValueError(msg)


def _edl_token(auth: AuthHandle | None) -> str:
    """The EDL bearer token, from the auth handle or earthaccess's login state.

    Older earthaccess releases raise from ``AuthHandle.token()`` because their
    ``get_edl_token()`` expects a string while ``Auth.token`` is the EDL token
    response mapping; fall back to reading that mapping directly.
    """
    if auth is not None:
        try:
            return auth.token()
        except RuntimeError:
            pass
    token = getattr(earthaccess.__auth__, "token", None)
    if isinstance(token, dict) and token.get("access_token"):
        return str(token["access_token"])
    msg = "You must be logged in to use indirect access. Call earthaccess.login() first."
    raise ValueError(msg)


def build_obstore_registry(
    granules: Sequence[Any],
    access: AccessType,
    *,
    auth: AuthHandle | None = None,
) -> Any:
    """Build an obstore ``ObjectStoreRegistry`` for the given granules.

    Only the first granule is inspected to determine the host/bucket.
    ``access="direct"`` builds an authenticated ``S3Store``; ``access="indirect"``
    builds an ``HTTPStore`` using the user's EDL Bearer token.

    Raises:
        ValueError: If ``access="indirect"`` and the user is not authenticated.
        ValueError: If ``access="direct"`` and no S3 credentials endpoint can
            be resolved.
        ImportError: If virtualizarr/obstore are not installed.
    """
    try:
        import obspec_utils  # noqa: F401, PLC0415
        import obstore  # noqa: F401, PLC0415
    except ImportError:
        msg = "earthaccess_vds requires `obstore>=0.12` and `virtualizarr>=2.8`"
        raise ImportError(msg) from None

    validate_granules(granules)

    parsed_url = urlparse(granules[0].data_links(access=access)[0])
    token = _edl_token(auth)
    host = parsed_url.netloc

    if access == "direct":
        credentials_endpoint, region = get_granule_credentials_endpoint_and_region(
            granules[0],
        )
        config = RegistryConfig(
            access="direct",
            scheme="s3",
            host=host,
            credentials_endpoint=credentials_endpoint,
            region=region,
        )
    else:
        config = RegistryConfig(access="indirect", scheme="https", host=host)

    # The object holds the token client-side; pickling ships only the config.
    return LazyObjectStoreRegistry(config, token=token)


def build_registry_for_urls(
    urls: Sequence[str],
    *,
    auth: AuthHandle | None = None,
) -> LazyObjectStoreRegistry:
    """Build a registry for plain file URLs (not granules).

    Used when the input is a list of file assets (for example Harmony subset
    output) rather than NASA granules:

    - ``s3://`` uses the environment's AWS credentials, because derived outputs
      live in the requester's bucket rather than behind NASA S3 credentials;
    - NASA HTTPS hosts get the EDL bearer token;
    - other HTTPS hosts stay anonymous, so the token is never sent to a third
      party (presigned URLs do not need it).
    """

    first = urlparse(str(urls[0]))
    if first.scheme == "s3":
        region = (
            os.environ.get("AWS_DEFAULT_REGION")
            or os.environ.get("AWS_REGION")
            or "us-west-2"
        )
        config = RegistryConfig(
            access="direct",
            scheme="s3",
            host=first.netloc,
            region=region,
            needs_token=False,
        )
        return LazyObjectStoreRegistry(config)

    nasa = "nasa.gov" in first.netloc.lower()
    config = RegistryConfig(
        access="indirect",
        scheme="https",
        host=first.netloc,
        needs_token=nasa,
    )
    return LazyObjectStoreRegistry(config, token=_edl_token(auth) if nasa else None)

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
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import earthaccess

from .types import AccessType

if TYPE_CHECKING:
    from collections.abc import Sequence

    from earthaccess_plugin_spec import AuthHandle

logger = logging.getLogger(__name__)


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
        from obspec_utils.registry import ObjectStoreRegistry  # noqa: PLC0415
        from obstore.auth.earthdata import NasaEarthdataCredentialProvider  # noqa: PLC0415
        from obstore.store import HTTPStore, S3Store  # noqa: PLC0415
    except ImportError:
        msg = "earthaccess_vds requires `obstore>=0.12` and `virtualizarr>=2.8`"
        raise ImportError(msg) from None

    validate_granules(granules)

    parsed_url = urlparse(granules[0].data_links(access=access)[0])
    token = _edl_token(auth)

    if access == "direct":
        credentials_endpoint, region = get_granule_credentials_endpoint_and_region(
            granules[0],
        )
        bucket = parsed_url.netloc
        s3_store = S3Store(
            bucket=bucket,
            region=region,
            credential_provider=NasaEarthdataCredentialProvider(
                credentials_endpoint,
                auth=token,
            ),
            virtual_hosted_style_request=False,
            client_options={"allow_http": True},
        )
        return ObjectStoreRegistry({f"s3://{bucket}": s3_store})

    # indirect — HTTPS
    domain = parsed_url.netloc
    http_store = HTTPStore.from_url(
        f"https://{domain}",
        client_options={
            "default_headers": {
                "Authorization": f"Bearer {token}",
            },
        },
    )
    return ObjectStoreRegistry({f"https://{domain}": http_store})

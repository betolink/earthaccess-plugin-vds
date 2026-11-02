"""Credential resolution guards.

The engine reads the EDL token through the plugin context's ``AuthHandle``.
Older earthaccess releases raised there because ``Auth.token`` is the EDL token
response mapping while ``get_edl_token()`` expected a plain string; the plugin
falls back to reading the mapping so it keeps working across core versions.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import earthaccess
import pytest

from earthaccess_vds.credentials import _edl_token


class _RaisingHandle:
    def token(self) -> str:
        raise RuntimeError("Token is not available or not in expected format")


def test_token_falls_back_to_the_login_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        earthaccess,
        "__auth__",
        SimpleNamespace(token={"access_token": "the-token"}),
        raising=False,
    )
    handle: Any = _RaisingHandle()

    assert _edl_token(handle) == "the-token"


def test_token_comes_from_the_handle_when_available() -> None:
    handle: Any = SimpleNamespace(token=lambda: "handle-token")

    assert _edl_token(handle) == "handle-token"


def test_token_error_names_the_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(earthaccess, "__auth__", None, raising=False)
    handle: Any = _RaisingHandle()

    with pytest.raises(ValueError, match="login"):
        _edl_token(handle)


def test_lazy_registry_pickles_without_the_token() -> None:
    import pickle

    from earthaccess_vds.credentials import LazyObjectStoreRegistry, RegistryConfig

    config = RegistryConfig(
        access="indirect",
        scheme="https",
        host="archive.podaac.earthdata.nasa.gov",
    )
    registry = LazyObjectStoreRegistry(config, token="super-secret-token")

    blob = pickle.dumps(registry)

    assert b"super-secret-token" not in blob
    restored = pickle.loads(blob)
    assert restored.config == config
    assert restored._token is None


def test_registry_config_payload_is_credential_free() -> None:
    from earthaccess_vds.credentials import RegistryConfig

    payload = RegistryConfig(
        access="direct",
        scheme="s3",
        host="podaac-bucket",
        credentials_endpoint="https://archive.podaac.earthdata.nasa.gov/s3credentials",
        region="us-west-2",
    ).to_payload()

    assert payload == {
        "access": "direct",
        "scheme": "s3",
        "host": "podaac-bucket",
        "credentials_endpoint": (
            "https://archive.podaac.earthdata.nasa.gov/s3credentials"
        ),
        "region": "us-west-2",
    }

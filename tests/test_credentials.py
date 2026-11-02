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

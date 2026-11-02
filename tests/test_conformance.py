"""Contract conformance for VDSPlugin.

The granule -> dataset step is faked; the store write is real (a local
icechunk repository under ``tmp_path``), so the job's ``assets`` output and
its lifecycle are exercised for real.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from typing import Any
from unittest.mock import patch

from earthaccess_plugin_spec.testing import Fixtures, run_conformance

from earthaccess_vds import VDSPlugin
from tests.synthetic import make_vds


@contextmanager
def _fake_virtualize() -> Iterator[None]:
    with patch(
        "earthaccess_vds.engine.virtualize",
        return_value=make_vds("2013-01-01"),
    ):
        yield


class _Fixtures(Fixtures):
    def backend(self) -> AbstractContextManager[Any]:
        return _fake_virtualize()


def test_conformance(tmp_path: Path) -> None:
    fixtures = _Fixtures(
        options={
            "store": str(tmp_path / "conformance.icechunk"),
            "concat_dim": "time",
        },
    )
    run_conformance(VDSPlugin, fixtures=fixtures)

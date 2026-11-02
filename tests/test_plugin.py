"""Top-level tests: drive ``VDSPlugin.request`` end to end on a local filesystem.

Only the granule -> virtual-dataset step is faked (no network). Stores are real
icechunk repositories and kerchunk-parquet directories under ``tmp_path``, so
the create/append/reopen behaviour is exercised through the public plugin API.
"""

from __future__ import annotations

from typing import Any

import pytest
import xarray as xr
from earthaccess_plugin_spec import Asset, GranuleResultSet, JobStatus
from earthaccess_plugin_spec.testing import fake_context, sample_result_set

from earthaccess_vds import VDSPlugin
from tests.synthetic import DATA_URL, captured_virtualize, make_vds


def _plugin() -> VDSPlugin:
    return VDSPlugin(fake_context())


def _request(monkeypatch: pytest.MonkeyPatch, vds: xr.Dataset, **options: Any) -> Any:
    captured_virtualize(monkeypatch, vds)
    job = _plugin().request(sample_result_set(), **options)
    status = job.wait(timeout=60)
    return job, status


def _open_icechunk(href: str) -> xr.Dataset:
    import icechunk

    repo = icechunk.Repository.open(icechunk.Storage.new_local_filesystem(href))
    return xr.open_zarr(repo.readonly_session("main").store, consolidated=False)


# ---------------------------------------------------------------------------
# icechunk: create and append
# ---------------------------------------------------------------------------


def test_icechunk_create_returns_a_reopenable_asset(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst.icechunk")
    job, status = _request(
        monkeypatch, make_vds("2013-01-01"), store=store, concat_dim="time"
    )

    assert status is JobStatus.SUCCEEDED, job.info()
    assert job.output == "assets"
    (asset,) = job.results()
    assert isinstance(asset, Asset)
    assert asset.href == store
    assert asset.access == "file"
    assert asset.format == "icechunk"
    assert asset.auth == "none"
    assert _open_icechunk(store).sizes["time"] == 1


def test_icechunk_append_defaults_append_dim_to_concat_dim(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst.icechunk")
    first, status = _request(
        monkeypatch, make_vds("2013-01-01"), store=store, concat_dim="time"
    )
    assert status is JobStatus.SUCCEEDED, first.info()

    second, status = _request(
        monkeypatch,
        make_vds("2013-01-02", url=f"{DATA_URL}/day2"),
        store=store,
        concat_dim="time",
    )

    assert status is JobStatus.SUCCEEDED, second.info()
    (asset,) = second.results()
    assert asset.href == store  # appends keep the href stable
    assert _open_icechunk(store).sizes["time"] == 2


def test_icechunk_existing_store_without_dimension_fails_clearly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst.icechunk")
    _request(monkeypatch, make_vds("2013-01-01"), store=store, concat_dim="time")

    job, status = _request(monkeypatch, make_vds("2013-01-02"), store=store)

    assert status is JobStatus.FAILED
    assert "append_dim" in str(job.info())


def test_icechunk_commit_false_leaves_the_store_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst.icechunk")
    job, status = _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        concat_dim="time",
        commit=False,
    )

    assert status is JobStatus.SUCCEEDED, job.info()
    import zarr.errors

    with pytest.raises(zarr.errors.GroupNotFoundError):
        _open_icechunk(store)


# ---------------------------------------------------------------------------
# kerchunk-parquet: create, append (versioned href), replace
# ---------------------------------------------------------------------------


def test_kerchunk_create_writes_a_parquet_references_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst_refs.parquet")
    job, status = _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )

    assert status is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()
    assert asset.format == "kerchunk"
    assert asset.href == store
    assert (tmp_path / "sst_refs.parquet").is_dir()
    assert xr.open_dataset(store, engine="kerchunk").sizes["time"] == 1


def test_kerchunk_append_rewrites_to_a_versioned_directory(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst_refs.parquet")
    first, status = _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )
    assert status is JobStatus.SUCCEEDED, first.info()

    second, status = _request(
        monkeypatch,
        make_vds("2013-01-02", url=f"{DATA_URL}/day2"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )

    assert status is JobStatus.SUCCEEDED, second.info()
    (asset,) = second.results()
    assert asset.href.endswith("sst_refs_v2.parquet")
    assert xr.open_dataset(asset.href, engine="kerchunk").sizes["time"] == 2
    # the original version is left untouched
    assert xr.open_dataset(store, engine="kerchunk").sizes["time"] == 1


def test_kerchunk_replace_keeps_the_href(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst_refs.parquet")
    _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )

    job, status = _request(
        monkeypatch,
        make_vds("2013-01-02", url=f"{DATA_URL}/day2"),
        store=store,
        format="kerchunk",
        concat_dim="time",
        replace=True,
    )

    assert status is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()
    assert asset.href == store
    assert xr.open_dataset(store, engine="kerchunk").sizes["time"] == 2


def test_kerchunk_append_without_dimension_fails_clearly(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    store = str(tmp_path / "sst_refs.parquet")
    _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )

    job, status = _request(
        monkeypatch, make_vds("2013-01-02"), store=store, format="kerchunk"
    )

    assert status is JobStatus.FAILED
    assert "append_dim" in str(job.info())


# ---------------------------------------------------------------------------
# store-to-store merge
# ---------------------------------------------------------------------------


def test_icechunk_source_merges_one_store_into_another(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    source = str(tmp_path / "v1.icechunk")
    target = str(tmp_path / "v2.icechunk")
    first, status = _request(
        monkeypatch, make_vds("2013-01-01"), store=source, concat_dim="time"
    )
    assert status is JobStatus.SUCCEEDED, first.info()

    captured_virtualize(monkeypatch, make_vds("2013-01-02", url=f"{DATA_URL}/day2"))
    job = _plugin().request(
        sample_result_set(),
        store=target,
        source=source,
        concat_dim="time",
    )
    assert job.wait(timeout=60) is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()
    assert asset.href == target
    assert _open_icechunk(target).sizes["time"] == 2


# ---------------------------------------------------------------------------
# non-persisting modes and eager validation
# ---------------------------------------------------------------------------


def test_no_store_returns_the_virtual_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vds = make_vds("2013-01-01")
    job, status = _request(monkeypatch, vds, concat_dim="time")

    assert status is JobStatus.SUCCEEDED, job.info()
    assert job.output == "virtual"
    assert job.results() is vds


def test_load_without_store_reports_xarray_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    vds = make_vds("2013-01-01").load()
    job, status = _request(monkeypatch, vds, concat_dim="time", load=True)

    assert status is JobStatus.SUCCEEDED, job.info()
    assert job.output == "xarray"
    assert isinstance(job.results(), xr.Dataset)


def test_load_with_store_is_rejected_with_open_guidance() -> None:
    with pytest.raises(ValueError, match=r"ea\.open\(asset\.href, load=True\)"):
        _plugin().request(sample_result_set(), store="x.icechunk", load=True)


def test_removed_scheduler_option_is_rejected() -> None:
    with pytest.raises(TypeError):
        _plugin().request(
            sample_result_set(),
            store="x.icechunk",
            scheduler_url="tcp://scheduler:8786",
        )


def test_source_requires_a_target_store() -> None:
    with pytest.raises(ValueError, match="store="):
        _plugin().request(sample_result_set(), source="v1.icechunk")


def test_replace_is_kerchunk_only() -> None:
    with pytest.raises(ValueError, match="kerchunk"):
        _plugin().request(
            sample_result_set(), store="sst.icechunk", replace=True
        )


# ---------------------------------------------------------------------------
# capabilities
# ---------------------------------------------------------------------------


class _Granule(dict):
    def data_links(self, access: str = "indirect") -> list[str]:
        return [DATA_URL]


def test_capabilities_advertise_virtualize_and_append() -> None:
    caps = _plugin().capabilities(GranuleResultSet([_Granule(meta={}, umm={})]))

    assert caps.ok is True
    assert caps.formats == frozenset({"icechunk", "kerchunk"})
    assert {"virtualize", "append"} <= {str(op) for op in caps.operations}
    assert "icechunk" in str(caps)


# ---------------------------------------------------------------------------
# end to end: the Asset the plugin returns is openable by earthaccess.open
# ---------------------------------------------------------------------------


def test_earthaccess_open_opens_the_icechunk_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    import earthaccess

    store = str(tmp_path / "sst.icechunk")
    job, status = _request(
        monkeypatch, make_vds("2013-01-01"), store=store, concat_dim="time"
    )
    assert status is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()

    ds = earthaccess.open(asset)

    assert isinstance(ds, xr.Dataset)
    assert ds.sizes["time"] == 1


def test_earthaccess_open_opens_the_kerchunk_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    import earthaccess

    store = str(tmp_path / "sst_refs.parquet")
    job, status = _request(
        monkeypatch,
        make_vds("2013-01-01"),
        store=store,
        format="kerchunk",
        concat_dim="time",
    )
    assert status is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()

    ds = earthaccess.open(asset)

    assert isinstance(ds, xr.Dataset)
    assert ds.sizes["time"] == 1

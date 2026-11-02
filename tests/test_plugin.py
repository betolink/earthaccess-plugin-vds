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


# ---------------------------------------------------------------------------
# progress reporting
# ---------------------------------------------------------------------------


def test_job_info_reports_live_virtualize_progress(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import threading
    import time

    gate = threading.Event()

    def slow_virtualize(
        granules: Any, *, progress: Any = None, parallel: Any = None, **kwargs: Any
    ) -> xr.Dataset:
        del kwargs
        progress.start("virtualize", total=2)
        parallel.submit(gate.wait)
        parallel.submit(gate.wait)
        while progress.opened < 2:
            time.sleep(0.01)
        return make_vds("2013-01-01")

    monkeypatch.setattr("earthaccess_vds.engine.virtualize", slow_virtualize)
    results = sample_result_set(
        umms=[{"GranuleUR": "G1"}, {"GranuleUR": "G2"}],
    )
    job = _plugin().request(results, concat_dim="time")

    info = job.info()
    assert info["phase"] == "virtualize"
    assert info["total"] == 2
    assert info["opened"] == 0
    assert info["percent"] == 0.0
    assert "cancel" in job.capabilities

    gate.set()
    assert job.wait(timeout=10) is JobStatus.SUCCEEDED, job.info()
    done = job.info()
    assert done["phase"] == "done"
    assert done["opened"] == 2
    assert done["percent"] == 100.0


def test_progress_executor_counts_and_cancels() -> None:
    import time

    from earthaccess_vds.progress import ProgressExecutor, ProgressState

    state = ProgressState()
    state.start("virtualize", total=4)
    executor = ProgressExecutor(state, show_progress=False)

    assert list(executor.map(lambda value: value * 2, range(4))) == [0, 2, 4, 6]
    assert state.snapshot()["opened"] == 4

    blocking = executor.submit(time.sleep, 5)
    assert executor.cancel() is True
    assert blocking.cancelled()
    executor.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------------------
# explicit Dask client
# ---------------------------------------------------------------------------


class _ImmediateFuture:
    def __init__(self, value: Any = None, exc: BaseException | None = None) -> None:
        self._value = value
        self._exc = exc
        self._callbacks: list[Any] = []
        self._cancelled = False

    def add_done_callback(self, fn: Any) -> None:
        self._callbacks.append(fn)
        fn(self)

    def cancelled(self) -> bool:
        return self._cancelled

    def exception(self) -> BaseException | None:
        return self._exc

    def result(self, timeout: float | None = None) -> Any:
        del timeout
        if self._exc is not None:
            raise self._exc
        return self._value

    def cancel(self) -> bool:
        self._cancelled = True
        return True


class _FakeClient:
    def __init__(self) -> None:
        self.dashboard_link = "http://dashboard"
        self.submitted: list[tuple[Any, tuple[Any, ...]]] = []
        self.canceled: list[Any] = []

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> _ImmediateFuture:
        self.submitted.append((fn, args))
        return _ImmediateFuture(fn(*args, **kwargs))

    def cancel(self, futures: Any) -> None:
        self.canceled.extend(futures)


def test_request_submits_opens_to_an_explicit_client(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    from earthaccess_vds.progress import ProgressExecutor

    client = _FakeClient()
    captured: dict[str, Any] = {}

    def fake_virtualize(
        granules: Any, *, progress: Any = None, parallel: Any = None, **kwargs: Any
    ) -> xr.Dataset:
        del kwargs
        captured["executor"] = parallel
        progress.start("virtualize", total=2)
        assert isinstance(parallel, ProgressExecutor)
        list(parallel.map(lambda value: value, range(2)))
        return make_vds("2013-01-01")

    monkeypatch.setattr("earthaccess_vds.engine.virtualize", fake_virtualize)
    job = _plugin().request(
        sample_result_set(umms=[{"GranuleUR": "G1"}, {"GranuleUR": "G2"}]),
        concat_dim="time",
        client=client,
    )

    assert job.wait(timeout=30) is JobStatus.SUCCEEDED, job.info()
    assert len(client.submitted) == 2
    assert job.info()["dashboard"] == "http://dashboard"
    assert captured["executor"].state.snapshot()["opened"] == 2
    assert isinstance(getattr(job, "_executor", None), ProgressExecutor)


def test_request_rejects_a_non_client() -> None:
    with pytest.raises(TypeError, match="distributed.Client"):
        _plugin().request(sample_result_set(), concat_dim="time", client=object())


def test_progress_executor_cancels_through_the_client() -> None:
    import time

    from earthaccess_vds.progress import ProgressExecutor, ProgressState

    client = _FakeClient()
    state = ProgressState()
    state.start("virtualize", total=1)
    executor = ProgressExecutor(state, client=client, show_progress=False)

    executor.submit(time.sleep, 0)

    assert executor.cancel() is True
    assert client.canceled


def test_open_virtual_load_false_returns_manifest_arrays(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    import earthaccess
    from virtualizarr.manifests import ManifestArray

    store = str(tmp_path / "sst.icechunk")
    job, status = _request(
        monkeypatch, make_vds("2013-01-01"), store=store, concat_dim="time"
    )
    assert status is JobStatus.SUCCEEDED, job.info()
    (asset,) = job.results()

    references = earthaccess.open_virtual(asset.href, load=False)

    assert isinstance(references["sst"].data, ManifestArray)
    assert references.sizes["time"] == 1


# ---------------------------------------------------------------------------
# list-of-assets input (e.g. Harmony subset output)
# ---------------------------------------------------------------------------


def test_assets_are_virtualized_via_the_hdf_parser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = captured_virtualize(
        monkeypatch, make_vds("2013-01-01", url="https://example.com/a.nc")
    )
    asset = Asset(
        href="https://example.com/a.nc", access="https", auth="edl", format="netcdf4"
    )

    job = _plugin().request(None, assets=[asset], concat_dim="time")
    assert job.wait(timeout=30) is JobStatus.SUCCEEDED, job.info()
    assert job.output == "virtual"

    granules = calls[0]["granules"]
    assert [granule.data_links()[0] for granule in granules] == [
        "https://example.com/a.nc"
    ]
    assert calls[0]["parser"] == "HDFParser"
    assert calls[0]["registry"] is not None


def test_assets_can_be_written_to_a_store(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Any
) -> None:
    captured_virtualize(
        monkeypatch, make_vds("2013-01-01", url="https://example.com/a.nc")
    )
    asset = Asset(
        href="https://example.com/a.nc", access="https", auth="edl", format="netcdf4"
    )
    store = str(tmp_path / "harmony.icechunk")

    job = _plugin().request(
        None, assets=[asset], store=store, concat_dim="time"
    )

    assert job.wait(timeout=30) is JobStatus.SUCCEEDED, job.info()
    (result,) = job.results()
    assert result.href == store
    assert _open_icechunk(store).sizes["time"] == 1


def test_opendap_assets_are_rejected() -> None:
    asset = Asset(href="https://x/dap4", access="opendap", auth="edl")

    with pytest.raises(ValueError, match="OPeNDAP"):
        _plugin().request(None, assets=[asset], concat_dim="time")


def test_non_file_assets_are_rejected() -> None:
    asset = Asset(href="https://x/data.zarr", access="https", auth="edl", format="zarr")

    with pytest.raises(ValueError, match="zarr"):
        _plugin().request(None, assets=[asset], concat_dim="time")


def test_assets_and_results_are_mutually_exclusive() -> None:
    asset = Asset(href="https://x/a.nc", access="https", auth="edl", format="netcdf4")

    with pytest.raises(ValueError, match="not both"):
        _plugin().request(sample_result_set(), assets=[asset], concat_dim="time")


def test_registry_for_derived_assets_keeps_the_token_local() -> None:
    from earthaccess_vds.credentials import build_registry_for_urls

    s3 = build_registry_for_urls(["s3://my-bucket/out.nc"])
    assert s3.config.host == "my-bucket"
    assert s3.config.credentials_endpoint is None
    assert s3.config.needs_token is False

    other = build_registry_for_urls(["https://storage.example.com/out.nc"])
    assert other.config.needs_token is False
    assert other._token is None

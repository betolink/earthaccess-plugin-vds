"""Synthetic manifest-form datasets shared by the top-level tests."""

from __future__ import annotations

from typing import Any

import numpy as np
import xarray as xr

DATA_URL = "https://example.com/data"


def make_vds(time: str, *, url: str | None = None) -> xr.Dataset:
    """A one-timestep manifest-form dataset pointing at a remote granule."""

    from virtualizarr.manifests import ChunkManifest, ManifestArray
    from zarr.codecs import BytesCodec
    from zarr.core.metadata.v3 import ArrayV3Metadata
    from zarr.dtype import parse_dtype

    metadata = ArrayV3Metadata(
        shape=(1,),
        data_type=parse_dtype("float32", zarr_format=3),
        chunk_grid={"name": "regular", "configuration": {"chunk_shape": (1,)}},
        chunk_key_encoding={"name": "default", "configuration": {"separator": "/"}},
        fill_value=np.float32("nan"),
        codecs=[BytesCodec()],
        attributes={},
        dimension_names=("time",),
    )
    manifest = ChunkManifest(
        entries={"0": {"path": url or DATA_URL, "offset": 0, "length": 4}},
    )
    array = ManifestArray(metadata=metadata, chunkmanifest=manifest)
    return xr.Dataset(
        {"sst": xr.DataArray(array, dims=["time"])},
        coords={"time": np.array([time], dtype="datetime64[ns]")},
    )


def captured_virtualize(monkeypatch: Any, vds: xr.Dataset) -> list[dict[str, Any]]:
    """Patch the granule -> dataset step; return the captured call kwargs."""

    calls: list[dict[str, Any]] = []

    def fake_virtualize(granules: Any, **kwargs: Any) -> xr.Dataset:
        calls.append(kwargs)
        return vds

    monkeypatch.setattr("earthaccess_vds.engine.virtualize", fake_virtualize)
    return calls

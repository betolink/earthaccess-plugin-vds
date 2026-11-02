# earthaccess-plugin-vds

Virtual datasets for earthaccess: virtualize NASA granules into
[icechunk](https://icechunk.io) (default) or kerchunk-parquet stores, create
new stores, and append to existing ones.

Built on [`earthaccess-plugin-spec`](https://github.com/earthaccess-dev/earthaccess-plugin-spec).

## Install

```bash
pip install "earthaccess-plugin-vds"          # icechunk only
pip install "earthaccess-plugin-vds[kerchunk]"    # kerchunk-parquet support
pip install "earthaccess-plugin-vds[distributed]" # Dask client + JupyterLab dashboard
```

## Usage

```python
import earthaccess as ea

ea.login()
results = ea.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2013-01-02", "2013-01-11"),
)

# Create a local icechunk store; the job returns one Asset.
job = ea.plugins.vds.request(results, store="mur_sst.icechunk", concat_dim="time")
asset = job.results()[0]
ds = ea.open(asset.href)

# Append: the same call. The store exists, so append_dim defaults to
# concat_dim and the write goes into the existing repository.
more = ea.search_data(
    short_name="MUR-JPL-L4-GLOB-v4.1",
    temporal=("2013-01-11", "2013-01-21"),
)
asset = ea.plugins.vds.request(
    more, store=asset.href, concat_dim="time"
).results()[0]

# No store → just a virtual dataset (manifest form).
vds = ea.plugins.vds.request(results, concat_dim="time").results()

# Kerchunk-parquet: a references *directory* named *.parquet. Appends write a
# versioned sibling (mur_refs.parquet → mur_refs_v2.parquet) unless
# replace=True, which rewrites in place.
asset = ea.plugins.vds.request(
    more, store="mur_refs.parquet", format="kerchunk", concat_dim="time"
).results()[0]

# Store-to-store merge (both formats): merge a source store into a target.
ea.plugins.vds.request(store="cube_v2", source="cube_v1", append_dim="time")

# Virtualize another plugin's *file* output (e.g. a Harmony subset):
# pass Assets or plain URLs. Derived files have no DMR++ sidecars, so the
# default parser switches to HDFParser. OPeNDAP assets (pydap) are rejected:
# a DAP response is not a byte-range-readable file.
assets = ea.plugins.harmony.request(results, variables=["analysed_sst"]).results()
asset = ea.plugins.vds.request(
    None, assets=assets, store="subset.icechunk", concat_dim="time"
).results()[0]
```

`store=` also accepts an obstore `S3Store` / `LocalStore` (or an
`icechunk.Storage` together with `href=`) as a preconfigured
client-side escape hatch. Credentials are never serialized: `s3://` targets and
granule reads resolve them from the environment where the code runs, so the
same request also works on a Dask cluster whose workers carry the secrets.

## Options

| Option | Meaning |
|---|---|
| `store` | Target store: local path, `s3://` URI, obstore store, or Storage. |
| `format` | `"icechunk"` (default) or `"kerchunk"`. |
| `source` | Existing store to merge into `store`. |
| `assets` | File assets/URLs to virtualize instead of search results. |
| `append_dim` | Append dimension; defaults to `concat_dim` when the store exists. |
| `replace` | Kerchunk only: rewrite references in place (stable href). |
| `commit` | Icechunk only: commit the write (default `True`). |
| `access` | `"indirect"` (HTTPS, default) or `"direct"` (S3). |
| `parser` | VirtualiZarr parser name; `"DMRPPParser"` default with automatic `"HDFParser"` fallback. |
| `group`, `concat_dim`, `combine`, `preprocess`, `drop_variables`, `loadable_variables` | VirtualiZarr / xarray combine controls. |
| `load` | Without `store`: return a concrete lazy `Dataset` (parquet round-trip). With `store`: rejected — use `ea.open(asset.href, load=True)`. |
| `tree` | Open a single granule as a `DataTree` (cannot be persisted). |
| `show_progress` | Progress reporting (reserved for the distributed executor). |

## Outputs

| Request | `job.output` | `job.results()` |
|---|---|---|
| `store=` given | `assets` | `[Asset(href, access, auth="none", format="icechunk"\|"kerchunk")]` |
| no store, `load=False` | `virtual` | manifest-form `xarray.Dataset` / `DataTree` |
| no store, `load=True` | `xarray` | concrete lazily-loaded `xarray.Dataset` |

## Development

```bash
pip install -e ".[test,kerchunk]"
pytest
ruff check .
mypy
```

The test suite is offline: the granule → virtual-dataset step is faked, while
create/append/reopen run against real local icechunk repositories and
kerchunk-parquet directories.

## License

MIT.

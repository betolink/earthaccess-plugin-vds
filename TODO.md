# earthaccess-plugin-vds — TODO / Progress

Handoff notes for any agent continuing this work. **Canonical design plan:**
`~/github/betolink/earthaccess/docs/plans/virtual-plugin.md` (fork branch
`virtual-plugin`) — read §3 (user-facing design) and §13 (decisions) before
writing code. This file tracks progress + next steps only.

Last updated: 2026-11-02 (after engine slice, commit `51173d7`).

## Repos / branches / state

| Repo | Branch | State |
|---|---|---|
| `~/github/betolink/earthaccess-plugin-spec` | `vds-contract` (pushed) | Phase 1 done, **not merged/tagged yet** |
| `~/github/betolink/earthaccess` (fork) | `virtual-plugin` (pushed) | plan + spec-branch pin committed |
| `~/github/betolink/earthaccess-plugin-vds` | `main` (pushed) | Phase 2 slice 1 done (engine) |

Environments:
- Plugin venv: `cd earthaccess-plugin-vds && .venv/bin/python -m pytest -q`;
  ruff via `.venv/bin/python -m ruff check .`; mypy via `.venv/bin/python -m mypy`.
  Contains editable `earthaccess` (fork checkout), spec `0.3.0 @ vds-contract`,
  virtualizarr 2.8.0 / icechunk 2.3.1 / obstore 0.12.1.
- Fork venv (`earthaccess/.venv`) has the spec installed **editable** from
  `../earthaccess-plugin-spec` — used to prove harmony/pydap/sliderule load.
- Never run bare `uv run` in the fork (re-syncs from `uv.lock` and reverts pins).

## Done

**Phase 1 — contract (`vds-contract`, commits `5af0e33`, `d793d8c`):**
`OUTPUTS` += `"virtual"`; `Format` += `"icechunk"`/`"kerchunk"`; `Asset.access`
+= `"file"`; MIME aliases in `format_name()` (`x-icechunk`, `vnd.icechunk`,
`x-kerchunk`, `vnd.kerchunk+parquet`, `application/virtual+kerchunk`);
`Capability.VIRTUALIZE` (outputs `{virtual, assets}`) and `APPEND` (options
include `source`); conformance duck-types the `virtual` output kind;
cookiecutter offers `virtual`. 91 tests green; non-breakage proven (three
plugins load, fork `test_plugins.py` 22/22). `CONTRACT_VERSION` stays 1.

**Phase 2 slice 1 — engine (plugin `main`, commit `51173d7`):**
`parser.py` (resolve/URL mapping, `IcechunkParser` support), `credentials.py`
(`build_obstore_registry` with optional `AuthHandle`), `engine.py`
(`virtualize` + dispatch + parquet-only `load=True` round-trip), `types.py`;
`tests/test_engine.py` — 20 tests, TDD (red → green), mypy/ruff clean.

**Plan revisions (fork `virtual-plugin`, commits `db6ffee` + pin commit):**
persisting always returns an Asset; `load` split from store writes;
`source=` store-to-store merge; fork pins spec `@vds-contract` (+ `uv.lock`).

## Binding design decisions (do not relitigate)

1. **No JSON output anywhere.** Kerchunk = parquet refs **directory**,
   conventionally *named* `*.parquet` (that's what URI sniffing matches).
2. **Output kinds:** `store=` → `output="assets"` = `[Asset(href, access="file"|"s3",
   auth="none", format="icechunk"|"kerchunk")]`; no store + `load=False` →
   `"virtual"`; no store + `load=True` → `"xarray"`. Plugin declares
   `output="assets"` and overrides per job (pydap precedent). `load=True` +
   `store=` → `ValueError` pointing at `ea.open(asset.href, load=True)`.
3. **Create vs append** decided by target-store existence; `append_dim`
   defaults to `concat_dim` when the store exists. No user flag.
4. **`source=`** (Asset | URI) opens another store (parser sniffed:
   `IcechunkParser` / `KerchunkParquetParser`) and merges it into `store`;
   `requires_results` effectively false when only `source` given.
5. **Credentials are never serialized.** No `scheduler_url`, no
   `write_credentials` options (both raise `TypeError`). Workers resolve
   `EARTHDATA_USERNAME`/`EARTHDATA_PASSWORD` + AWS env themselves (lazy
   `earthaccess.login(strategy="environment")` per worker); the client ships
   only a serializable registry *config* (URLs/access/endpoints). Explicit
   obstore store / icechunk `Storage` = client-side escape hatch, never
   pickled. Active distributed `Client` is detected (`distributed
   .default_client()`); dask default routing does the rest.
6. **Icechunk writer:** `Repository.open_or_create(storage, config=<vcc cfg>)`
   → `writable_session("main")` → `to_icechunk(append_dim=…)` → `commit`.
   Verified: on create the config persists automatically; on an existing repo
   the passed config is **ignored** → explicit VCC merge required
   (`Repository.fetch_config(storage)` → merge new `scheme://host/` prefixes →
   `save_config()`). One write task per job (never concurrent creates).
   `authorize_virtual_chunk_access=None` is deprecated — always pass explicit
   mapping client-side; env sentinels (`S3Credentials.FromEnv()`,
   `HttpAccess`, `LocalFileSystemAccess`) on workers.
7. **Kerchunk writer:** `vds.virtualize.to_kerchunk(format="dict")` →
   `kerchunk.df.refs_to_dataframe(refs, url, storage_options=…)` (fsspec,
   unconditional; `to_kerchunk` itself has no storage_options). Append =
   read-combine-rewrite (O(full)): new versioned refs dir by default
   (`mur_refs.parquet` → `mur_refs_v2.parquet`), `replace=True` = fsspec-rm
   root + rewrite in place (stable href).
8. **`ea.open()` dispatches on `Asset.format`** (icechunk/kerchunk), URI
   sniffing as fallback; `.json`/`.parquet` bare strings are NOT intercepted.

## Verified facts / gotchas

- `Repository.fetch_config` is **static** — takes `storage`, not self.
- `ObjectStoreRegistry` imports from `obspec_utils.registry` (virtualizarr's
  re-export is deprecated).
- `to_kerchunk(format="parquet")` writes a refs *directory*; parquet refs only
  carry Zarr v2 metadata (`zarr_format=3` raises).
- `IcechunkParser(branch=, tag=, snapshot_id=, group=, skip_variables=,
  batch_size=)` — keyword-only.
- obstore `S3Store` exposes `.config` (dict: bucket/region/endpoint) and
  `.credential_provider`; icechunk 2.3.1 has **no** constructor taking an
  obstore store instance — build `icechunk.S3Options(...)` +
  `Storage.new_s3_object_store(opts, bucket, prefix, credentials=...)`;
  `LocalStore`/path → `Storage.new_local_filesystem(path)`.
- `DataCollection.virtual_collection_url()` is **called but not defined
  anywhere in the fork** (latent AttributeError) — must be implemented in
  Phase 3.
- The spec repo has moved to the `earthaccess-dev` org (betolink URLs
  redirect); retarget all pins when merging/tagging.
- uv pip refuses one resolution with two different direct-URL pins of the same
  dist → keep fork + plugin spec pins identical (`@vds-contract` until the
  tag). After editing the fork's pyproject, reinstall its editable package so
  metadata refreshes.
- The plugin's `tests/` has no `__init__.py` → mypy checks test files strictly
  (annotate everything, or add `__init__.py`).

## Remaining work (in order; TDD everywhere — red first, mocked/offline tests)

### Phase 2 (this repo)

1. **`storage.py` + `tests/test_storage.py`** — target resolution:
   `str | Path | S3Store | LocalStore | icechunk.Storage` → icechunk `Storage`
   (+ bucket/prefix); derive `Asset` `href`/`access`/`format`. Bridge the
   obstore credential provider → `get_credentials`/`S3StaticCredentials`
   (spike: exact obstore `Credential` field names — one function).
2. **`writer.py` + `tests/test_writer.py`** — port fixtures from
   `~/github/betolink/earthaccess/tests/unit/test_virtual_write.py`
   (`FakeManifestArray` etc.). Icechunk writer (open_or_create, **VCC
   merge-on-append** against a mocked `Repository`, commit, append_dim);
   kerchunk writer (dict → `refs_to_dataframe`, versioned dir, `replace=True`
   rm+rewrite with mocked fsspec); Asset construction; validation (existing
   store without `append_dim`/`concat_dim` → clear error; `replace` only with
   kerchunk; `commit` icechunk-only).
3. **`credentials.py` worker-side + tests** — serializable registry config
   (no secrets), per-worker lazy env login, registry build inside the task;
   **no-secrets test**: `pickle.dumps` of every task payload contains no
   token/key material; typed error naming missing env vars.
4. **`progress.py` + tests** — `ProgressExecutor` (concurrent.futures Executor
   protocol: `submit`/`map`/context manager; detects active client;
   `client.compute` → futures; tqdm callbacks; dashboard link) and
   `VirtualJob(Job)` (`_poll` aggregates futures → status +
   `{"opened", "total", "failed", "phase", "dashboard"}`; `_cancel` =
   `client.cancel`; local threaded fallback). Spikes: whether
   `open_virtual_mfdataset` drives the executor via `submit` or `map`; tqdm
   callback ordering.
5. **`engine.open_refs_store()` + tests** — port `_open_virtual_via_virtualizarr`
   (parquet branch) + the IcechunkParser path; used by append and `source=`;
   combine via `xr.combine_nested([old, delta], concat_dim=…)`.
6. **`plugin.py` + conformance** — final `Options` (see plan §3.2), output
   wiring (assets default, per-job override), `_supports` (data-links check →
   VIRTUALIZE/APPEND + store formats), `_request` orchestration (delta →
   write; `source` merge; `append_dim ← concat_dim`), validation rules;
   fixtures for `run_conformance` (drive the store path so `job.output`
   matches the declared `"assets"`); update `tests/test_conformance.py`;
   README.
7. Commit style: conventional commits, ruff + mypy clean, push.

### Phase 3 (fork, branch `virtual-plugin`)

- `earthaccess/plugins/registry.py`: `_KNOWN` += `("vds", "earthaccess-plugin-vds",
  "Virtual datasets: virtualize granules, create and append icechunk / kerchunk
  stores")` + test.
- `earthaccess/api.py::open()`: route single icechunk-sniffed `str`/`Path`
  (`icechunk://`, `.icechunk` suffix, local dir with
  `snapshots/1CECHNKREP0F1RSTCMT0`) and `DataCollection` → `open_virtual()`;
  dispatch on `Asset.format` for asset/job inputs; **implement
  `DataCollection.virtual_collection_url()`** (missing). Do not intercept bare
  `.json`/`.parquet`. Tests: tmp marker dir, mocked `open_virtual`, routing table.
- `pyproject.toml`: re-pin spec to the `v0.3.0` tag + refresh `uv.lock`; add
  optional extra `vds = ["earthaccess-plugin-vds @ git+…"]`.
- Notebooks: `docs/tutorials/plugins/vds.ipynb` (acceptance, minimal kwargs),
  `vds-in-depth.ipynb` (parsers, access modes, formats, bring-your-own-cluster
  + secrets, VCC auth, progress/cancel); remove `docs/tutorials/
  icechunk_virtual.ipynb`; update `docs/tutorials/vds/index.md`,
  `docs/tutorials/plugins/index.md`, `docs/.nav.yml`.

### Phase 4 — acceptance (see plan §8 for the full checklist)

4-worker `LocalCluster(processes=True)` with `EARTHDATA_USERNAME`/
`EARTHDATA_PASSWORD` exported **before** workers start; 2×10 MUR SST granules
(`MUR-JPL-L4-GLOB-v4.1`, Jan 2013); create → `ea.open(asset)` → append (active
Client, `show_progress=True`, `job.info()` mid-flight) → reopen spans both;
kerchunk create+append+open; assert pickled task payloads carry no secrets;
full fork suite green; `ea.list_plugins(details=True)` shows four plugins.

### Then

- Merge `vds-contract` → spec `main`, **tag `v0.3.0`**, retarget pins (plugin
  pyproject, fork pyproject + `uv.lock`) — also consider the `earthaccess-dev`
  org URLs.
- Out of scope (follow-ups): deprecation shims for `ea.virtualize`/
  `write_virtual`/`open_virtual` + engine removal from core; `from_assets`
  composition; real-S3 runs; PyPI publishing.

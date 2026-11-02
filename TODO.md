# earthaccess-plugin-vds — TODO / Progress

Handoff notes for any agent continuing this work. **Canonical design plan:**
`~/github/betolink/earthaccess/docs/plans/virtual-plugin.md` (fork branch
`virtual-plugin`) — read §3 (user-facing design) and §13 (decisions) before
writing code. This file tracks progress + next steps only.

Last updated: 2026-11-02 (after stores slice: local create+append works for
both formats; see `tests/test_plugin.py`).

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

**Phase 2 slice 2 — stores (plugin `main`):** `storage.py` (target resolution:
path / `s3://` / obstore `S3Store`|`LocalStore` / icechunk `Storage`; env-resolved
credentials; provider → refreshable `S3StaticCredentials` bridge), `writer.py`
(icechunk `open_or_create` + VCC merge-on-append + commit; kerchunk
`to_kerchunk(format="dict")` → `refs_to_dataframe`, versioned `_v2.parquet`
append dirs, `replace=True`), `engine.open_refs_store` /
`combine_virtual` / `materialize_coords` (append + `source=` reopening),
`plugin.py` wiring (Options, eager validation, output kinds, icechunk vs
kerchunk orchestration). Tests are top-level (`tests/test_plugin.py`, 16 cases
driving `VDSPlugin.request` end to end against **real local** icechunk repos and
kerchunk dirs; only `virtualize` is faked) + updated conformance fixtures
(`tests/test_conformance.py`, real local write). 37 tests green, mypy/ruff
clean. **Local-filesystem create+append acceptance for both formats is
covered.**

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

### Phase 2 (this repo) — remaining

1. **Worker-side credentials + no-secrets test** — serializable registry
   *config* (URLs, access mode, credentials endpoint — nothing secret), per-worker
   lazy `earthaccess.login(strategy="environment")` + registry build inside the
   task, typed error naming missing env vars. Test: `pickle.dumps` of every task
   payload contains no token/key material. This is the prerequisite for running
   the **open step** on a Dask cluster: today `ProgressExecutor` is a local
   thread pool (that is why no credential is ever serialized), and an active
   Client is only surfaced as `info()["dashboard"]`.
   Core now ships `ea.distribute_credentials(env, client=/cluster=/scheduler=)`
   (fork `virtual-plugin`): it prefers Coiled's private `send_private_envs`,
   refuses plain Dask unless `private=False`, sends the login token as
   `EARTHDATA_TOKEN`, and returns only key names. Use it to prepare workers,
   then have the plugin rebuild the registry from the environment in each task.
2. **Cluster executor** — once (1) lands, dispatch `ProgressExecutor` tasks via
   `client.submit` (distributed futures, `client.cancel`), and optionally attach
   `distributed.progress`. Dask's own `ProgressBar` needs a Dask collection;
   VirtualiZarr needs a `concurrent.futures.Executor`, so the executor is the
   right seam.
3. **Review passes** — `source=` for kerchunk (works via
   `open_refs_store`+`combine`; only the icechunk path is covered by a test
   today), `s3://` write targets (env credentials; untested offline),
   `tree=True` handling, docs in `README.md` (done — keep in sync).

### Phase 2 — done notes

- **Bug fixed after a real run:** `LazyEDLAuthHandle.token()` calls
  `earthaccess.get_edl_token()`, which compared `Auth.token` to `str`; the EDL
  login stores the token *response mapping* (`access_token` + metadata), so the
  accessor raised `RuntimeError: Token is not available or not in expected
  format` and every vds job failed at `0/N` before opening a granule. Fixed in
  core (`get_edl_token` accepts the mapping or a bare string, tests in
  `tests/unit/test_edl_token.py`) and the plugin now falls back to
  `earthaccess.__auth__.token["access_token"]` if an older core raises.
  Verified for real with MUR granules: icechunk create+append and kerchunk
  create+append both reach 4 time steps and `ea.open(asset)` reads them.
- The plugin's `distributed` extra installs `dask-labextension>=7.0` so the
  cluster/dashboard can be monitored from JupyterLab.

- **Progress:** `progress.py` adds `ProgressState` (thread-safe `opened`/`failed`/
  `total`/`phase`/`dashboard`/`percent`), `ProgressExecutor` (a
  `concurrent.futures.Executor` that VirtualiZarr drives via `map`; counts each
  finished granule, optional `tqdm` bar, cancel of pending futures) and
  `VirtualJob(Job)` (`info()` merges the counters with the job status; `phase`
  goes `virtualize → combine → write → done`; `cancel` capability auto-derived).
  `engine.virtualize(progress=…)` starts the phase and counts single-granule /
  DataTree opens; `plugin._request` creates the state/executor and returns a
  `VirtualJob`. Tests cover live `job.info()` mid-flight, the 100% terminal
  state, executor counting and cancellation.
- **Credential distribution:** `ea.distribute_credentials(...)` in core
  (`earthaccess/auth/distribute.py`, exported at the top level) — Coiled private
  channel by default, explicit `private=False` fallback via `client.run`, token
  defaults to the current login, env validation, keys-only summary. 8
  top-level tests in `tests/unit/test_distribute_credentials.py` (fork suite
  868 green).
- **Notebook:** `docs/tutorials/plugins/vds.ipynb` (fork) replaces
  `icechunk_virtual.ipynb`: search → request → `wait_with_progress` → `ea.open`
  → append both months → same for kerchunk parquet (versioned href), with
  progress/cancel notes. Nav + docs index updated.

- `storage.py` resolves path / `s3://` / obstore `S3Store`|`LocalStore` /
  icechunk `Storage` (+`href=` for bare Storage) and derives the Asset
  `href`/`access`. S3 targets use `Storage.new_s3_object_store(S3Options(...),
  bucket, prefix, credentials=FromEnv())`; an obstore credential provider is
  bridged to a refreshable `S3StaticCredentials` callback (credential flow
  confirmed: obstore `S3Credential` keys are `access_key_id`,
  `secret_access_key`, `token`, `expires_at`).
- `writer.write_icechunk` uses `Repository.exists` only to decide create vs
  append and VCC merging (single write task per job). On append it fetches the
  stored config, adds missing prefixes, `save_config()`, then
  `open_or_create` + `writable_session("main")` + `to_icechunk(append_dim)` +
  `commit`. Verified locally (not just mocked) that save_config persists the
  merged containers.
- `writer.write_kerchunk` requires a `*.parquet` target name (refs are a
  directory), versions to `_v2`/`_v3`, `replace=True` rm+rewrite, and writes via
  `to_kerchunk(format="dict")` → `refs_to_dataframe(refs, href, storage_options=)`.
- `engine.open_refs_store` reopens icechunk (`IcechunkParser`) and kerchunk
  (`KerchunkParquetParser`) stores in manifest form and attaches concrete
  coordinates; bare local paths are converted to `file://` for VirtualiZarr and
  registered with the matching obstore registry.
- `plugin.VDSPlugin` now validates eagerly (`load`+`store`, `replace` on
  icechunk, `source` without `store`, `tree`+`store`, empty request) and
  orchestrates: icechunk = sequential create/append (+ `source` first);
  kerchunk = read old refs (if any) + `source` + delta, `combine_nested`, write.
- Test layout: `tests/test_plugin.py` (top-level, real local stores),
  `tests/synthetic.py` (manifest dataset factory), `tests/test_conformance.py`
  (real local write with faked virtualize). `tests/` is now a package so
  mypy's `tests.*` override applies.

### Phase 3 (fork, branch `virtual-plugin`)

**Done:** `_KNOWN` now includes the `vds` row; `ea.open()` dispatches
`Asset(format=icechunk|kerchunk)` (single or list, incl. plugin jobs) and
icechunk-sniffed paths / `DataCollection` to `open_virtual(..., load=...)`
(bare `.parquet`/`.json` are not intercepted); `DataCollection.virtual_collection_url()`
now exists (reads the `VIRTUAL COLLECTION` RelatedUrl subtype). Top-level tests:
`tests/unit/test_open_dispatch.py` (8 cases) + vds in the registry row test;
860 unit tests green. **Remaining:**

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

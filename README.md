# earthaccess-plugin-vds

Virtual datasets for earthaccess: virtualize granules, create and append icechunk / kerchunk-parquet stores

An [`earthaccess`](https://github.com/earthaccess-dev/earthaccess) plugin built
on [`earthaccess-plugin-spec`](https://github.com/earthaccess-dev/earthaccess-plugin-spec).

## Install

```bash
pip install "earthaccess-plugin-vds"
```

## What to fill in

1. `src/earthaccess_vds/plugin.py` — declare your `Options` and
   implement `_request()`.

3. `tests/test_conformance.py` — point `Fixtures` at your backend fake and pass
   real options.

```bash
pip install -e ".[test]"
pytest
```

## Contract

- `name = "vds"`, `output = "assets"`,
  `auth = "edl"`, `needs = { umm }`.
- Import `` inside methods, never at module level.
- `request()` returns a `Job`; `results()` blocks and returns the output.

## License

MIT.

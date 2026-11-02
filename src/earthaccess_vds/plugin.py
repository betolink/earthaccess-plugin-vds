"""VDSPlugin: Virtual datasets for earthaccess: virtualize granules, create and append icechunk / kerchunk-parquet stores."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from earthaccess_plugin_spec import Capabilities, PluginBase


class VDSPlugin(PluginBase):
    name = "vds"
    auth = "edl"
    output = "assets"
    needs = frozenset({"umm"})

    @dataclass
    class Options:
        # TODO: declare the options this plugin accepts.
        pass

    def _supports(self, results: Any) -> Capabilities:
        # Optional. Report the collection's capabilities; callers ask with
        # plugin.capabilities(results) or plugin.capabilities("SHORT_NAME").
        # Return Capabilities(False, reason=...) to reject it early. This is
        # the default: accept everything, with no extra detail.
        return Capabilities(True)

    def _request(self, results: Any, options: Options) -> Any:
        # Import the backend inside the method so plugin discovery stays cheap.

        def work() -> Any:
            # TODO: call the backend and return the assets output.
            raise NotImplementedError("implement VDSPlugin._request")

        return self.job(work)

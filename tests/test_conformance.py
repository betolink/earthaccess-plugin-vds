from earthaccess_plugin_spec.testing import Fixtures, run_conformance

from earthaccess_vds import VDSPlugin


def test_conformance() -> None:
    # Replace Fixtures and options once the backend fake is in place.
    run_conformance(VDSPlugin, fixtures=Fixtures(options={}))

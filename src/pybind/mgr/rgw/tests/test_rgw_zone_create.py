import unittest
from unittest import mock

from rgw.module import Module
from ceph.deployment.service_spec import RGWSpec


class TestRgwZoneCreate(unittest.TestCase):
    """
    Regression tests for rgw_zone_create().

    The loop over rgw_specs previously had `return created_zones` nested
    inside the loop body (inside `if rgw_spec.rgw_zone is not None:`),
    so the function returned as soon as the first spec was processed,
    silently skipping any remaining specs. This is triggered by
    `ceph rgw zone create -i <file>` when the input file contains
    multiple '---'-separated zone specs, a case the CLI and the
    surrounding code (e.g. the pluralized "Zones ... created
    successfully" message) explicitly support.
    """

    def _make_module(self):
        # Module.__init__ requires a full mgr context we can't construct
        # here; bypass it and only set the attributes rgw_zone_create()
        # actually touches.
        module = Module.__new__(Module)
        module.env = mock.MagicMock()
        module.log = mock.MagicMock()
        return module

    def _make_specs(self, zone_names):
        specs = []
        for name in zone_names:
            spec = mock.MagicMock(spec=RGWSpec)
            spec.rgw_zone = name
            specs.append(spec)
        return specs

    @mock.patch('rgw.module.RGWAM')
    def test_multiple_specs_all_created(self, mock_rgwam_cls):
        """
        Regression test: with 3 zone specs, all 3 must be created and
        returned, not just the first one.
        """
        module = self._make_module()
        specs = self._make_specs(['zone-a', 'zone-b', 'zone-c'])

        # Bypass the spec-parsing branch entirely by calling the
        # loop logic directly via rgw_zone_create with inbuf unset and
        # zone_name/realm_token unset would raise; instead we exercise
        # the loop by patching _parse_rgw_specs to return our fake specs.
        with mock.patch.object(module, '_parse_rgw_specs', return_value=specs):
            result = module.rgw_zone_create(inbuf='fake-yaml-content')

        self.assertEqual(result, ['zone-a', 'zone-b', 'zone-c'])
        # RGWAM(...).zone_create should have been called once per spec
        self.assertEqual(mock_rgwam_cls.return_value.zone_create.call_count, 3)

    @mock.patch('rgw.module.RGWAM')
    def test_single_spec_still_works(self, mock_rgwam_cls):
        """A single-spec input (the common case) must continue to work."""
        module = self._make_module()
        specs = self._make_specs(['zone-only'])

        with mock.patch.object(module, '_parse_rgw_specs', return_value=specs):
            result = module.rgw_zone_create(inbuf='fake-yaml-content')

        self.assertEqual(result, ['zone-only'])
        self.assertEqual(mock_rgwam_cls.return_value.zone_create.call_count, 1)


if __name__ == '__main__':
    unittest.main()

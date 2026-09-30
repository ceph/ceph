from unittest import mock

import pytest

from rgw.module import Module
from ceph.deployment.service_spec import RGWSpec
from ceph.rgw.types import RGWAMException


class TestRgwZoneCreate:
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
        # actually touches. log is a read-only MgrModule property backed
        # by the unit-test logger, so it must not be assigned.
        module = Module.__new__(Module)
        module.env = mock.MagicMock()
        return module

    def _make_specs(self, zone_names):
        specs = []
        for name in zone_names:
            spec = mock.MagicMock(spec=RGWSpec)
            spec.rgw_zone = name
            specs.append(spec)
        return specs

    @pytest.mark.parametrize('zone_names', [
        pytest.param(['zone-a', 'zone-b', 'zone-c'], id='multiple'),
        pytest.param(['zone-only'], id='single'),
    ])
    def test_all_specs_created(self, zone_names):
        """Every requested zone spec is created and returned."""
        module = self._make_module()
        specs = self._make_specs(zone_names)

        with mock.patch('rgw.module.RGWAM') as mock_rgwam_cls, \
                mock.patch.object(module, '_parse_rgw_specs', return_value=specs):
            result = module.rgw_zone_create(inbuf='fake-yaml-content')

        assert result == zone_names
        assert mock_rgwam_cls.return_value.zone_create.call_count == len(zone_names)

    def test_later_spec_failure_propagates(self):
        """A failure on a later spec must surface, not be skipped."""
        module = self._make_module()
        specs = self._make_specs(['zone-a', 'zone-b', 'zone-c'])

        with mock.patch('rgw.module.RGWAM') as mock_rgwam_cls, \
                mock.patch.object(module, '_parse_rgw_specs', return_value=specs):
            mock_rgwam_cls.return_value.zone_create.side_effect = [
                None,
                RGWAMException('boom'),
            ]
            with pytest.raises(RGWAMException):
                module.rgw_zone_create(inbuf='fake-yaml-content')

        # zone-a was created, zone-b failed, zone-c must not be attempted
        assert mock_rgwam_cls.return_value.zone_create.call_count == 2

# -*- mode:python -*-
# vim: ts=4 sw=4 expandtab

from unittest.mock import MagicMock

from ceph.rgw.rgwam_core import RGWAM, EnvArgs, EntityKey, EntityID
from ceph.rgw.types import RGWAMCmdRunException


# ---------------------------------------------------------------------------
# Shared helpers / fixtures
# ---------------------------------------------------------------------------

def _make_period(master_zone_id, master_zonegroup_id,
                 zonegroups=None, realm_id='realm-id-1'):
    """Build a minimal period dict as radosgw-admin would return it."""
    if zonegroups is None:
        zonegroups = []
    return {
        'id': 'period-id-1',
        'epoch': 1,
        'master_zone': master_zone_id,
        'master_zonegroup': master_zonegroup_id,
        'realm_id': realm_id,
        'period_map': {
            'zonegroups': zonegroups,
        },
    }


def _make_zonegroup(zg_id, zg_name, master_zone_id, zones, is_master=True):
    """Build a minimal zonegroup entry inside a period map."""
    return {
        'id': zg_id,
        'name': zg_name,
        'api_name': zg_name,
        'is_master': is_master,
        'master_zone': master_zone_id,
        'endpoints': [],
        'zones': zones,
    }


def _make_zone_entry(zone_id, zone_name, endpoints=None):
    """Build a minimal zone entry inside a zonegroup."""
    return {
        'id': zone_id,
        'name': zone_name,
        'endpoints': endpoints or [],
    }


def _zone_params(zone_id, zone_name, access_key='AK', secret='SK'):
    """Build a minimal zone params dict (returned by 'zone get')."""
    return {
        'id': zone_id,
        'name': zone_name,
        'system_key': {
            'access_key': access_key,
            'secret_key': secret,
        },
    }


def _make_rgwam():
    """Return an RGWAM instance backed by a MagicMock manager."""
    env = EnvArgs(MagicMock())
    return RGWAM(env)


# ---------------------------------------------------------------------------
# Tests for _get_master_zone_ep_from_period
# ---------------------------------------------------------------------------

class TestGetMasterZoneEpFromPeriod:
    """Unit tests for RGWAM._get_master_zone_ep_from_period."""

    def test_returns_endpoints_for_master_zone(self):
        """When the period has a master zonegroup with a master zone that has
        endpoints, those endpoints are returned."""
        period = _make_period(
            master_zone_id='zone-a',
            master_zonegroup_id='zg-1',
            zonegroups=[
                _make_zonegroup(
                    'zg-1', 'default', 'zone-a',
                    zones=[
                        _make_zone_entry('zone-a', 'zone-a',
                                         endpoints=['http://host1:8080',
                                                    'http://host2:8080']),
                    ],
                    is_master=True,
                )
            ],
        )
        rgwam = _make_rgwam()
        eps = rgwam._get_master_zone_ep_from_period(period)
        assert eps == ['http://host1:8080', 'http://host2:8080']

    def test_returns_empty_list_when_master_zone_has_no_endpoints(self):
        """Master zone present but endpoints list is empty."""
        period = _make_period(
            master_zone_id='zone-a',
            master_zonegroup_id='zg-1',
            zonegroups=[
                _make_zonegroup(
                    'zg-1', 'default', 'zone-a',
                    zones=[_make_zone_entry('zone-a', 'zone-a', endpoints=[])],
                    is_master=True,
                )
            ],
        )
        rgwam = _make_rgwam()
        assert rgwam._get_master_zone_ep_from_period(period) == []

    def test_skips_non_master_zonegroups(self):
        """Only the master zonegroup is searched; secondary zonegroups are ignored."""
        period = _make_period(
            master_zone_id='zone-a',
            master_zonegroup_id='zg-1',
            zonegroups=[
                _make_zonegroup(
                    'zg-secondary', 'secondary', 'zone-b',
                    zones=[
                        _make_zone_entry('zone-b', 'zone-b',
                                         endpoints=['http://secondary:8080']),
                    ],
                    is_master=False,
                ),
                _make_zonegroup(
                    'zg-1', 'default', 'zone-a',
                    zones=[
                        _make_zone_entry('zone-a', 'zone-a',
                                         endpoints=['http://primary:8080']),
                    ],
                    is_master=True,
                ),
            ],
        )
        rgwam = _make_rgwam()
        assert rgwam._get_master_zone_ep_from_period(period) == ['http://primary:8080']

    def test_returns_empty_list_when_no_zonegroups(self):
        """Period map with an empty zonegroup list returns []."""
        period = _make_period('zone-a', 'zg-1', zonegroups=[])
        rgwam = _make_rgwam()
        assert rgwam._get_master_zone_ep_from_period(period) == []

    def test_returns_empty_list_when_period_map_absent(self):
        """Graceful degradation when 'period_map' key is missing entirely."""
        period = {
            'id': 'p1', 'epoch': 1, 'master_zone': 'z1',
            'master_zonegroup': 'zg1', 'realm_id': 'r1',
        }
        rgwam = _make_rgwam()
        assert rgwam._get_master_zone_ep_from_period(period) == []


# ---------------------------------------------------------------------------
# Tests for get_realms_info — primary-site scenario
# ---------------------------------------------------------------------------

class TestGetRealmsInfoPrimary:
    """On a primary site the master zone params exist locally."""

    def _setup_primary(self, rgwam, realm_name, realm_id,
                       master_zone_id, endpoint, access_key, secret):
        """Wire up all mocks for a single-realm primary-site setup."""
        period = _make_period(
            master_zone_id=master_zone_id,
            master_zonegroup_id='zg-1',
            realm_id=realm_id,
            zonegroups=[
                _make_zonegroup(
                    'zg-1', 'default', master_zone_id,
                    zones=[
                        _make_zone_entry(master_zone_id, 'zone-primary',
                                         endpoints=[endpoint]),
                    ],
                    is_master=True,
                )
            ],
        )
        realm_key = EntityKey(realm_name, realm_id)

        rgwam.realm_op = MagicMock(return_value=MagicMock(
            list=MagicMock(return_value=[realm_name]),
            get=MagicMock(return_value={'name': realm_name, 'id': realm_id}),
        ))
        rgwam.period_op = MagicMock(return_value=MagicMock(
            get=MagicMock(return_value=period),
        ))
        rgwam.zone_op = MagicMock(return_value=MagicMock(
            get=MagicMock(return_value=_zone_params(
                master_zone_id, 'zone-primary', access_key, secret
            )),
            list=MagicMock(return_value=['zone-primary']),
        ))
        return realm_key

    def test_primary_site_returns_correct_info(self):
        rgwam = _make_rgwam()
        self._setup_primary(
            rgwam,
            realm_name='my-realm',
            realm_id='realm-id-1',
            master_zone_id='zone-id-1',
            endpoint='http://primary-host:8080',
            access_key='ACCESS_KEY',
            secret='SECRET',
        )

        result = rgwam.get_realms_info()

        assert len(result) == 1
        info = result[0]
        assert info['realm_name'] == 'my-realm'
        assert info['realm_id'] == 'realm-id-1'
        assert info['master_zone_id'] == 'zone-id-1'
        assert info['endpoint'] == 'http://primary-host:8080'
        assert info['access_key'] == 'ACCESS_KEY'
        assert info['secret'] == 'SECRET'


# ---------------------------------------------------------------------------
# Tests for get_realms_info — secondary-site (fallback) scenario
# ---------------------------------------------------------------------------

class TestGetRealmsInfoSecondary:
    """On a secondary site the master zone params are absent; a local zone
    belonging to the same realm is used as fallback."""

    def _cmd_error(self):
        return RGWAMCmdRunException('zone get', -2, '', 'ENOENT')

    def _setup_secondary(self, rgwam, realm_name, realm_id,
                         master_zone_id, secondary_zone_id,
                         secondary_zone_name, endpoint,
                         access_key='AK', secret='SK',
                         extra_zone_name=None, extra_realm_zone_id=None):
        """
        Wire up mocks for a secondary-site scenario.

        If extra_zone_name / extra_realm_zone_id are provided a second zone
        (belonging to a *different* realm) is also present in zone list().
        """
        period = _make_period(
            master_zone_id=master_zone_id,
            master_zonegroup_id='zg-1',
            realm_id=realm_id,
            zonegroups=[
                _make_zonegroup(
                    'zg-1', 'default', master_zone_id,
                    zones=[
                        _make_zone_entry(master_zone_id, 'zone-primary',
                                         endpoints=[endpoint]),
                        _make_zone_entry(secondary_zone_id, secondary_zone_name,
                                         endpoints=[]),
                    ],
                    is_master=True,
                )
            ],
        )

        # 'zone get --zone-id=<master_zone_id>' raises ENOENT (not local)
        # 'zone get --rgw-zone=<secondary_zone_name>' succeeds
        local_zone_params = _zone_params(secondary_zone_id, secondary_zone_name,
                                         access_key, secret)

        zone_list = [secondary_zone_name]
        if extra_zone_name:
            zone_list.append(extra_zone_name)

        def zone_get_side_effect(entity):
            # First call is by EntityID (master zone — raises)
            if isinstance(entity, EntityID):
                raise self._cmd_error()
            # Subsequent calls are by EntityName
            if entity.name == secondary_zone_name:
                return local_zone_params
            if extra_zone_name and entity.name == extra_zone_name:
                # Belongs to a different realm
                return _zone_params(extra_realm_zone_id or 'other-zone-id',
                                    extra_zone_name, 'OTHER_AK', 'OTHER_SK')
            raise self._cmd_error()

        rgwam.realm_op = MagicMock(return_value=MagicMock(
            list=MagicMock(return_value=[realm_name]),
            get=MagicMock(return_value={'name': realm_name, 'id': realm_id}),
        ))
        rgwam.period_op = MagicMock(return_value=MagicMock(
            get=MagicMock(return_value=period),
        ))
        rgwam.zone_op = MagicMock(return_value=MagicMock(
            get=MagicMock(side_effect=zone_get_side_effect),
            list=MagicMock(return_value=zone_list),
        ))

    def test_secondary_site_uses_local_zone_credentials(self):
        """When master zone params are absent the local secondary zone's
        system_key is returned."""
        rgwam = _make_rgwam()
        self._setup_secondary(
            rgwam,
            realm_name='my-realm',
            realm_id='realm-id-1',
            master_zone_id='zone-primary-id',
            secondary_zone_id='zone-secondary-id',
            secondary_zone_name='zone-secondary',
            endpoint='http://primary-host:8080',
            access_key='SECONDARY_AK',
            secret='SECONDARY_SK',
        )

        result = rgwam.get_realms_info()

        assert len(result) == 1
        info = result[0]
        assert info['realm_name'] == 'my-realm'
        assert info['endpoint'] == 'http://primary-host:8080'
        assert info['access_key'] == 'SECONDARY_AK'
        assert info['secret'] == 'SECONDARY_SK'

    def test_secondary_site_ignores_zone_from_other_realm(self):
        """When zone list() contains a zone from a different realm (not in the
        current period's zone IDs), it must be skipped; only the zone that
        belongs to this realm is used."""
        rgwam = _make_rgwam()
        # extra_zone_name belongs to a different realm (its ID is not in the period)
        self._setup_secondary(
            rgwam,
            realm_name='my-realm',
            realm_id='realm-id-1',
            master_zone_id='zone-primary-id',
            secondary_zone_id='zone-secondary-id',
            secondary_zone_name='zone-secondary',
            endpoint='http://primary-host:8080',
            access_key='CORRECT_AK',
            secret='CORRECT_SK',
            extra_zone_name='zone-other-realm',
            extra_realm_zone_id='zone-other-id',   # NOT in this realm's period
        )

        # Make zone-other-realm appear *first* in the list to ensure the filter
        # rejects it and continues to zone-secondary.
        rgwam.zone_op.return_value.list.return_value = [
            'zone-other-realm', 'zone-secondary'
        ]

        result = rgwam.get_realms_info()

        assert len(result) == 1
        info = result[0]
        assert info['access_key'] == 'CORRECT_AK'
        assert info['secret'] == 'CORRECT_SK'

    def test_secondary_site_no_local_zone_yields_empty_creds(self):
        """If no local zone for this realm is found, access_key and secret
        fall back to empty strings."""
        rgwam = _make_rgwam()
        period = _make_period(
            master_zone_id='zone-primary-id',
            master_zonegroup_id='zg-1',
            realm_id='realm-id-1',
            zonegroups=[
                _make_zonegroup(
                    'zg-1', 'default', 'zone-primary-id',
                    zones=[
                        _make_zone_entry('zone-primary-id', 'zone-primary',
                                         endpoints=['http://primary:8080']),
                    ],
                    is_master=True,
                )
            ],
        )

        def zone_get_always_fails(entity):
            raise RGWAMCmdRunException('zone get', -2, '', 'ENOENT')

        rgwam.realm_op = MagicMock(return_value=MagicMock(
            list=MagicMock(return_value=['my-realm']),
            get=MagicMock(return_value={'name': 'my-realm', 'id': 'realm-id-1'}),
        ))
        rgwam.period_op = MagicMock(return_value=MagicMock(
            get=MagicMock(return_value=period),
        ))
        rgwam.zone_op = MagicMock(return_value=MagicMock(
            get=MagicMock(side_effect=zone_get_always_fails),
            list=MagicMock(return_value=['zone-primary']),
        ))

        result = rgwam.get_realms_info()

        assert len(result) == 1
        assert result[0]['access_key'] == ''
        assert result[0]['secret'] == ''

from dataclasses import replace
from unittest.mock import Mock

import pytest

import smb
from smb.enums import PersistentHandlesDurability as Durability
from smb.resources import ShareAvailability


def _cluster(cluster_id='foo', share_availability=None, durability=None):
    return smb.resources.Cluster(
        cluster_id=cluster_id,
        auth_mode=smb.enums.AuthMode.USER,
        clustering=smb.enums.SMBClustering.NEVER,
        user_group_settings=[
            smb.resources.UserGroupSource(
                source_type=smb.enums.UserGroupSourceType.EMPTY
            )
        ],
        share_defaults=(
            smb.resources.ShareDefaults(availability=share_availability)
            if share_availability is not None
            else None
        ),
        persistent_handles_durability=durability,
    )


def _share(share_id='s1', availability=None, cluster_id='foo'):
    return smb.resources.Share(
        cluster_id=cluster_id,
        share_id=share_id,
        cephfs=smb.resources.CephFSStorage(volume='cephfs'),
        availability=availability,
    )


@pytest.fixture
def amodule():
    module = smb.module.Module(
        'smb',
        '',
        '',
        internal_store=smb.config_store.MemConfigStore(),
        public_store=smb.config_store.MemConfigStore(),
        priv_store=smb.config_store.MemConfigStore(),
        path_resolver=smb.handler._FakePathResolver(),
        authorizer=smb.handler._FakeAuthorizer(),
        earmark_resolver=smb.handler._FakeEarmarkResolver(),
        update_orchestration=False,
    )
    module._handler._orch = Mock()
    return module


def _apply(module, resources):
    result = module._apply_res(resources)
    assert result.success, result.to_simplified()
    return result


def test_orchestration_history_lifecycle(amodule):
    cluster = _cluster()
    _apply(amodule, [cluster, _share()])
    entry = amodule._public_store['foo', 'cluster-info']
    assert entry.get()['orch_submitted'] is True
    _apply(
        amodule, [smb.resources.RemovedShare(cluster_id='foo', share_id='s1')]
    )
    orch = amodule._handler._orch
    amodule._handler._orch = None
    _apply(amodule, [cluster])
    assert entry.get()['orch_submitted'] is True
    enabled = _share(availability=ShareAvailability(persistent_handles=True))
    result = amodule._apply_res([enabled]).one()
    assert not result.success
    assert 'persistent handles cannot be enabled' in result.msg
    amodule._handler._orch = orch
    _apply(amodule, [smb.resources.RemovedCluster(cluster_id='foo')])
    assert not entry.exists()
    orch.remove_smb_service.assert_called_once_with('smb.foo')
    _apply(amodule, [cluster])
    assert entry.get()['orch_submitted'] is False
    _apply(amodule, [enabled])


def test_unknown_orchestration_history(amodule):
    cluster = _cluster()
    _apply(amodule, [cluster])
    entry = amodule._public_store['foo', 'cluster-info']
    info = dict(entry.get())
    del info['orch_submitted']
    entry.set(info)
    _apply(amodule, [cluster])
    assert entry.get()['orch_submitted'] is None
    result = amodule._apply_res(
        [_share(availability=ShareAvailability(persistent_handles=True))]
    ).one()
    assert not result.success
    assert 'history is unknown' in result.msg


def _results_by_id(result_group):
    found = {}
    for result in result_group:
        src = result.src
        rid = getattr(src, 'share_id', None) or src.cluster_id
        assert rid not in found, f'duplicate result for {rid}'
        found[rid] = result
    return found


def test_transition_error_blames_enablers(amodule):
    _apply(amodule, [_cluster(), _share('s0')])
    enabled = ShareAvailability(persistent_handles=True)
    rg = amodule._apply_res(
        [
            _cluster(share_availability=enabled),
            _share('s1'),
            _share('s2', enabled),
        ]
    )
    assert not rg.success
    found = _results_by_id(rg)
    assert set(found) == {'foo', 's1', 's2'}
    # Inheriting the default does not make s1 responsible.
    assert isinstance(found['s1'], smb.results.ResourceResult)
    assert found['s1'].success
    for rid in ('foo', 's2'):
        assert isinstance(found[rid], smb.results.ErrorResult)
        assert 'persistent handles cannot be enabled' in found[rid].msg


def test_default_reserves_global_support(amodule):
    cluster = _cluster(
        share_availability=ShareAvailability(persistent_handles=True)
    )
    assert smb.resources.requires_persistent_handles(cluster, [])
    _apply(amodule, [cluster])
    assert _globals(amodule)['persistent handles'] == 'yes'
    # Future shares can still opt in.
    _apply(
        amodule,
        [
            _share('inherited'),
            _share('later', ShareAvailability(persistent_handles=True)),
        ],
    )
    assert (
        _config(amodule)['shares']['inherited']['options'][
            'continuous availability'
        ]
        == 'yes'
    )
    stored = amodule._internal_store['shares', 'foo.inherited'].get()
    assert 'availability' not in stored


def _persistent_handle_warnings(result):
    return [
        warning
        for warning in result.get('warnings', [])
        if 'persistent handles' in warning.lower()
    ]


def _config(module, cluster_id='foo'):
    return module._public_store[cluster_id, 'config.smb'].get()


def _globals(module, cluster_id='foo'):
    return _config(module, cluster_id)['globals'][cluster_id]['options']


@pytest.mark.parametrize(
    'cluster_availability,share_availability',
    [
        (None, None),
        ({'persistent_handles': True}, None),
        ({'persistent_handles': True}, {'persistent_handles': False}),
        ({'persistent_handles': False}, {'persistent_handles': True}),
    ],
)
def test_availability_inheritance(
    amodule, cluster_availability, share_availability
):
    cluster_data = _cluster().to_simplified()
    share_data = _share().to_simplified()
    if cluster_availability is not None:
        cluster_data['share_defaults'] = {
            'availability': cluster_availability
        }
    if share_availability is not None:
        share_data['availability'] = share_availability
    result = _apply(amodule, smb.resources.load([share_data, cluster_data]))
    cluster_enabled = bool(
        (cluster_availability or {}).get('persistent_handles')
    )
    share_value = (share_availability or {}).get('persistent_handles')
    enabled = cluster_enabled if share_value is None else share_value
    results_by_type = {
        item['resource']['resource_type']: item
        for item in result.to_simplified()['results']
    }
    cluster_warnings = _persistent_handle_warnings(
        results_by_type['ceph.smb.cluster']
    )
    share_warnings = _persistent_handle_warnings(
        results_by_type['ceph.smb.share']
    )
    assert bool(cluster_warnings) == cluster_enabled
    assert bool(share_warnings) == enabled
    for warning in cluster_warnings + share_warnings:
        assert 'performance' in warning
    cfg = _config(amodule)
    global_opts = cfg['globals']['foo']['options']
    share_opts = cfg['shares']['s1']['options']
    assert global_opts.get('persistent handles', 'no') == (
        'yes' if cluster_enabled or enabled else 'no'
    )
    assert share_opts.get('continuous availability', 'no') == (
        'yes' if enabled else 'no'
    )
    # Do not persist inherited values on shares.
    stored = amodule._internal_store['shares', 'foo.s1'].get()
    stored_availability = stored.get('availability') or {}
    assert stored_availability.get('persistent_handles') == share_value


def test_availability_reconciliation(amodule):
    cluster = _cluster(
        share_availability=ShareAvailability(
            persistent_handles=False,
        ),
        durability=Durability.PARTIAL_OUTAGE,
    )
    enabled = ShareAvailability(persistent_handles=True)
    share1 = _share(availability=enabled)
    share2 = _share('s2', availability=enabled)
    _apply(
        amodule,
        [
            cluster,
            share1,
            share2,
            _cluster('other'),
            _share(cluster_id='other', availability=enabled),
        ],
    )
    opts = _globals(amodule)
    assert opts['persistent handles'] == 'yes'
    assert opts['persistent handles durability'] == 'partial_outage'

    # Keep support required by other stored shares.
    disabled = ShareAvailability(persistent_handles=False)
    _apply(amodule, [replace(share1, availability=disabled)])
    assert _globals(amodule)['persistent handles'] == 'yes'
    _apply(
        amodule, [smb.resources.RemovedShare(cluster_id='foo', share_id='s2')]
    )
    opts = _globals(amodule)
    assert 'persistent handles' not in opts
    assert 'persistent handles durability' not in opts
    assert _globals(amodule, 'other')['persistent handles'] == 'yes'

    # Removing the final requirement prevents re-enablement.
    result = amodule._apply_res([share1]).one()
    assert not result.success, result.to_simplified()
    assert 'persistent handles cannot be enabled' in result.msg
    stored_share = amodule._internal_store['shares', 'foo.s1'].get()
    assert stored_share['availability']['persistent_handles'] is False
    assert 'persistent handles' not in _globals(amodule)
    stored = amodule._internal_store['clusters', 'foo'].get()
    assert stored['persistent_handles_durability'] == 'partial_outage'
    assert 'availability' not in stored
    assert (
        stored['share_defaults']['availability']['persistent_handles']
        is False
    )
    _apply(amodule, [cluster])
    assert 'persistent handles durability' not in _globals(amodule)


@pytest.mark.parametrize(
    'old,new,enabled,allowed',
    [
        (None, Durability.FULL_OUTAGE, True, True),
        (Durability.PARTIAL_OUTAGE, Durability.FULL_OUTAGE, False, False),
    ],
)
def test_durability_immutable(amodule, old, new, allowed, enabled):
    cluster = _cluster(
        share_availability=ShareAvailability(persistent_handles=enabled),
        durability=old,
    )
    _apply(amodule, [cluster, _share()])
    result = amodule._apply_res(
        [
            replace(
                cluster,
                persistent_handles_durability=new,
            )
        ]
    )
    assert result.success == allowed, result.to_simplified()
    if not allowed:
        assert 'persistent_handles_durability value may not be changed' in (
            result.to_simplified()['results'][0]['msg']
        )
        assert (
            amodule._internal_store['clusters', 'foo'].get()
            == cluster.to_simplified()
        )


def test_availability_invalid_boolean():
    data = _cluster().to_simplified()
    data['share_defaults'] = {'availability': {'persistent_handles': 'false'}}
    with pytest.raises(ValueError, match='must be a boolean'):
        smb.resources.load(data)


def test_rgw_rejects_availability_mapping():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='rgw',
        rgw=smb.resources.RGWStorage(
            bucket='bucket', credential_ref='credential'
        ),
    )
    data = share.to_simplified()
    data['availability'] = {'persistent_handles': True}
    with pytest.raises(
        ValueError, match='availability is not supported for RGW shares'
    ):
        smb.resources.load(data)

from dataclasses import replace

import pytest

import smb
from smb.resources import ShareAvailability


def _cluster(cluster_id='foo', share_availability=None):
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
    return smb.module.Module(
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


def _apply(module, resources):
    result = module._apply_res(resources)
    assert result.success, result.to_simplified()
    return result


@pytest.mark.parametrize('opt_out', [False, True])
def test_default_reserves_global_support(amodule, opt_out):
    cluster = _cluster(
        share_availability=ShareAvailability(persistent_handles=True)
    )
    shares = (
        [_share(availability=ShareAvailability(persistent_handles=False))]
        if opt_out
        else []
    )
    assert smb.resources.requires_persistent_handles(cluster, shares)
    _apply(amodule, [cluster, *shares])
    assert _globals(amodule)['persistent handles'] == 'yes'
    if opt_out:
        assert (
            _config(amodule)['shares']['s1']['options'][
                'continuous availability'
            ]
            == 'no'
        )
    # Reserved support lets future shares opt in without late enablement.
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
        ({'persistent_handles': False}, None),
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
    _apply(amodule, smb.resources.load([share_data, cluster_data]))
    cluster_enabled = bool(
        (cluster_availability or {}).get('persistent_handles')
    )
    share_value = (share_availability or {}).get('persistent_handles')
    enabled = cluster_enabled if share_value is None else share_value
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
        )
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
    # Other clusters must not keep foo enabled.
    assert _globals(amodule, 'other')['persistent handles'] == 'yes'

    # Removing the final requirement prevents re-enablement.
    result = amodule._apply_res([share1]).one()
    assert not result.success, result.to_simplified()
    assert 'persistent handles cannot be enabled' in result.msg
    stored_share = amodule._internal_store['shares', 'foo.s1'].get()
    assert stored_share['availability']['persistent_handles'] is False
    assert 'persistent handles' not in _globals(amodule)


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

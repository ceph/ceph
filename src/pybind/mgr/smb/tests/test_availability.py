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
    # Reserved support lets future shares opt in without late enablement.
    _apply(
        amodule,
        [_share('later', ShareAvailability(persistent_handles=True))],
    )


def test_availability_reconciliation(amodule):
    enabled = ShareAvailability(persistent_handles=True)
    share1 = _share(availability=enabled)
    share2 = _share('s2', availability=enabled)
    _apply(amodule, [_cluster(), share1, share2])
    _apply(
        amodule,
        [
            replace(
                share1,
                availability=ShareAvailability(persistent_handles=False),
            )
        ],
    )
    _apply(
        amodule, [smb.resources.RemovedShare(cluster_id='foo', share_id='s2')]
    )
    result = amodule._apply_res([share1]).one()
    assert not result.success
    assert 'persistent handles cannot be enabled' in result.msg
    stored = amodule._internal_store['shares', 'foo.s1'].get()
    assert stored['availability']['persistent_handles'] is False


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

import pytest

import smb.enums
import smb.resources
import smb.results


def test_resource_result_std():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult(share, success=True)
    assert rr.mgr_return_value() == 0
    assert rr.mgr_status_value() == ''
    dump = rr.to_simplified()
    assert set(dump) == {'success', 'resource'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'


def test_resource_result_msg():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult(share, success=True, msg='Robble robble')
    assert rr.mgr_return_value() == 0
    assert rr.mgr_status_value() == ''
    dump = rr.to_simplified()
    assert set(dump) == {'success', 'resource', 'msg'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'
    assert dump['msg'] == 'Robble robble'


def test_resource_result_processed():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult.processed(share, smb.enums.State.PRESENT)
    assert rr.mgr_return_value() == 0
    assert rr.mgr_status_value() == ''
    dump = rr.to_simplified()
    assert set(dump) == {'success', 'resource', 'state'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'
    assert dump['state'] == 'present'


def test_resource_result_checked():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult.checked(share)
    assert rr.mgr_return_value() == 0
    assert rr.mgr_status_value() == ''
    dump = rr.to_simplified()
    assert set(dump) == {'success', 'resource', 'checked'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'
    assert dump['checked'] is True


def test_resoruce_result_bad_keys():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    with pytest.raises(Exception, match='bloop, flurp'):
        smb.results.ResourceResult(
            share, success=True, status={'bloop': 'frungy', 'flurp': 'womble'}
        )


def test_resoruce_result_replace():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='foo',
        share_id='bar2',
        name='Foo Bar 2',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult(
        share,
        success=True,
        msg='Swapme',
        status={'state': smb.enums.State.UPDATED},
    )
    rr2 = rr.replace_resource(share2)

    assert rr2.mgr_return_value() == 0
    assert rr2.mgr_status_value() == ''
    dump = rr2.to_simplified()
    assert set(dump) == {'success', 'resource', 'msg', 'state'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar2'
    assert dump['msg'] == 'Swapme'
    assert dump['state'] == 'updated'


def test_error_result_std():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    er = smb.results.ErrorResult(share, msg='Whoops share fell down')
    assert er.mgr_return_value() == -11
    assert 'for details' in er.mgr_status_value()
    dump = er.to_simplified()
    assert set(dump) == {'success', 'resource', 'msg'}
    assert dump['success'] is False
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'
    assert dump['msg'] == 'Whoops share fell down'


def test_error_result_bad_keys():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    with pytest.raises(Exception, match='rumpled'):
        smb.results.ErrorResult(share, status={'rumpled': 'very'})


def test_error_result_good_keys():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    er = smb.results.ErrorResult(
        share,
        msg='Scruffled by notha clusta',
        status={'cluster_id': 'foo', 'other_cluster_id': 'oof'},
    )
    assert er.mgr_return_value() == -11
    assert 'for details' in er.mgr_status_value()
    dump = er.to_simplified()
    assert set(dump) == {
        'success',
        'resource',
        'msg',
        'cluster_id',
        'other_cluster_id',
    }
    assert dump['success'] is False
    assert dump['resource'].get('resource_type') == 'ceph.smb.share'
    assert dump['resource'].get('cluster_id') == 'foo'
    assert dump['resource'].get('share_id') == 'bar'
    assert dump['msg'] == 'Scruffled by notha clusta'


def test_invalid_resource_result_std():
    irr = smb.results.InvalidResourceResult(
        resource_data={
            'resource_type': 'ceph.smb.gnopf',
            'gnopf_id': 'abc',
            'power_level': '12',
        },
        msg='That aint a real thing',
    )
    assert irr.mgr_return_value() == -11
    assert 'for details' in irr.mgr_status_value()
    dump = irr.to_simplified()
    assert dump['success'] is False
    assert dump['resource'].get('resource_type') == 'ceph.smb.gnopf'
    assert dump['resource'].get('gnopf_id') == 'abc'
    assert dump['msg'] == 'That aint a real thing'


def test_result_group_one():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    rr = smb.results.ResourceResult(share, success=True)

    rg = smb.results.ResultGroup(initial_results=[rr])
    assert rg.mgr_return_value() == 0
    assert rg.mgr_status_value() == ''
    dump = rg.to_simplified()
    assert set(dump) == {'success', 'results'}
    assert dump['success'] is True
    assert len(dump['results']) == 1
    r0 = dump['results'][0]
    assert set(r0) == {'success', 'resource'}
    assert r0['success'] is True
    assert r0['resource'].get('resource_type') == 'ceph.smb.share'
    assert r0['resource'].get('cluster_id') == 'foo'
    assert r0['resource'].get('share_id') == 'bar'

    # assert the one function returns a ResourceResult with the orig share
    rr1 = rg.one()
    assert isinstance(rr1, smb.results.ResourceResult)
    assert rr1.src is share


def test_result_group_two():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='foo',
        share_id='bar2',
        name='Foo Bar 2',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )

    rr = smb.results.ResourceResult(share, success=True)

    rg = smb.results.ResultGroup(initial_results=[rr])
    rg.append(
        smb.results.ResourceResult.processed(share2, smb.enums.State.UPDATED)
    )

    assert rg.mgr_return_value() == 0
    assert rg.mgr_status_value() == ''
    dump = rg.to_simplified()
    assert set(dump) == {'success', 'results'}
    assert dump['success'] is True
    assert len(dump['results']) == 2

    r0 = dump['results'][0]
    assert set(r0) == {'success', 'resource'}
    assert r0['success'] is True
    assert r0['resource'].get('resource_type') == 'ceph.smb.share'
    assert r0['resource'].get('cluster_id') == 'foo'
    assert r0['resource'].get('share_id') == 'bar'

    r1 = dump['results'][1]
    assert set(r1) == {'success', 'resource', 'state'}
    assert r1['success'] is True
    assert r1['state'] == 'updated'
    assert r1['resource'].get('resource_type') == 'ceph.smb.share'
    assert r1['resource'].get('cluster_id') == 'foo'
    assert r1['resource'].get('share_id') == 'bar2'

    # assert the one function raises on a RG with >1 result
    with pytest.raises(ValueError):
        rg.one()


def test_result_group_two_bad():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='foo',
        share_id='bar2',
        name='Foo Bar 2',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )

    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ErrorResult(share, msg='Wonky circut'),
            smb.results.ErrorResult(share2, msg='Damaged doodad'),
        ]
    )

    assert rg.mgr_return_value() == -11
    assert '2 resources' in rg.mgr_status_value()
    dump = rg.to_simplified()
    assert set(dump) == {'success', 'results'}
    assert dump['success'] is False
    assert len(dump['results']) == 2


def test_result_group_iter_basic():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='foo',
        share_id='bar2',
        name='Foo Bar 2',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )

    rr = smb.results.ResourceResult(share, success=True)

    rg = smb.results.ResultGroup(initial_results=[rr])
    rg.append(
        smb.results.ResourceResult.processed(share2, smb.enums.State.UPDATED)
    )

    # direct iter
    lst = list(rg)
    assert len(lst) == 2

    # resource iter
    lst2 = list(rg.resources())
    assert len(lst2) == 2

    # resource iter (no fail on non-rr)
    lst2 = list(rg.resources(check=False))
    assert len(lst2) == 2


def test_result_group_iter_mixed():
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )

    rr = smb.results.ResourceResult(share, success=True)

    rg = smb.results.ResultGroup(initial_results=[rr])
    rg.append(
        smb.results.InvalidResourceResult(
            resource_data={
                'resource_type': 'ceph.smb.gnopf',
                'gnopf_id': 'abc',
                'power_level': '12',
            },
            msg='That aint a real thing',
        )
    )

    # direct iter
    lst = list(rg)
    assert len(lst) == 2

    # resource iter
    with pytest.raises(ValueError):
        list(rg.resources())

    # resource iter (no fail on non-rr)
    lst2 = list(rg.resources(check=False))
    assert len(lst2) == 1
    assert lst2[0].src is share


def test_result_group_squash():
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
        linked_to_cluster='c1',
    )
    cluster = smb.resources.Cluster(
        cluster_id='c1',
        auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_settings=smb.resources.DomainSettings(
            realm='MYDOMAIN.EXAMPLE.ORG',
            join_sources=[
                smb.resources.JoinSource(
                    source_type=smb.enums.JoinSourceType.RESOURCE,
                    ref='join1',
                ),
            ],
        ),
        custom_dns=['192.168.76.204'],
    )

    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(ja, smb.enums.State.CREATED),
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.CREATED
            ),
        ]
    )

    squashed = rg.squash(cluster)
    assert squashed.mgr_return_value() == 0
    assert squashed.mgr_status_value() == ''
    dump = squashed.to_simplified()
    assert set(dump) == {'success', 'resource', 'additional_results', 'state'}
    assert dump['success'] is True
    assert dump['resource'].get('resource_type') == 'ceph.smb.cluster'
    assert dump['resource'].get('cluster_id') == 'c1'
    assert dump['state'] == 'created'
    assert len(dump['additional_results']) == 1
    assert dump['additional_results'][0].get('success') is True
    assert (
        dump['additional_results'][0].get('resource').get('resource_type')
        == 'ceph.smb.join.auth'
    )


def test_result_group_squash_fail():
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
        linked_to_cluster='c1',
    )
    cluster = smb.resources.Cluster(
        cluster_id='c1',
        auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_settings=smb.resources.DomainSettings(
            realm='MYDOMAIN.EXAMPLE.ORG',
            join_sources=[
                smb.resources.JoinSource(
                    source_type=smb.enums.JoinSourceType.RESOURCE,
                    ref='join1',
                ),
            ],
        ),
        custom_dns=['192.168.76.204'],
    )

    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(ja, smb.enums.State.CREATED),
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.CREATED
            ),
        ]
    )

    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    with pytest.raises(ValueError):
        rg.squash(share)


def test_result_group_convert():
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
        linked_to_cluster='c1',
    )
    cluster = smb.resources.Cluster(
        cluster_id='c1',
        auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_settings=smb.resources.DomainSettings(
            realm='MYDOMAIN.EXAMPLE.ORG',
            join_sources=[
                smb.resources.JoinSource(
                    source_type=smb.enums.JoinSourceType.RESOURCE,
                    ref='join1',
                ),
            ],
        ),
        custom_dns=['192.168.76.204'],
    )

    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(ja, smb.enums.State.CREATED),
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.CREATED
            ),
        ]
    )

    rg2 = rg.convert_results(
        (smb.enums.PasswordFilter.NONE, smb.enums.PasswordFilter.BASE64)
    )
    assert rg.mgr_return_value() == 0
    assert rg.mgr_status_value() == ''
    dump = rg.to_simplified()
    assert set(dump) == {'success', 'results'}
    assert dump['success'] is True
    assert len(dump['results']) == 2

    ja_res = [
        rr for rr in rg2 if getattr(rr.src, 'auth_id', None) == ja.auth_id
    ][0]
    assert ja_res.src.auth.password != 'Passw0rd'


def test_result_group_convert_mixed_content():
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
        linked_to_cluster='c1',
    )
    cluster = smb.resources.Cluster(
        cluster_id='c1',
        auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_settings=smb.resources.DomainSettings(
            realm='MYDOMAIN.EXAMPLE.ORG',
            join_sources=[
                smb.resources.JoinSource(
                    source_type=smb.enums.JoinSourceType.RESOURCE,
                    ref='join1',
                ),
            ],
        ),
        custom_dns=['192.168.76.204'],
    )

    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(ja, smb.enums.State.CREATED),
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.CREATED
            ),
            smb.results.InvalidResourceResult(
                resource_data={
                    'resource_type': 'ceph.smb.gnopf',
                    'gnopf_id': 'abc',
                    'power_level': '12',
                },
                msg='That aint a real thing',
            ),
        ]
    )

    rg2 = rg.convert_results(
        (smb.enums.PasswordFilter.NONE, smb.enums.PasswordFilter.BASE64)
    )
    assert rg.mgr_return_value() == -11
    assert '1 resource' in rg.mgr_status_value()
    dump = rg.to_simplified()
    assert set(dump) == {'success', 'results'}
    assert dump['success'] is False
    assert len(dump['results']) == 3

    ja_res = [
        rr
        for rr in rg2
        if getattr(getattr(rr, 'src', None), 'auth_id', None) == ja.auth_id
    ][0]
    assert ja_res.src.auth.password != 'Passw0rd'


@pytest.fixture()
def cs2():
    # quick and dirty fixture to avoid copy paste for summary tests
    cluster = smb.resources.Cluster(
        cluster_id='foo',
        auth_mode=smb.enums.AuthMode.ACTIVE_DIRECTORY,
        domain_settings=smb.resources.DomainSettings(
            realm='MYDOMAIN.EXAMPLE.ORG',
            join_sources=[
                smb.resources.JoinSource(
                    source_type=smb.enums.JoinSourceType.RESOURCE,
                    ref='join1',
                ),
            ],
        ),
        custom_dns=['192.168.76.204'],
    )
    share = smb.resources.Share(
        cluster_id='foo',
        share_id='bar',
        name='Foo Bar',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    share2 = smb.resources.Share(
        cluster_id='foo',
        share_id='bar2',
        name='Foo Bar 2',
        cephfs=smb.resources.CephFSStorage(
            volume='myvol',
            path='/',
        ),
    )
    return cluster, share, share2


def test_cluster_share_summary(cs2):
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share2, smb.enums.State.UPDATED
            ),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    assert len(summary.successful_clusters) == 1
    assert len(summary.failed_clusters) == 0
    assert len(summary.successful_shares) == 2
    assert len(summary.failed_shares) == 0

    dct = summary.build_dict(
        successful_shares_key='good_shares',
        failed_shares_key='bad_shares',
        cluster_updated_key='cluster_updated',
    )
    assert set(dct) == {'good_shares', 'bad_shares', 'cluster_updated'}
    assert len(dct['good_shares']) == 2
    assert len(dct['bad_shares']) == 0
    assert dct['cluster_updated'] is True


def test_cluster_share_summary_failed(cs2):
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ErrorResult(share2, msg='Blip'),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    assert len(summary.successful_clusters) == 1
    assert len(summary.failed_clusters) == 0
    assert len(summary.successful_shares) == 1
    assert len(summary.failed_shares) == 1

    dct = summary.build_dict(
        successful_shares_key='good_shares',
        failed_shares_key='bad_shares',
        cluster_updated_key='cluster_updated',
    )
    assert set(dct) == {'good_shares', 'bad_shares', 'cluster_updated'}
    assert len(dct['good_shares']) == 1
    assert len(dct['bad_shares']) == 1
    assert dct['cluster_updated'] is True


def test_cluster_share_summary_failed2(cs2):
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ErrorResult(cluster, msg='Blat'),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ErrorResult(share2, msg='Blip'),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    assert len(summary.successful_clusters) == 0
    assert len(summary.failed_clusters) == 1
    assert len(summary.successful_shares) == 1
    assert len(summary.failed_shares) == 1

    with pytest.raises(ValueError):
        summary.build_dict(
            successful_shares_key='shares',
            failed_shares_key='failed_shares',
        )


def test_cluster_share_summary_invalid(cs2):
    cluster, share, _ = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.InvalidResourceResult(
                resource_data={
                    'resource_type': 'ceph.smb.gnopf',
                    'gnopf_id': 'abc',
                    'power_level': '12',
                },
                msg='That aint a real thing',
            ),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    assert len(summary.successful_clusters) == 1
    assert len(summary.failed_clusters) == 0
    assert len(summary.successful_shares) == 1
    assert len(summary.failed_shares) == 0

    dct = summary.build_dict(
        successful_shares_key='shares',
        failed_shares_key='failed_shares',
    )
    assert set(dct) == {'shares', 'failed_shares'}
    assert len(dct['shares']) == 1
    assert len(dct['failed_shares']) == 0


def test_cluster_share_summary_other(cs2):
    ja = smb.resources.JoinAuth(
        auth_id='join1',
        auth=smb.resources.JoinAuthValues(
            username='testadmin',
            password='Passw0rd',
        ),
        linked_to_cluster='c1',
    )
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(ja, smb.enums.State.UPDATED),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share2, smb.enums.State.UPDATED
            ),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    assert len(summary.successful_clusters) == 1
    assert len(summary.failed_clusters) == 0
    assert len(summary.successful_shares) == 2
    assert len(summary.failed_shares) == 0

    dct = summary.build_dict(
        successful_shares_key='shares',
        failed_shares_key='failed_shares',
    )
    assert set(dct) == {'shares', 'failed_shares'}
    assert len(dct['shares']) == 2
    assert len(dct['failed_shares']) == 0


def test_client_compat_batch_result(cs2):
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share2, smb.enums.State.UPDATED
            ),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    dct = summary.build_dict(
        successful_shares_key='successful_share_updates',
        failed_shares_key='failed_share_updates',
        cluster_updated_key='cluster_updated',
    )
    ccbr = smb.results.ClientCompatBatchResult.create(
        dct
        | dict(
            cluster_id=cluster.cluster_id,
            client_compat='fred',
            total_shares=3,
        )
    )

    dump = ccbr.to_simplified()
    assert dump['success'] is True
    assert dump['successful_share_updates'] == ['bar', 'bar2']
    assert dump['failed_share_updates'] == []
    assert dump['cluster_updated'] is True


def test_client_compat_batch_result_bad_key():
    with pytest.raises(KeyError):
        smb.results.ClientCompatBatchResult.create(
            dict(
                cluster_id='yuzpink',
                client_compat='fred',
                total_shares=3,
            )
        )


def test_qos_batch_result(cs2):
    cluster, share, share2 = cs2
    rg = smb.results.ResultGroup(
        initial_results=[
            smb.results.ResourceResult.processed(
                cluster, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share, smb.enums.State.UPDATED
            ),
            smb.results.ResourceResult.processed(
                share2, smb.enums.State.UPDATED
            ),
        ]
    )

    summary = smb.results.ClusterShareSummary.from_result_group(rg)
    dct = summary.build_dict(
        successful_shares_key='successful_updates',
        failed_shares_key='failed_updates',
    )
    qbr = smb.results.QoSBatchResult.create(
        dct
        | dict(
            cluster_id=cluster.cluster_id,
            total_shares=3,
            unchanged_shares=['nop1', 'nop2'],
        )
    )

    assert qbr.success is True
    assert qbr.mgr_return_value() == 0
    assert qbr.mgr_status_value() == ''
    dump = qbr.to_simplified()
    assert dump['success'] is True
    assert dump['successful_updates'] == ['bar', 'bar2']
    assert dump['failed_updates'] == []


def test_qos_batch_result_bad_key():
    with pytest.raises(KeyError):
        smb.results.QoSBatchResult.create(
            dict(
                cluster_id='yuzpink',
                total_shares=3,
            )
        )


def test_qos_batch_result_unchanged():
    qbr = smb.results.QoSBatchResult.unchanged(
        dict(
            cluster_id='wibble',
            message='Nothing changed',
            total_shares=3,
            unchanged_shares=['nop1', 'nop2'],
        )
    )

    assert qbr.success is True
    assert qbr.mgr_return_value() == 0
    assert qbr.mgr_status_value() == ''
    dump = qbr.to_simplified()
    assert dump['success'] is True
    assert dump['message'] == 'Nothing changed'


def test_qos_batch_result_unchanged_bad_key():
    with pytest.raises(KeyError):
        smb.results.QoSBatchResult.unchanged(
            dict(
                cluster_id='yuzpink',
                total_shares=3,
            )
        )


def test_qos_batch_result_unhandled():
    qbr = smb.results.QoSBatchResult.unhandled_error(
        'Something strange in the neighborhood'
    )

    assert qbr.success is False
    assert qbr.mgr_return_value() == -11
    assert 'Something strange' in qbr.mgr_status_value()
    dump = qbr.to_simplified()
    assert dump['success'] is False
    assert dump['msg'] == 'Something strange in the neighborhood'

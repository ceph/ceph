import contextlib
import time

import pytest

import cephutil
import smbutil


@pytest.fixture
def subvols(smb_cfg):
    test_vols = [
        {
            'fs': 'cephfs',
            'group_name': 'smb',
            'name': 'tcs0',
            'casesensitive': 0,
        },
        {
            'fs': 'cephfs',
            'group_name': 'smb',
            'name': 'tcs1',
            'casesensitive': 1,
        },
    ]

    for tv in test_vols:
        cephutil.cephadm_shell_cmd(
            smb_cfg,
            [
                'ceph',
                'fs',
                'subvolume',
                'create',
                tv['fs'],
                tv['name'],
                f'--group-name={tv["group_name"]}',
                f'--casesensitive={tv["casesensitive"]}',
            ],
            check=True,
        )
    time.sleep(15)
    yield test_vols
    for tv in test_vols:
        cephutil.cephadm_shell_cmd(
            smb_cfg,
            [
                'ceph',
                'fs',
                'subvolume',
                'rm',
                tv['fs'],
                tv['name'],
                f'--group-name={tv["group_name"]}',
            ],
            check=True,
        )


def _shares(smb_cfg, subvols, case_insensitive):
    fs_case_params = smb_cfg.params.get('fs_case') or {}
    cluster_id = fs_case_params['cluster_id']
    share_prefix = fs_case_params.get('share_prefix', 'fs-case-')

    tv_case_i, tv_case_s = subvols
    assert tv_case_i['casesensitive'] == 0
    assert tv_case_s['casesensitive'] == 1

    new_share1 = {
        "resource_type": "ceph.smb.share",
        "cluster_id": cluster_id,
        "share_id": f'{share_prefix}1',
        "cephfs": {
            "volume": tv_case_i['fs'],
            "subvolumegroup": tv_case_i['group_name'],
            "subvolume": tv_case_i["name"],
            "path": "/",
            "case_insensitive": case_insensitive,
        },
    }
    new_share2 = {
        "resource_type": "ceph.smb.share",
        "cluster_id": cluster_id,
        "share_id": f'{share_prefix}2',
        "cephfs": {
            "volume": tv_case_s['fs'],
            "subvolumegroup": tv_case_s['group_name'],
            "subvolume": tv_case_s["name"],
            "path": "/",
            "case_insensitive": case_insensitive,
        },
    }
    return [new_share1, new_share2]


@contextlib.contextmanager
def _applied(smb_cfg, shares):
    try:
        yield smbutil.apply_resources(smb_cfg, shares)
    finally:
        remove = [
            {
                "resource_type": "ceph.smb.share",
                "cluster_id": s['cluster_id'],
                "share_id": s['share_id'],
                "intent": "removed",
            }
            for s in shares
        ]
        smbutil.apply_resources(smb_cfg, remove)


@pytest.mark.fs_case
def test_fs_case_warnings(smb_cfg, subvols):
    shares = _shares(smb_cfg, subvols, "warn")
    with _applied(smb_cfg, shares) as jres:
        assert jres["success"]
        assert len(jres["results"]) == 2
        # ensure results are paired with the original subvolumes
        result1 = result2 = None
        for result in jres['results']:
            subname = result['resource']['cephfs']['subvolume']
            if subname == subvols[0]['name']:
                result1 = result
            elif subname == subvols[1]['name']:
                result2 = result
            else:
                raise ValueError('should not happen')
        assert result1
        assert result2

        # compare results - the subvolume that is case insensitive should have
        # no warnings. The subvolume that is case sensitive should have a
        # warning
        assert 'warnings' not in result1
        assert 'warnings' in result2
        assert len(result2['warnings']) == 1
        assert 'case insensitive' in result2['warnings'][0]

        assert 'warnings_summary' in jres
        assert 'count' in jres['warnings_summary']
        assert jres['warnings_summary']['count'] == 1
        assert 'case insensitive' in jres['warnings_summary']['recap'][0]


@pytest.mark.fs_case
def test_fs_case_ignore(smb_cfg, subvols):
    shares = _shares(smb_cfg, subvols, "ignore")
    with _applied(smb_cfg, shares) as jres:
        assert jres["success"]
        assert len(jres["results"]) == 2
        # ensure results are paired with the original subvolumes
        result1 = result2 = None
        for result in jres['results']:
            subname = result['resource']['cephfs']['subvolume']
            if subname == subvols[0]['name']:
                result1 = result
            elif subname == subvols[1]['name']:
                result2 = result
            else:
                raise ValueError('should not happen')
        assert result1
        assert result2

        # no warnings anywhere
        assert 'warnings' not in result1
        assert 'warnings' not in result2
        assert 'warnings_summary' not in jres


@pytest.mark.fs_case
def test_fs_case_require(smb_cfg, subvols):
    shares = _shares(smb_cfg, subvols, "require")
    cmd_res = smbutil.apply_resources_unchecked(smb_cfg, shares)
    assert cmd_res.returncode != 0
    jres = cmd_res.obj
    assert not jres["success"]
    assert len(jres["results"]) == 2
    # ensure results are paired with the original subvolumes
    result1 = result2 = None
    for result in jres['results']:
        subname = result['resource']['cephfs']['subvolume']
        if subname == subvols[0]['name']:
            result1 = result
        elif subname == subvols[1]['name']:
            result2 = result
        else:
            raise ValueError('should not happen')
    assert result1
    assert result2

    # no warnings anywhere
    assert 'warnings' not in result1
    assert 'warnings' not in result2
    assert 'warnings_summary' not in jres

    # but an error
    assert 'msg' not in result1
    assert result1['success']
    assert 'msg' in result2
    assert not result2['success']
    assert 'case insensitive' in result2['msg']

from unittest import mock
import json

import pytest

from tests.fixtures import with_cephadm_ctx, cephadm_fs, import_cephadm

_cephadm = import_cephadm()


SAMPLE_UUID = "2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae"


def good_nfs_json():
    return nfs_json(
        pool=True,
        files=True,
    )


def nfs_json(**kwargs):
    result = {}
    if kwargs.get("pool"):
        result["pool"] = "party"
    if kwargs.get("files"):
        result["files"] = {
            "ganesha.conf": "",
            "idmap.conf": "",
        }
    if kwargs.get("rgw_content"):
        result["rgw"] = dict(kwargs["rgw_content"])
    elif kwargs.get("rgw"):
        result["rgw"] = {
            "keyring": "foobar",
            "user": "jsmith",
        }
    return result


@pytest.mark.parametrize(
    "args,kwargs",
    # args: <fsid>, <daemon_id>, <config_json>; kwargs: <image>
    [
        # fail due to: invalid fsid
        (["foobar", "fred", good_nfs_json()], {}),
        # fail due to: invalid daemon_id
        ([SAMPLE_UUID, "", good_nfs_json()], {}),
        # fail due to: invalid image
        (
            [SAMPLE_UUID, "fred", good_nfs_json()],
            {"image": ""},
        ),
        # fail due to: no files in config_json
        (
            [
                SAMPLE_UUID,
                "fred",
                nfs_json(pool=True),
            ],
            {},
        ),
        # fail due to: no pool in config_json
        (
            [
                SAMPLE_UUID,
                "fred",
                nfs_json(files=True),
            ],
            {},
        ),
        # fail due to: bad rgw content
        (
            [
                SAMPLE_UUID,
                "fred",
                nfs_json(pool=True, files=True, rgw_content={"foo": True}),
            ],
            {},
        ),
        # fail due to: rgw keyring given but no user
        (
            [
                SAMPLE_UUID,
                "fred",
                nfs_json(
                    pool=True, files=True, rgw_content={"keyring": "foo"}
                ),
            ],
            {},
        ),
    ],
)
def test_nfsganesha_validation_errors(args, kwargs):
    with pytest.raises(_cephadm.Error):
        with with_cephadm_ctx([]) as ctx:
            _cephadm.NFSGanesha(ctx, *args, **kwargs)


def test_nfsganesha_init():
    with with_cephadm_ctx([]) as ctx:
        ctx.config_json = json.dumps(good_nfs_json())
        ctx.image = "test_image"
        nfsg = _cephadm.NFSGanesha.init(
            ctx,
            SAMPLE_UUID,
            "fred",
        )
    assert nfsg.fsid == SAMPLE_UUID
    assert nfsg.daemon_id == "fred"
    assert nfsg.pool == "party"


def test_nfsganesha_container_mounts():
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        cmounts = nfsg._get_container_mounts("/var/tmp")
        assert len(cmounts) == 4
        assert cmounts["/var/tmp/config"] == "/etc/ceph/ceph.conf:z"
        assert cmounts["/var/tmp/keyring"] == "/etc/ceph/keyring:z"
        assert cmounts["/var/tmp/etc/ganesha"] == "/etc/ganesha:z"
        assert (
            cmounts["/var/tmp/ganesha-entrypoint.sh"]
            == "/usr/local/scripts/ganesha-entrypoint.sh"
        )

    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            nfs_json(pool=True, files=True, rgw=True),
        )
        cmounts = nfsg._get_container_mounts("/var/tmp")
        assert len(cmounts) == 5
        assert cmounts["/var/tmp/config"] == "/etc/ceph/ceph.conf:z"
        assert cmounts["/var/tmp/keyring"] == "/etc/ceph/keyring:z"
        assert cmounts["/var/tmp/etc/ganesha"] == "/etc/ganesha:z"
        assert (
            cmounts["/var/tmp/keyring.rgw"]
            == "/var/lib/ceph/radosgw/ceph-jsmith/keyring:z"
        )


def test_nfsganesha_container_envs():
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        envs = nfsg.get_container_envs()
        assert len(envs) == 1
        assert envs[0] == "CEPH_CONF=/etc/ceph/ceph.conf"


def test_nfsganesha_get_version():
    from cephadmlib.daemons import nfs

    with with_cephadm_ctx([]) as ctx:
        nfsg = nfs.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )

        with mock.patch("cephadmlib.daemons.nfs.call") as _call:
            _call.return_value = ("NFS-Ganesha Release = V100", "", 0)
            ver = nfsg.get_version(ctx, "fake_version")
            _call.assert_called()
        assert ver == "100"


def test_nfsganesha_get_daemon_name():
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        assert nfsg.get_daemon_name() == "nfs.fred"


def test_nfsganesha_get_container_name():
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        name1 = nfsg.get_container_name()
        assert name1 == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-nfs.fred"
        name2 = nfsg.get_container_name(desc="extra")
        assert (
            name2 == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-nfs.fred-extra"
        )


def test_nfsganesha_get_daemon_args():
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        args = nfsg.get_daemon_args()
        assert args == ["-F", "-L", "STDERR"]


@pytest.mark.parametrize(
    'conf,expected',
    [
        ('Protocols = 3, 4;', True),
        ('Protocols = 4, 3;', True),
        ('Protocols = 4;', False),
        ('Protocols = 4, nfsrdma, rpcrdma;', False),
        ('', True),
    ],
)
def test_nfsv3_enabled_in_ganesha_conf(conf, expected):
    assert _cephadm.NFSGanesha.nfsv3_enabled_in_ganesha_conf(conf) is expected


def test_nfsganesha_entrypoint_script_nfsv3():
    script = _cephadm.NFSGanesha.ganesha_entrypoint_script(nfsv3=True)
    assert 'rpcbind' in script
    assert 'exec /usr/bin/ganesha.nfsd "$@"' in script


def test_nfsganesha_entrypoint_script_v4_only():
    script = _cephadm.NFSGanesha.ganesha_entrypoint_script(nfsv3=False)
    assert 'rpcbind' not in script
    assert 'exec /usr/bin/ganesha.nfsd "$@"' in script


def test_nfsganesha_entrypoint_script_from_conf():
    conf_v3 = '\n'.join(
        [
            'NFS_CORE_PARAM {',
            '        Protocols = 3, 4;',
            '}',
        ]
    )
    script = _cephadm.NFSGanesha.ganesha_entrypoint_script(
        nfsv3=_cephadm.NFSGanesha.nfsv3_enabled_in_ganesha_conf(conf_v3),
    )
    assert 'rpcbind' in script

    conf_v4 = 'NFS_CORE_PARAM { Protocols = 4; }'
    script = _cephadm.NFSGanesha.ganesha_entrypoint_script(
        nfsv3=_cephadm.NFSGanesha.nfsv3_enabled_in_ganesha_conf(conf_v4),
    )
    assert 'rpcbind' not in script


@mock.patch("cephadm.logger")
def test_nfsganesha_create_daemon_dirs(_logger, cephadm_fs):
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            good_nfs_json(),
        )
        with pytest.raises(OSError):
            nfsg.create_daemon_dirs("/var/tmp", 45, 54)
        cephadm_fs.create_dir("/var/tmp")
        nfsg.create_daemon_dirs("/var/tmp", 45, 54)
        with open("/var/tmp/ganesha-entrypoint.sh") as f:
            assert 'rpcbind' in f.read()

        nfsg_v4 = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            {
                'pool': 'party',
                'files': {
                    'ganesha.conf': 'NFS_CORE_PARAM { Protocols = 4; }',
                    'idmap.conf': '',
                },
            },
        )
        nfsg_v4.create_daemon_dirs("/var/tmp", 45, 54)
        with open("/var/tmp/ganesha-entrypoint.sh") as f:
            assert 'rpcbind' not in f.read()


@mock.patch("cephadm.logger")
def test_nfsganesha_create_daemon_dirs_rgw(_logger, cephadm_fs):
    with with_cephadm_ctx([]) as ctx:
        nfsg = _cephadm.NFSGanesha(
            ctx,
            SAMPLE_UUID,
            "fred",
            nfs_json(pool=True, files=True, rgw=True),
        )
        cephadm_fs.create_dir("/var/tmp")
        nfsg.create_daemon_dirs("/var/tmp", 45, 54)
        # TODO: make assertions about the dirs created

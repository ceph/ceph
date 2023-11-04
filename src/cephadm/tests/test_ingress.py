from unittest import mock
import json

import pytest

from tests.fixtures import with_cephadm_ctx, cephadm_fs, import_cephadm

_cephadm = import_cephadm()

SAMPLE_UUID = "2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae"
SAMPLE_HAPROXY_IMAGE = "registry.example.net/haproxy/haproxy:latest"
SAMPLE_KEEPALIVED_IMAGE = "registry.example.net/keepalive/keepalived:latest"


def good_haproxy_json():
    return haproxy_json(files=True)


def haproxy_json(**kwargs):
    if kwargs.get("files"):
        return {
            "files": {
                "haproxy.cfg": "",
            },
        }
    return {}


def good_keepalived_json():
    return keepalived_json(files=True)


def keepalived_json(**kwargs):
    if kwargs.get("files"):
        return {
            "files": {
                "keepalived.conf": "",
            },
        }
    return {}


@pytest.mark.parametrize(
    "args",
    # args: <fsid>, <daemon_id>, <config_json>, <image>
    [
        # fail due to: invalid fsid
        (["foobar", "wilma", good_haproxy_json(), SAMPLE_HAPROXY_IMAGE]),
        # fail due to: invalid daemon_id
        ([SAMPLE_UUID, "", good_haproxy_json(), SAMPLE_HAPROXY_IMAGE]),
        # fail due to: invalid image
        ([SAMPLE_UUID, "wilma", good_haproxy_json(), ""]),
        # fail due to: no files in config_json
        (
            [
                SAMPLE_UUID,
                "wilma",
                haproxy_json(files=False),
                SAMPLE_HAPROXY_IMAGE,
            ]
        ),
    ],
)
def test_haproxy_validation_errors(args):
    with pytest.raises(_cephadm.Error):
        with with_cephadm_ctx([]) as ctx:
            _cephadm.HAproxy(ctx, *args)


def test_haproxy_init():
    with with_cephadm_ctx([]) as ctx:
        ctx.config_json = json.dumps(good_haproxy_json())
        ctx.image = SAMPLE_HAPROXY_IMAGE
        hap = _cephadm.HAproxy.init(
            ctx,
            SAMPLE_UUID,
            "wilma",
        )
    assert hap.fsid == SAMPLE_UUID
    assert hap.daemon_id == "wilma"
    assert hap.image == SAMPLE_HAPROXY_IMAGE


def test_haproxy_container_mounts():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        cmounts = hap._get_container_mounts("/var/tmp")
        assert len(cmounts) == 1
        assert cmounts["/var/tmp/haproxy"] == "/var/lib/haproxy"


def test_haproxy_get_daemon_name():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        assert hap.get_daemon_name() == "haproxy.wilma"


def test_haproxy_get_container_name():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        name1 = hap.get_container_name()
        assert (
            name1 == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-haproxy.wilma"
        )
        name2 = hap.get_container_name(desc="extra")
        assert (
            name2
            == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-haproxy.wilma-extra"
        )


def test_haproxy_get_daemon_args():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        args = hap.get_daemon_args()
        assert args == ["haproxy", "-f", "/var/lib/haproxy/haproxy.cfg"]


@mock.patch("cephadm.logger")
def test_haproxy_create_daemon_dirs(_logger, cephadm_fs):
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        with pytest.raises(OSError):
            hap.create_daemon_dirs("/var/tmp", 45, 54)
        cephadm_fs.create_dir("/var/tmp")
        hap.create_daemon_dirs("/var/tmp", 45, 54)
        # TODO: make assertions about the dirs created


def test_haproxy_extract_uid_gid_haproxy():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        with mock.patch("cephadmlib.container_types.CephContainer") as cc:
            cc.return_value.run.return_value = "500 500"
            uid, gid = hap.uid_gid(ctx)
            cc.return_value.run.assert_called()
        assert uid == 500
        assert gid == 500


def test_haproxy_get_sysctl_settings():
    with with_cephadm_ctx([]) as ctx:
        hap = _cephadm.HAproxy(
            ctx,
            SAMPLE_UUID,
            "wilma",
            good_haproxy_json(),
            SAMPLE_HAPROXY_IMAGE,
        )
        ss = hap.get_sysctl_settings()
        assert len(ss) == 3


@pytest.mark.parametrize(
    "args",
    # args: <fsid>, <daemon_id>, <config_json>, <image>
    [
        # fail due to: invalid fsid
        (
            [
                "foobar",
                "barney",
                good_keepalived_json(),
                SAMPLE_KEEPALIVED_IMAGE,
            ]
        ),
        # fail due to: invalid daemon_id
        ([SAMPLE_UUID, "", good_keepalived_json(), SAMPLE_KEEPALIVED_IMAGE]),
        # fail due to: invalid image
        ([SAMPLE_UUID, "barney", good_keepalived_json(), ""]),
        # fail due to: no files in config_json
        (
            [
                SAMPLE_UUID,
                "barney",
                keepalived_json(files=False),
                SAMPLE_KEEPALIVED_IMAGE,
            ]
        ),
    ],
)
def test_keepalived_validation_errors(args):
    with pytest.raises(_cephadm.Error):
        with with_cephadm_ctx([]) as ctx:
            _cephadm.Keepalived(ctx, *args)


def test_keepalived_init():
    with with_cephadm_ctx([]) as ctx:
        ctx.config_json = json.dumps(good_keepalived_json())
        ctx.image = SAMPLE_KEEPALIVED_IMAGE
        kad = _cephadm.Keepalived.init(
            ctx,
            SAMPLE_UUID,
            "barney",
        )
    assert kad.fsid == SAMPLE_UUID
    assert kad.daemon_id == "barney"
    assert kad.image == SAMPLE_KEEPALIVED_IMAGE


def test_keepalived_container_mounts():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        cmounts = kad._get_container_mounts("/var/tmp")
        assert len(cmounts) == 1
        assert (
            cmounts["/var/tmp/keepalived.conf"]
            == "/etc/keepalived/keepalived.conf"
        )


def test_keepalived_get_daemon_name():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        assert kad.get_daemon_name() == "keepalived.barney"


def test_keepalived_get_container_name():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        name1 = kad.get_container_name()
        assert (
            name1
            == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-keepalived.barney"
        )
        name2 = kad.get_container_name(desc="extra")
        assert (
            name2
            == "ceph-2d018a3f-8a8f-4cb9-a7cf-48bebb2cbaae-keepalived.barney-extra"
        )


def test_keepalived_get_container_envs():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        args = kad.get_container_envs()
        assert args == [
            "KEEPALIVED_AUTOCONF=false",
            "KEEPALIVED_CONF=/etc/keepalived/keepalived.conf",
            "KEEPALIVED_CMD=/usr/sbin/keepalived -n -l -f /etc/keepalived/keepalived.conf",
            "KEEPALIVED_DEBUG=false",
        ]


@mock.patch("cephadm.logger")
def test_keepalived_create_daemon_dirs(_logger, cephadm_fs):
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        with pytest.raises(OSError):
            kad.create_daemon_dirs("/var/tmp", 45, 54)
        cephadm_fs.create_dir("/var/tmp")
        kad.create_daemon_dirs("/var/tmp", 45, 54)
        # TODO: make assertions about the dirs created


def test_keepalived_extract_uid_gid_keepalived():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        with mock.patch("cephadmlib.container_types.CephContainer") as cc:
            cc.return_value.run.return_value = "500 500"
            uid, gid = kad.uid_gid(ctx)
            cc.return_value.run.assert_called()
        assert uid == 500
        assert gid == 500


def test_keepalived_get_sysctl_settings():
    with with_cephadm_ctx([]) as ctx:
        kad = _cephadm.Keepalived(
            ctx,
            SAMPLE_UUID,
            "barney",
            good_keepalived_json(),
            SAMPLE_KEEPALIVED_IMAGE,
        )
        ss = kad.get_sysctl_settings()
        assert len(ss) == 3

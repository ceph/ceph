import contextlib
import copy
import uuid

import pytest
import smbclient.security

import smbutil


def _rmtree(smb_cfg, share_name, dir_name):
    with smbutil.connection(smb_cfg, share_name) as sharep:
        (sharep / dir_name).rmtree()


def _rmshare(smb_cfg, share_def):
    cluster_id = share_def['cluster_id']
    share_id = share_def['share_id']
    remove = [
        {
            "resource_type": "ceph.smb.share",
            "cluster_id": cluster_id,
            "share_id": share_id,
            "intent": "removed",
        }
    ]
    smbutil.apply_resources(smb_cfg, remove)


def _everyone_full_control():
    return smbclient.security.SecurityDescriptor(
        owner=None,
        group=None,
        d_acl=[
            smbclient.security.ACE(
                sid='S-1-1-0',  # Everyone
                ace_type=smbclient.security.ACEType.ALLOWED,
                ace_flags=0,
                mask=0x001F01FF,  # Full control
            )
        ],
    )


@pytest.mark.acl_support
def test_acl_disabled_share(smb_cfg):
    tdir = f'TestACLDisabledDir_{uuid.uuid4()}'
    orig_share = smbutil.get_shares(smb_cfg)[0]
    orig_share_name = orig_share.get('name') or orig_share['share_id']
    assert not orig_share.get('acl_support')

    with contextlib.ExitStack() as estack:
        # set up test dir
        with smbutil.connection(smb_cfg, orig_share_name) as sharep:
            (sharep / tdir).mkdir()
        estack.callback(_rmtree, smb_cfg, orig_share_name, tdir)

        noacl_share = copy.deepcopy(orig_share)
        noacl_share['share_id'] = f'{orig_share["share_id"]}noacl'
        noacl_share_name = noacl_share['name'] = noacl_share['share_id']
        noacl_share['acl_support'] = 'disabled'

        smbutil.apply_resources(smb_cfg, [noacl_share])
        estack.callback(_rmshare, smb_cfg, noacl_share)

        # on a share with acls enabled the NT ACL will be able to be
        # set to a single Everyone-Full-Control ACE and persist that
        # value.
        with smbutil.connection(smb_cfg, orig_share_name) as sharep:
            acl_dir = sharep / tdir / 'hasacl'
            acl_dir.mkdir()
            sd = acl_dir.get_security_descriptor()
            orig_ace_count = len(sd.d_acl)
            acl_dir.set_security_descriptor(_everyone_full_control())
            sd = acl_dir.get_security_descriptor()
        assert orig_ace_count > len(sd.d_acl)
        assert len(sd.d_acl) == 1

        # on a share with acl disabled (on cephfs) the NT ACL is a translation
        # of the posix acl only. Trying to set a SD with a single ACE will
        # not round trip. The ACL will contain the various enries from the
        # posix acl.
        with smbutil.connection(smb_cfg, noacl_share_name) as sharep:
            noacl_dir = sharep / tdir / 'noacl'
            noacl_dir.mkdir()
            sd = noacl_dir.get_security_descriptor()
            orig_ace_count = len(sd.d_acl)
            noacl_dir.set_security_descriptor(_everyone_full_control())
            sd = noacl_dir.get_security_descriptor()
        assert orig_ace_count > 1
        assert len(sd.d_acl) > 1

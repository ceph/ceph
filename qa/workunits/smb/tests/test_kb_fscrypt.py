import pytest
import smbprotocol

import smbutil


@pytest.mark.kb_fscrypt
def test_invalid_key_uid_share(smb_cfg):
    """Configure a share for fscrypt but with an invalid KMIP ID.
    This tests basic error handling in keybridge and validates
    that when a key can not be fetched that the share is not
    accessible.
    """
    kb_fscrypt = smb_cfg.params["kb_fscrypt"]
    cluster_id = kb_fscrypt["cluster_id"]
    unused_subvolume = kb_fscrypt["unused_subvolume"]
    share_id = kb_fscrypt.get("share_id", "invalidkey1")
    invalid_key_id = kb_fscrypt.get("invalid_key_id", "9999")
    filename = "foo.txt"

    new_share = {
        "resource_type": "ceph.smb.share",
        "cluster_id": cluster_id,
        "share_id": share_id,
        "cephfs": {
            "volume": "cephfs",
            "subvolume": unused_subvolume,
            "path": "/",
            "fscrypt_key": {
                "scope": "kmip",
                "name": invalid_key_id,
            },
        },
    }
    smbutil.apply_share_config(smb_cfg, {"resources": [new_share]})

    with pytest.raises(smbprotocol.exceptions.Unsuccessful):
        with smbutil.connection(smb_cfg, share_id) as sharep:
            fname = sharep / filename
            fname.write_text("value: NOPE\n")

    del_share = {
        "resource_type": "ceph.smb.share",
        "cluster_id": cluster_id,
        "share_id": share_id,
        "intent": "removed",
    }
    smbutil.apply_share_config(smb_cfg, {"resources": [del_share]})

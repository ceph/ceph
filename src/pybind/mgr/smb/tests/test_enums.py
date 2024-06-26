import pytest

import smb.enums


@pytest.mark.parametrize(
    "value, strval",
    [
        (smb.enums.CephFSStorageProvider.KERNEL_MOUNT, "kcephfs"),
        (smb.enums.CephFSStorageProvider.SAMBA_VFS, "samba-vfs"),
        (smb.enums.SubSystem.CEPHFS, "cephfs"),
        (smb.enums.Intent.PRESENT, "present"),
        (smb.enums.Intent.REMOVED, "removed"),
        (smb.enums.State.CREATED, "created"),
        (smb.enums.State.NOT_PRESENT, "not present"),
        (smb.enums.State.PRESENT, "present"),
        (smb.enums.State.REMOVED, "removed"),
        (smb.enums.State.UPDATED, "updated"),
        (smb.enums.AuthMode.USER, "user"),
        (smb.enums.AuthMode.ACTIVE_DIRECTORY, "active-directory"),
    ],
)
def test_stringified(value, strval):
    assert str(value) == strval

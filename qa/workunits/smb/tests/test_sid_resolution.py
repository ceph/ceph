import pytest

import cephutil
import smbutil


@pytest.mark.domain
def test_sid_resolution(smb_cfg):
    """Verify that rpcclient lookupsids resolves domain user SIDs correctly
    inside the smbd container, preventing regressions on /run bind mount
    permissions that break smbd to winbindd communication (tracker#77120).
    """
    cluster_id = smbutil.get_shares(smb_cfg)[0]['cluster_id']
    username = smb_cfg.username
    password = smb_cfg.password

    result = cephutil.cephadm_enter_cmd(
        smb_cfg,
        cluster_id,
        ['wbinfo', '-n', username],
        capture_output=True,
        check=True,
    )
    user_sid = result.stdout.decode().split()[0]
    assert user_sid.startswith('S-'), f'unexpected SID format: {user_sid}'

    auth = f'{username}%{password}'
    result = cephutil.cephadm_enter_cmd(
        smb_cfg,
        cluster_id,
        [
            'rpcclient',
            'localhost',
            '-U',
            auth,
            '-c',
            f'lookupsids {user_sid}',
        ],
        capture_output=True,
        check=True,
    )
    output = result.stdout.decode()
    short_name = username.split('\\')[-1]
    assert short_name in output, (
        f'SID resolution failed: {short_name!r} not found in: {output}'
    )

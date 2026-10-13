import copy
import uuid

import pytest
import smbprotocol

import smbutil


@pytest.mark.users_groups_access
def test_update_users_groups(smb_cfg):
    filename = 'TestUserAccess1.txt'
    tv = str(uuid.uuid4())
    share_name1 = smbutil.get_shares(smb_cfg)[0]['name']

    # test with existing user, update test file
    with smbutil.connection(smb_cfg, share_name1) as sharep:
        fname = sharep / filename
        fname.write_text(f'value: {tv}\n')

    new_u = [
        {"name": "tc_duke", "password": "bcb7690188317ef17f54"},  # notsecret
        {"name": "tc_shelley", "password": "0177941ab5e569681b02"},  # notsecret
        {"name": "tc_gordon", "password": "ef97aec3a09c6de8cce8"},  # notsecret
        {"name": "tc_alex", "password": "085872411a373142a55c"},  # notsecret
    ]

    # these users should not be defined
    for uinfo in new_u:
        username = uinfo['name']
        password = uinfo['password']
        with pytest.raises(smbprotocol.exceptions.LogonFailure):
            with smbutil.connection(
                smb_cfg, share_name1, username=username, password=password
            ) as sharep:
                fname = sharep / filename
                fname.read_text()

    # fetch current u/g
    ug = smbutil.get_ug(smb_cfg)[0]
    assert 'values' in ug
    assert 'users' in ug['values']
    ug2 = copy.deepcopy(ug)

    # update u/g
    ug2['values']['users'].extend(new_u)
    smbutil.apply_resource(smb_cfg, ug2)

    try:
        # check updated users for access
        for uinfo in new_u:
            username = uinfo['name']
            password = uinfo['password']
            with smbutil.connection(
                smb_cfg, share_name1, username=username, password=password
            ) as sharep:

                fname = sharep / filename
                assert fname.read_text() == f'value: {tv}\n'
    finally:
        # restore orig config
        smbutil.apply_resource(smb_cfg, ug)

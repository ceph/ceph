from unittest import mock

from smb.rgw_auth import RGWAuthorizer


def test_rgw_authorizer_uses_aes256k():
    mon_command_issuer = mock.MagicMock()
    mon_command_issuer.mon_command.return_value = (0, '', '')

    RGWAuthorizer(mon_command_issuer).authorize_entity('client.smb.rgw.foo')

    command = mon_command_issuer.mon_command.call_args.args[0]
    assert command['prefix'] == 'auth get-or-create'
    assert command['key_type'] == 'aes256k'

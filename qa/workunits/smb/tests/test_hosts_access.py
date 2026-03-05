import copy

import pytest
import smbprotocol

import smbutil


# BOGUS is an IP that should never be assigned to a test node running in
# teuthology (or in general)
BOGUS = '192.0.2.222'
# BOGUS_NET is a full network address version of the above.
BOGUS_NET = '192.0.2.0/24'


@pytest.mark.hosts_access
class TestHostsAccessToggle1:

    @pytest.fixture(scope='class')
    def config(self, smb_cfg):
        filename = 'TestHostAcess1.txt'
        orig = smbutil.get_shares(smb_cfg)[0]
        share_name = orig['name']

        print('Testing original share configuration...')
        with smbutil.connection(smb_cfg, share_name) as sharep:
            fname = sharep / filename
            fname.write_text('value: setup\n')

        yield (filename, orig)

        print('Restoring original share configuration...')
        smbutil.apply_share_config(smb_cfg, orig)
        # With the IP restriction removed, access should succeed and we can
        # clean up our test file
        with smbutil.connection(smb_cfg, share_name) as sharep:
            fname = sharep / filename
            fname.unlink()

    @pytest.fixture(autouse=True)
    def config_each(self, config):
        """Bind configuration values to each test class instance."""
        # Pytest won't pass the same 'self' to a class scope fixture and the
        # methods.
        self.filename, self.orig = config

    @property
    def share_name(self):
        return self.orig['name']

    def test_no_access_bogus_allow(self, smb_cfg):
        "Reject access when only the bogus address is allowed"
        mod_share = copy.deepcopy(self.orig)
        mod_share['hosts_access'] = [
            {'access': 'allow', 'address': BOGUS},
        ]
        applied = smbutil.apply_share_config(smb_cfg, mod_share)
        assert applied['share_id'] == mod_share['share_id']
        assert applied['hosts_access'] == mod_share['hosts_access']

        with pytest.raises(smbprotocol.exceptions.AccessDenied):
            with smbutil.connection(smb_cfg, self.share_name) as sharep:
                fname = sharep / self.filename
                fname.write_text('value: NOPE\n')

    def test_access_bogus_deny(self, smb_cfg):
        "Allow access when only the bogus address is denied"
        mod_share = copy.deepcopy(self.orig)
        mod_share['hosts_access'] = [
            {'access': 'deny', 'address': BOGUS},
        ]
        applied = smbutil.apply_share_config(smb_cfg, mod_share)
        assert applied['share_id'] == mod_share['share_id']
        assert applied['hosts_access'] == mod_share['hosts_access']

        with smbutil.connection(smb_cfg, self.share_name) as sharep:
            fname = sharep / self.filename
            fname.write_text('value: test_access_bogus_deny\n')

    def test_access_self_allow(self, smb_cfg):
        "Allow access when the client ip is allowed"
        mod_share = copy.deepcopy(self.orig)
        mod_share['hosts_access'] = [
            {'access': 'allow', 'address': BOGUS},
            {'access': 'allow', 'address': smb_cfg.default_client.ip_address},
        ]
        applied = smbutil.apply_share_config(smb_cfg, mod_share)
        assert applied['share_id'] == mod_share['share_id']
        assert applied['hosts_access'] == mod_share['hosts_access']

        with smbutil.connection(smb_cfg, self.share_name) as sharep:
            fname = sharep / self.filename
            fname.write_text('value: test_access_self_allow\n')

    def test_no_access_self_deny(self, smb_cfg):
        "Deny access when the client ip is explicitly denied"
        mod_share = copy.deepcopy(self.orig)
        mod_share['hosts_access'] = [
            {'access': 'deny', 'address': smb_cfg.default_client.ip_address},
        ]
        applied = smbutil.apply_share_config(smb_cfg, mod_share)
        assert applied['share_id'] == mod_share['share_id']
        assert applied['hosts_access'] == mod_share['hosts_access']

        with pytest.raises(smbprotocol.exceptions.AccessDenied):
            with smbutil.connection(smb_cfg, self.share_name) as sharep:
                fname = sharep / self.filename
                fname.write_text('value: NOPE\n')

    def test_no_access_bogus_net_allow(self, smb_cfg):
        "Reject access when only the bogus network address is allowed"
        mod_share = copy.deepcopy(self.orig)
        mod_share['hosts_access'] = [
            {'access': 'allow', 'network': BOGUS_NET},
        ]
        applied = smbutil.apply_share_config(smb_cfg, mod_share)
        assert applied['share_id'] == mod_share['share_id']
        assert applied['hosts_access'] == mod_share['hosts_access']

        with pytest.raises(smbprotocol.exceptions.AccessDenied):
            with smbutil.connection(smb_cfg, self.share_name) as sharep:
                fname = sharep / self.filename
                fname.write_text('value: NOPE\n')

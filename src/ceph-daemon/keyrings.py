import logging
from container import CephContainer
import tempfile
import os


class Keyring(object):
    def __init__(self, config):
        self.config = config

    def __getattr__(self, attr):
        return getattr(self.config, attr)

    def create_mon_key(self):
        # create some initial keys
        logging.info('Creating initial mon key..')
        mon_key = CephContainer(
            image=self.image,
            entrypoint='/usr/bin/ceph-authtool',
            args=['--gen-print-key'],
        ).run().stdout

        return mon_key

    def create_admin_key(self):
        logging.info('Creating initial admin key..')
        admin_key = CephContainer(
            image=self.image,
            entrypoint='/usr/bin/ceph-authtool',
            args=['--gen-print-key'],
        ).run().stdout
        return admin_key

    def create_mgr_key(self):
        logging.info('Creating initial mgr key..')
        mgr_key = CephContainer(
            image=self.image,
            entrypoint='/usr/bin/ceph-authtool',
            args=['--gen-print-key'],
        ).run().stdout
        return mgr_key

    def create_initial_keyring(self, mgr_id):
        mon_key = self.create_mon_key()
        admin_key = self.create_admin_key()
        mgr_key = self.create_mgr_key()

        return f"""
            [mon.]
                key = {mon_key}
                caps mon = allow *'
            [client.admin]
                key = {admin_key}
                caps mon = allow *
                caps mds = allow *
                caps mgr = allow *
                caps osd = allow *
            [mgr.{mgr_id}]
                key = {mgr_key}
                caps mon = allow profile mgr
                caps mds = allow *
                caps osd = allow *
        """, mon_key, admin_key, mgr_key

    def write_tmp_keyring(self, keyring):
        # tmp keyring file
        tmp_keyring = tempfile.NamedTemporaryFile(mode='w')
        os.fchmod(tmp_keyring.fileno(), 0o600)
        os.fchown(tmp_keyring.fileno(), self.ceph_uid, self.ceph_gid)
        tmp_keyring.write(keyring)
        tmp_keyring.flush()
        return tmp_keyring

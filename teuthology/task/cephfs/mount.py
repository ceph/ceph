
import logging
import os

log = logging.getLogger(__name__)


class CephFSMount(object):
    def __init__(self, test_dir, client_id, client_remote):
        """
        :param test_dir: Global teuthology test dir
        :param client_id: Client ID, the 'foo' in client.foo
        :param client_remote: Remote instance for the host where client will run
        """

        self.test_dir = test_dir
        self.client_id = client_id
        self.client_remote = client_remote

        self.mountpoint = os.path.join(self.test_dir, 'mnt.{id}'.format(id=self.client_id))
        self.test_files = ['a', 'b', 'c']

    @property
    def _mount_path(self):
        return os.path.join(self.test_dir, 'mnt.{0}'.format(self.client_id))

    def create_files(self):
        for suffix in self.test_files:
            log.info("Creating file {0}".format(suffix))
            self.client_remote.run(args=[
                'sudo', 'touch', os.path.join(self._mount_path, suffix)
            ])

    def check_files(self):
        """
        This will raise a CommandFailedException if expected files are not present
        """
        for suffix in self.test_files:
            log.info("Checking file {0}".format(suffix))
            r = self.client_remote.run(args=[
                'sudo', 'ls', os.path.join(self._mount_path, suffix)
            ], check_status=False)
            if r.exitstatus != 0:
                raise RuntimeError("Expected file {0} not found".format(suffix))

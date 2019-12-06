import logging
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.drive_selection.selector import DriveSelection

logger = logging.getLogger(__name__)


class ToCephVolume(object):

    def __init__(self,
                 spec,  # type: DriveGroupSpec
                 selection  # type: DriveSelection
                 ):

        self.spec = spec
        self.selection = selection

    def run(self):
        """ Generate ceph-volume commands based on the DriveGroup filters """
        try:
            data_devices = [x.path for x in self.selection.data_devices()]
        except AttributeError:
            data_devices = [x for x in self.selection.data_devices()]
        db_devices = [x.path for x in self.selection.db_devices()]
        wal_devices = [x.path for x in self.selection.wal_devices()]
        journal_devices = [x.path for x in self.selection.journal_devices()]

        if not data_devices:
            return None

        if self.spec.objectstore == 'filestore':
            cmd = "lvm batch --no-auto"

            cmd += " {}".format(" ".join(data_devices))

            if self.spec.journal_size:
                cmd += " --journal-size {}".format(self.spec.journal_size)

            if journal_devices:
                cmd += " --journal-devices {}".format(
                    ' '.join(journal_devices))

            cmd += " --filestore"

        if self.spec.objectstore == 'bluestore':

            cmd = "lvm batch --no-auto {}".format(" ".join(data_devices))

            if db_devices:
                cmd += " --db-devices {}".format(" ".join(db_devices))

            if wal_devices:
                cmd += " --wal-devices {}".format(" ".join(wal_devices))

            if self.spec.block_wal_size:
                cmd += " --block-wal-size {}".format(self.spec.block_wal_size)

            if self.spec.block_db_size:
                cmd += " --block-db-size {}".format(self.spec.block_db_size)

        if self.spec.encrypted:
            cmd += " --dmcrypt"

        if self.spec.osds_per_device:
            cmd += " --osds-per-device {}".format(self.spec.osds_per_device)

        cmd += " --yes"
        cmd += " --no-systemd"

        return cmd

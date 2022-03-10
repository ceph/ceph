import logging

try:
    from typing import Optional, List
except ImportError:
    pass

from ceph.deployment.drive_selection.selector import DriveSelection

logger = logging.getLogger(__name__)


# TODO refactor this to a DriveSelection method
class to_ceph_volume(object):

    def __init__(self,
                 selection,  # type: DriveSelection
                 osd_id_claims=None,  # type: Optional[List[str]]
                 preview=False  # type: bool
                 ):

        self.selection = selection
        self.spec = selection.spec
        self.preview = preview
        self.osd_id_claims = osd_id_claims

    def run(self):
        # type: () -> List[str]
        """ Generate ceph-volume commands based on the DriveGroup filters """
        data_devices = [x.path for x in self.selection.data_devices()]
        db_devices = [x.path for x in self.selection.db_devices()]
        wal_devices = [x.path for x in self.selection.wal_devices()]
        journal_devices = [x.path for x in self.selection.journal_devices()]

        if not data_devices:
            return []

        cmds: List[str] = []
        if self.spec.method == 'raw':
            assert self.spec.objectstore == 'bluestore'
            # ceph-volume raw prepare only support 1:1 ratio of data to db/wal devices
            if data_devices and db_devices:
                if len(data_devices) != len(db_devices):
                    raise ValueError('Number of data devices must match number of '
                                     'db devices for raw mode osds')
            if data_devices and wal_devices:
                if len(data_devices) != len(wal_devices):
                    raise ValueError('Number of data devices must match number of '
                                     'wal devices for raw mode osds')
            # for raw prepare each data device needs its own prepare command
            dev_counter = 0
            while dev_counter < len(data_devices):
                cmd = "raw prepare --bluestore"
                cmd += " --data {}".format(data_devices[dev_counter])
                if db_devices:
                    cmd += " --block.db {}".format(db_devices[dev_counter])
                if wal_devices:
                    cmd += " --block.wal {}".format(wal_devices[dev_counter])
                cmds.append(cmd)
                dev_counter += 1

        elif self.spec.objectstore == 'filestore':
            # for lvm batch we can just do all devices in one command
            cmd = "lvm batch --no-auto"

            cmd += " {}".format(" ".join(data_devices))

            if self.spec.journal_size:
                cmd += " --journal-size {}".format(self.spec.journal_size)

            if journal_devices:
                cmd += " --journal-devices {}".format(
                    ' '.join(journal_devices))

            cmd += " --filestore"
            cmds.append(cmd)

        elif self.spec.objectstore == 'bluestore':
            # for lvm batch we can just do all devices in one command
            cmd = "lvm batch --no-auto {}".format(" ".join(data_devices))

            if db_devices:
                cmd += " --db-devices {}".format(" ".join(db_devices))

            if wal_devices:
                cmd += " --wal-devices {}".format(" ".join(wal_devices))

            if self.spec.block_wal_size:
                cmd += " --block-wal-size {}".format(self.spec.block_wal_size)

            if self.spec.block_db_size:
                cmd += " --block-db-size {}".format(self.spec.block_db_size)
            cmds.append(cmd)

        for i in range(len(cmds)):
            if self.spec.encrypted:
                cmds[i] += " --dmcrypt"

            if self.spec.osds_per_device:
                cmds[i] += " --osds-per-device {}".format(self.spec.osds_per_device)

            if self.spec.data_allocate_fraction:
                cmds[i] += " --data-allocate-fraction {}".format(self.spec.data_allocate_fraction)

            if self.osd_id_claims:
                cmds[i] += " --osd-ids {}".format(" ".join(self.osd_id_claims))

            if self.spec.method != 'raw':
                cmds[i] += " --yes"
                cmds[i] += " --no-systemd"

            if self.preview:
                cmds[i] += " --report"
                cmds[i] += " --format json"

        return cmds

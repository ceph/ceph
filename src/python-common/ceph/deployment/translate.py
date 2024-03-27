import logging

try:
    from typing import Optional, List, Dict
except ImportError:
    pass

from ceph.deployment.drive_selection.selector import DriveSelection

logger = logging.getLogger(__name__)


# TODO refactor this to a DriveSelection method
class to_ceph_volume(object):

    NO_CRUSH = '_NO_CRUSH'

    def __init__(self,
                 selection,  # type: DriveSelection
                 osd_id_claims=None,  # type: Optional[List[str]]
                 preview=False  # type: bool
                 ):

        self.selection = selection
        self.spec = selection.spec
        self.preview = preview
        self.osd_id_claims = osd_id_claims

    def prepare_devices(self):

        # type: () -> Dict[str, List[str]]

        lvcount: Dict[str, List[str]] = dict()

        """
        for each device, check if it's just a path or it has a crush_device
        class definition, and append an entry to the right crush_device_
        class group
        """
        for device in self.selection.data_devices():
            # iterate on List[Device], containing both path and
            # crush_device_class
            path = device.path
            crush_device_class = (
                device.crush_device_class
                or self.spec.crush_device_class
                or self.NO_CRUSH
            )

            if path is None:
                raise ValueError("Device path can't be empty")

            lvcount.setdefault(crush_device_class, []).append(path)

        return lvcount

    def run(self):
        # type: () -> List[str]
        """ Generate ceph-volume commands based on the DriveGroup filters """

        db_devices = [x.path for x in self.selection.db_devices()]
        wal_devices = [x.path for x in self.selection.wal_devices()]

        if not self.selection.data_devices():
            return []

        cmds: List[str] = []

        devices = self.prepare_devices()
        # get the total number of devices provided by the Dict[str, List[str]]
        devices_count = len(sum(list(devices.values()), []))

        if devices and db_devices:
            if (devices_count != len(db_devices)) and (self.spec.method == 'raw'):
                raise ValueError('Number of data devices must match number of '
                                 'db devices for raw mode osds')

        if devices and wal_devices:
            if (devices_count != len(wal_devices)) and (self.spec.method == 'raw'):
                raise ValueError('Number of data devices must match number of '
                                 'wal devices for raw mode osds')

        for d in devices.keys():
            data_devices: Optional[List[str]] = devices.get(d)
            if not data_devices:
                continue

            if self.spec.method == 'raw':
                assert self.spec.objectstore == 'bluestore'
                # ceph-volume raw prepare only support 1:1 ratio of data to db/wal devices
                # for raw prepare each data device needs its own prepare command
                dev_counter = 0
                # reversing the lists as we're assigning db_devices sequentially
                db_devices.reverse()
                wal_devices.reverse()

                while dev_counter < len(data_devices):
                    cmd = "raw prepare --bluestore"
                    cmd += " --data {}".format(data_devices[dev_counter])
                    if db_devices:
                        cmd += " --block.db {}".format(db_devices.pop())
                    if wal_devices:
                        cmd += " --block.wal {}".format(wal_devices.pop())
                    if d != self.NO_CRUSH:
                        cmd += " --crush-device-class {}".format(d)

                    cmds.append(cmd)
                    dev_counter += 1

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

                if d != self.NO_CRUSH:
                    cmd += " --crush-device-class {}".format(d)
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

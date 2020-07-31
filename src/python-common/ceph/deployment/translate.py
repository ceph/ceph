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
        # type: () -> Optional[str]
        """ Generate ceph-volume commands based on the DriveGroup filters """
        data_devices = [x.path for x in self.selection.data_devices()]
        db_devices = [x.path for x in self.selection.db_devices()]
        wal_devices = [x.path for x in self.selection.wal_devices()]
        journal_devices = [x.path for x in self.selection.journal_devices()]

        if not data_devices:
            return None

        cmd = ""
        if self.spec.objectstore == 'filestore':
            cmd = "lvm batch --no-auto"

            cmd += " {}".format(" ".join(data_devices))

            if self.spec.journal_size:
                cmd += " --journal-size {}".format(self.spec.journal_size)

            if journal_devices:
                cmd += " --journal-devices {}".format(
                    ' '.join(journal_devices))

            cmd += " --filestore"

        # HORRIBLE HACK
        if self.spec.objectstore == 'bluestore' and \
           not self.spec.encrypted and \
           not self.spec.osds_per_device and \
           len(data_devices) == 1 and \
           not db_devices and \
           not wal_devices:
            cmd = "lvm prepare --bluestore --data %s --no-systemd" % (' '.join(data_devices))
            if self.osd_id_claims:
                cmd += " --osd-id {}".format(str(self.osd_id_claims[0]))
            if self.preview:
                # Like every horrible hack, this has sideffects on other features.
                # In this case, 'lvm prepare' has neither a '--report' nor a '--format json' option
                # which essentially doesn't allow for a proper previews here.
                # Fall back to lvm batch in order to get a preview.
                return f"lvm batch --no-auto {' '.join(data_devices)} --report --format json"
            return cmd

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

        if self.osd_id_claims:
            cmd += " --osd-ids {}".format(" ".join(self.osd_id_claims))

        cmd += " --yes"
        cmd += " --no-systemd"

        if self.preview:
            cmd += " --report"
            cmd += " --format json"

        return cmd

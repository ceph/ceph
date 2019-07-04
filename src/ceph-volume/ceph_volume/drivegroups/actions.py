import logging
import argparse
import yaml
from textwrap import dedent
from ceph_volume import terminal
from .selector import DriveSelection
from ceph_volume.devices.lvm.batch import Batch

mlogger = terminal.MultiLogger(__name__)
logger = logging.getLogger(__name__)


class DriveGroupsCommand(object):
    """ Parent class for DriveGroup commands """

    def __init__(self, argv, report_only=False, non_interative=False):
        self.argv = argv
        self.non_interative = non_interative
        self.report = report_only

    @staticmethod
    def _load_drive_group_file(filename):
        with open(filename, 'rb') as _fd:
            return yaml.load(_fd)

    def apply(self, drive_selector):
        data = drive_selector.data_devices()
        db = drive_selector.db_devices()
        wal = drive_selector.wal_devices()
        journal = drive_selector.journal_devices()

        if not data:
            return "Nothing matched #TODO message"

        data_strings = [dev.path for dev in data]

        argv_string = data_strings

        if db:
            db_strings = [dev.path for dev in db]
            argv_string.extend(['--db-devices'])
            argv_string.extend(db_strings)

        if wal:
            wal_strings = [dev.path for dev in wal]
            argv_string.extend(['--wal-devices'])
            argv_string.extend(wal_strings)

        if journal:
            journal_strings = [dev.path for dev in journal]
            argv_string.extend(['--journal-devices'])
            argv_string.extend(journal_strings)

        if self.report:
            argv_string.extend(["--report"])

        if self.non_interative:
            argv_string.extend(["--yes"])

        #TODO: Improve this! Bare string passing is a hack!
        Batch(argv_string).main()

    def _run(self, _args):
        drive_group_def = self._load_drive_group_file(_args.filename)
        for drive_group_name, drive_group_spec in drive_group_def.items():
            mlogger.info(f"Processing DriveGroup <{drive_group_name}>")
            self.apply(DriveSelection(drive_group_spec))


class DriveGroupsApply(DriveGroupsCommand):
    def __init__(self, argv):
        DriveGroupsCommand.__init__(self, argv, report_only=False)

    def main(self):
        sub_command_help = dedent("""

        Dummy text for DriveGroup foo.

            ceph-volume drivegroup apply <example_drivegroup_file.yaml>

        When you apply DriveGroups, ceph-volume will apply foo #TODO.
        # TODO: re-write
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume drivegroups apply',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'filename', help='A file containing the DriveGroups spec')
        if len(self.argv) == 0:
            print(sub_command_help)
            return

        args = parser.parse_args(self.argv)
        self._run(args)


class DriveGroupsShow(DriveGroupsCommand):
    def __init__(self, argv):
        DriveGroupsCommand.__init__(self, argv, report_only=True)

    def main(self):
        sub_command_help = dedent("""

        Dummy text for DriveGroup foo.

            ceph-volume drivegroup show <example_drivegroup_file.yaml>

        When you apply DriveGroups, ceph-volume will show foo #TODO.
        # TODO: re-write
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume drivegroups show',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'filename', help='A file containing the DriveGroups spec')
        if len(self.argv) == 0:
            print(sub_command_help)
            return

        args = parser.parse_args(self.argv)
        self._run(args)

import argparse

from textwrap import dedent


class Zap(object):

    help = 'Destroy a logical volume or partition.'

    def __init__(self, argv):
        self.argv = argv

    def zap(self, args):
        pass

    def main(self):
        sub_command_help = dedent("""
        Destroys the given logical volume or partition. If given a path to a logical
        volume it must be in the format of vg name/lv name. The logical volume will then
        be removed. If given a partition name like /dev/sdc1 the partition will be destroyed.

        Example calls for supported scenarios:

          Zapping a logical volume:

              ceph-volume lvm zap {vg name/lv name}

          Zapping a partition:

              ceph-volume lvm zap /dev/sdc1

        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume lvm zap',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=sub_command_help,
        )

        parser.add_argument(
            'device',
            metavar='DEVICE',
            nargs='?',
            help='Path to an lv (as vg/lv) or to a partition like /dev/sda1'
        )
        if len(self.argv) == 0:
            print(sub_command_help)
            return
        args = parser.parse_args(self.argv)
        self.zap(args)

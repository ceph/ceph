import argparse

from textwrap import dedent
# from ceph_volume.util import arg_validators

class Zap(object):

    help = 'Zap a device'

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
	Zap a device
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs inventory',
            description=sub_command_help,
        )
        parser.add_argument(
            'devices',
            metavar='DEVICES',
            nargs='*',
            # type=arg_validators.ValidDevice(gpt_ok=True),
            default=[],
            help='Path to one or many lv (as vg/lv), partition (as /dev/sda1) or device (as /dev/sda)'
        )

        if len(self.argv) == 0 or len(self.argv) > 0:
            print("Zap: Print Help")
            print(sub_command_help)
            return


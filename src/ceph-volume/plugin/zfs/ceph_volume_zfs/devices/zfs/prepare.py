import argparse

from textwrap import dedent
# from ceph_volume.util import arg_validators

class Prepare(object):

    help = 'Prepare a device'

    def __init__(self, argv):
        self.argv = argv

    def main(self):
        sub_command_help = dedent("""
	Prepare a device
        """)
        parser = argparse.ArgumentParser(
            prog='ceph-volume zfs prepare',
            description=sub_command_help,
        )
        if len(self.argv) == 0 or len(self.argv) > 0:
            print("Prepare: Print Help")
            print(sub_command_help)
            return


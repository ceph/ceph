import argparse
from textwrap import dedent
from ceph_volume import terminal, decorators
from ceph_volume.util import disk


device_list_template = """
  * {path: <25} {size: <10} {state}"""


def device_formatter(devices):
    lines = []
    for path, details in devices:
        lines.append(device_list_template.format(
            path=path, size=details['human_readable_size'],
            state='solid' if details['rotational'] == '0' else 'rotational')
        )

    return ''.join(lines)


class Auto(object):

    help = 'Auto-detect devices for multi-OSD provisioning with minimal interaction'

    _help = dedent("""
    Automatically detect devices ready for OSD provisioning based on configurable strategies.

    Detected devices:
    {detected_devices}

    Current strategy: {strategy_name}
    Path: {strategy_path}

    """)

    # TODO: add the reporting sub-command here (list?)
    mapper = {
    }

    def __init__(self, argv):
        self.argv = argv

    def get_devices(self):
        all_devices = disk.get_devices()
        # remove devices with partitions
        # XXX Should be optional when getting device info
        for device, detail in all_devices.items():
            if detail.get('partitions') != {}:
                del all_devices[device]
        devices = sorted(all_devices.items(), key=lambda x: (x[0], x[1]['size']))
        return device_formatter(devices)

    def print_help(self, sub_help):
        return self._help.format(
            detected_devices=self.get_devices(),
            strategy_name='default',
            strategy_path='/etc/ceph/osd/strategies/default')

    @decorators.needs_root
    def main(self):
        terminal.dispatch(self.mapper, self.argv)
        parser = argparse.ArgumentParser(
            prog='ceph-volume auto',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=self.print_help(terminal.subhelp(self.mapper)),
        )
        parser.parse_args(self.argv)
        if len(self.argv) <= 1:
            return parser.print_help()

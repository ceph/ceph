import argparse
import os
from ceph_volume import terminal
from ceph_volume import decorators
from ceph_volume.util import disk
from ceph_volume.util.device import Device


class ValidDevice(object):

    def __init__(self, as_string=False, gpt_ok=False):
        self.as_string = as_string
        self.gpt_ok = gpt_ok

    def __call__(self, dev_path):
        device = self._is_valid_device(dev_path)
        return self._format_device(device)

    def _format_device(self, device):
        if self.as_string:
            if device.is_lv:
                # all codepaths expect an lv path to be returned in this format
                return "{}/{}".format(device.vg_name, device.lv_name)
            return device.path
        return device

    def _is_valid_device(self, dev_path):
        device = Device(dev_path)
        error = None
        if not device.exists:
            error = "Unable to proceed with non-existing device: %s" % dev_path
        # FIXME this is not a nice API, this validator was meant to catch any
        # non-existing devices upfront, not check for gpt headers. Now this
        # needs to optionally skip checking gpt headers which is beyond
        # verifying if the device exists. The better solution would be to
        # configure this with a list of checks that can be excluded/included on
        # __init__
        elif device.has_gpt_headers and not self.gpt_ok:
            error = "GPT headers found, they must be removed on: %s" % dev_path
        if device.has_partitions:
            raise RuntimeError("Device {} has partitions.".format(dev_path))
        if error:
            raise argparse.ArgumentError(None, error)
        return device


class ValidBatchDevice(ValidDevice):

    def __call__(self, dev_path):
        dev = self._is_valid_device(dev_path)
        if dev.is_partition:
            raise argparse.ArgumentError(
                None,
                '{} is a partition, please pass '
                'LVs or raw block devices'.format(dev_path))
        return self._format_device(dev)


class OSDPath(object):
    """
    Validate path exists and it looks like an OSD directory.
    """

    @decorators.needs_root
    def __call__(self, string):
        if not os.path.exists(string):
            error = "Path does not exist: %s" % string
            raise argparse.ArgumentError(None, error)

        arg_is_partition = disk.is_partition(string)
        if arg_is_partition:
            return os.path.abspath(string)
        absolute_path = os.path.abspath(string)
        if not os.path.isdir(absolute_path):
            error = "Argument is not a directory or device which is required to scan"
            raise argparse.ArgumentError(None, error)
        key_files = ['ceph_fsid', 'fsid', 'keyring', 'ready', 'type', 'whoami']
        dir_files = os.listdir(absolute_path)
        for key_file in key_files:
            if key_file not in dir_files:
                terminal.error('All following files must exist in path: %s' % ' '.join(key_files))
                error = "Required file (%s) was not found in OSD dir path: %s" % (
                    key_file,
                    absolute_path
                )
                raise argparse.ArgumentError(None, error)

        return os.path.abspath(string)


def exclude_group_options(parser, groups, argv=None):
    """
    ``argparse`` has the ability to check for mutually exclusive options, but
    it only allows a basic XOR behavior: only one flag can be used from
    a defined group of options. This doesn't help when two groups of options
    need to be separated. For example, with filestore and bluestore, neither
    set can be used in conjunction with the other set.

    This helper validator will consume the parser to inspect the group flags,
    and it will group them together from ``groups``. This allows proper error
    reporting, matching each incompatible flag with its group name.

    :param parser: The argparse object, once it has configured all flags. It is
                   required to contain the group names being used to validate.
    :param groups: A list of group names (at least two), with the same used for
                  ``add_argument_group``
    :param argv: Consume the args (sys.argv) directly from this argument

    .. note: **Unfortunately** this will not be able to validate correctly when
    using default flags. In the case of filestore vs. bluestore, ceph-volume
    defaults to --bluestore, but we can't check that programmatically, we can
    only parse the flags seen via argv
    """
    # Reduce the parser groups to only the groups we need to intersect
    parser_groups = [g for g in parser._action_groups if g.title in groups]
    # A mapping of the group name to flags/options
    group_flags = {}
    flags_to_verify = []
    for group in parser_groups:
        # option groups may have more than one item in ``option_strings``, this
        # will loop over ``_group_actions`` which contains the
        # ``option_strings``, like ``['--filestore']``
        group_flags[group.title] = [
            option for group_action in group._group_actions
            for option in group_action.option_strings
        ]

    # Gather all the flags present in the groups so that we only check on those.
    for flags in group_flags.values():
        flags_to_verify.extend(flags)

    seen = []
    last_flag = None
    last_group = None
    for flag in argv:
        if flag not in flags_to_verify:
            continue
        for group_name, flags in group_flags.items():
            if flag in flags:
                seen.append(group_name)
                # We are mutually excluding groups, so having more than 1 group
                # in ``seen`` means we must raise an error
                if len(set(seen)) == len(groups):
                    terminal.warning('Incompatible flags were found, some values may get ignored')
                    msg = 'Cannot use %s (%s) with %s (%s)' % (
                        last_flag, last_group, flag, group_name
                    )
                    terminal.warning(msg)
            last_group = group_name
        last_flag = flag

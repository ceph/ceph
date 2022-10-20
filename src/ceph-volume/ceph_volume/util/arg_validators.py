import argparse
import os
import math
import json
from ceph_volume import terminal, decorators, process, conf
from ceph_volume.util import disk


def valid_osd_id(val):
    return str(int(val))

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

class ValidFraction(object):
    """
    Validate fraction is in (0, 1.0]
    """

    def __call__(self, fraction):
        fraction_float = float(fraction)
        if math.isnan(fraction_float) or fraction_float <= 0.0 or fraction_float > 1.0:
            raise argparse.ArgumentError(None, 'Fraction %f not in (0,1.0]' % fraction_float)
        return fraction_float

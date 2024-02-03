import argparse
import os
import math
from ceph_volume import terminal, decorators, process
from ceph_volume.util.device import Device
from ceph_volume.util import disk
from ceph_volume.util.encryption import set_dmcrypt_no_workqueue
from ceph_volume import process, conf

def valid_osd_id(val):
    return str(int(val))

class DmcryptAction(argparse._StoreTrueAction):
    def __init__(self, *args, **kwargs):
        super(DmcryptAction, self).__init__(*args, **kwargs)

    def __call__(self, *args, **kwargs):
        set_dmcrypt_no_workqueue()
        super(DmcryptAction, self).__call__(*args, **kwargs)

class ValidDevice(object):

    def __init__(self, as_string=False, gpt_ok=False):
        self.as_string = as_string
        self.gpt_ok = gpt_ok

    def __call__(self, dev_path):
        self.get_device(dev_path)
        self._validated_device = self._is_valid_device()
        return self._format_device(self._validated_device)

    def get_device(self, dev_path):
        self._device = Device(dev_path)
        self.dev_path = dev_path

    def _format_device(self, device):
        if self.as_string:
            if device.is_lv:
                # all codepaths expect an lv path to be returned in this format
                return "{}/{}".format(device.vg_name, device.lv_name)
            return device.path
        return device

    def _is_valid_device(self):
        error = None
        if not self._device.exists:
            error = "Unable to proceed with non-existing device: %s" % self.dev_path
        # FIXME this is not a nice API, this validator was meant to catch any
        # non-existing devices upfront, not check for gpt headers. Now this
        # needs to optionally skip checking gpt headers which is beyond
        # verifying if the device exists. The better solution would be to
        # configure this with a list of checks that can be excluded/included on
        # __init__
        elif self._device.has_gpt_headers and not self.gpt_ok:
            error = "GPT headers found, they must be removed on: %s" % self.dev_path
        if self._device.has_partitions:
            raise RuntimeError("Device {} has partitions.".format(self.dev_path))
        if error:
            raise argparse.ArgumentError(None, error)
        return self._device


class ValidZapDevice(ValidDevice):
    def __call__(self, dev_path):
        super().get_device(dev_path)
        return self._format_device(self._is_valid_device())

    def _is_valid_device(self, raise_sys_exit=True):
        super()._is_valid_device()
        return self._device


class ValidDataDevice(ValidDevice):
    def __call__(self, dev_path):
        super().get_device(dev_path)
        return self._format_device(self._is_valid_device())

    def _is_valid_device(self, raise_sys_exit=True):
        super()._is_valid_device()
        if self._device.used_by_ceph:
            terminal.info('Device {} is already prepared'.format(self.dev_path))
            if raise_sys_exit:
                raise SystemExit(0)
        if self._device.has_fs and not self._device.used_by_ceph:
            raise RuntimeError("Device {} has a filesystem.".format(self.dev_path))
        if self.dev_path[0] == '/' and disk.has_bluestore_label(self.dev_path):
            raise RuntimeError("Device {} has bluestore signature.".format(self.dev_path))
        return self._device

class ValidRawDevice(ValidDevice):
    def __call__(self, dev_path):
        super().get_device(dev_path)
        return self._format_device(self._is_valid_device())

    def _is_valid_device(self, raise_sys_exit=True):
        out, err, rc = process.call([
	    'ceph-bluestore-tool', 'show-label',
	    '--dev', self.dev_path], verbose_on_failure=False)
        if not rc:
            terminal.info("Raw device {} is already prepared.".format(self.dev_path))
            raise SystemExit(0)
        if disk.blkid(self.dev_path).get('TYPE') == 'crypto_LUKS':
            terminal.info("Raw device {} might already be in use for a dmcrypt OSD, skipping.".format(self.dev_path))
            raise SystemExit(0)
        super()._is_valid_device()
        return self._device

class ValidBatchDevice(ValidDevice):
    def __call__(self, dev_path):
        super().get_device(dev_path)
        return self._format_device(self._is_valid_device())

    def _is_valid_device(self, raise_sys_exit=False):
        super()._is_valid_device()
        if self._device.is_partition:
            raise argparse.ArgumentError(
                None,
                '{} is a partition, please pass '
                'LVs or raw block devices'.format(self.dev_path))
        return self._device


class ValidBatchDataDevice(ValidBatchDevice, ValidDataDevice):
    def __call__(self, dev_path):
        super().get_device(dev_path)
        return self._format_device(self._is_valid_device())

    def _is_valid_device(self):
        # if device is already used by ceph,
        # leave the validation to Batch.get_deployment_layout()
        # This way the idempotency isn't broken (especially when using --osds-per-device)
        for lv in self._device.lvs:
            if lv.tags.get('ceph.type') in ['db', 'wal']:
                return self._device
        if self._device.used_by_ceph:
            return self._device
        super()._is_valid_device(raise_sys_exit=False)
        return self._device


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

import base64
import os
import logging
import re
import json
from ceph_volume import process, conf, terminal
from ceph_volume.util import constants, system
from ceph_volume.util.device import Device
from .prepare import write_keyring
from .disk import lsblk, device_family, get_part_entry_type, _dd_read
from packaging import version
from typing import Any, Dict, List

logger = logging.getLogger(__name__)
mlogger = terminal.MultiLogger(__name__)

def set_dmcrypt_no_workqueue(target_version: str = '2.3.4') -> None:
    """Set `conf.dmcrypt_no_workqueue` to `True` if the installed version
    of `cryptsetup` is greater than or equal to the specified `target_version`.

    Depending on the crypsetup version, `cryptsetup --version` output can be different.
    Eg:

    CentOS Stream9:
    $ cryptsetup --version
    cryptsetup 2.6.0 flags: UDEV BLKID KEYRING FIPS KERNEL_CAPI PWQUALITY

    CentOS Stream8:
    $ cryptsetup --version
    cryptsetup 2.3.7

    Args:
        target_version (str, optional): The minimum version required for setting
            `conf.dmcrypt_no_workqueue` to `True`. Defaults to '2.3.4'.

    Raises:
        RuntimeError: If failed to retrieve the cryptsetup version.
        RuntimeError: If failed to parse the cryptsetup version.
        RuntimeError: If failed to compare the cryptsetup version with the target version.
    """
    command = ["cryptsetup", "--version"]
    out, err, rc = process.call(command)

    # This regex extracts the version number from
    # the `cryptsetup --version` output
    pattern: str = r'(\d+\.?)+'

    if rc:
        raise RuntimeError(f"Can't retrieve cryptsetup version: {err}")

    try:
        cryptsetup_version = re.search(pattern, out[0])

        if cryptsetup_version is None:
            _output: str = "\n".join(out)
            raise RuntimeError('Error while checking cryptsetup version.\n',
                               '`cryptsetup --version` output:\n',
                               f'{_output}')

        if version.parse(cryptsetup_version.group(0)) >= version.parse(target_version):
            conf.dmcrypt_no_workqueue = True
    except IndexError:
        mlogger.debug(f'cryptsetup version check: rc={rc} out={out} err={err}')
        raise RuntimeError("Couldn't check the cryptsetup version.")

def bypass_workqueue(device: str) -> bool:
    return not Device(device).rotational and conf.dmcrypt_no_workqueue

def get_key_size_from_conf():
    """
    Return the osd dmcrypt key size from config file.
    Default is 512.
    """
    default_key_size = '512'
    key_size = conf.ceph.get_safe(
        'osd',
        'osd_dmcrypt_key_size',
        default='512', check_valid=False)

    if key_size not in ['256', '512']:
        logger.warning(("Invalid value set for osd_dmcrypt_key_size ({}). "
                        "Falling back to {}bits".format(key_size, default_key_size)))
        return default_key_size

    return key_size

def create_dmcrypt_key() -> str:
    """
    Create the secret dm-crypt key (KEK) used to encrypt/decrypt the Volume Key.
    """
    random_string = os.urandom(128)
    key = base64.b64encode(random_string).decode('utf-8')
    return key


def luks_format(key: str, device: str) -> None:
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    :param key: dmcrypt secret key, will be used for decrypting
    :param device: Absolute path to device
    """
    command = [
        'cryptsetup',
        '--batch-mode', # do not prompt
        '--key-size',
        get_key_size_from_conf(),
        '--key-file', # misnomer, should be key
        '-',          # because we indicate stdin for the key here
        'luksFormat',
        device,
    ]
    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def plain_open(key, device, mapping):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup in plain mode

    .. note: ceph-disk will require an additional b64decode call for this to work

    :param key: dmcrypt secret key
    :param device: absolute path to device
    :param mapping: mapping name used to correlate device. Usually a UUID
    """
    command = [
        'cryptsetup',
        '--key-file',
        '-',
        '--allow-discards',  # allow discards (aka TRIM) requests for device
        'open',
        device,
        mapping,
        '--type', 'plain',
        '--key-size', '256',
    ]

    if bypass_workqueue(device):
        command.extend(['--perf-no_read_workqueue',
                        '--perf-no_write_workqueue'])

    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def luks_close(mapping: str) -> None:
    """Close a LUKS2 mapper device.

    Args:
        mapping (str): the name of the mapper to be closed.
    """
    command: List[str] = ['cryptsetup',
                          'luksClose',
                          mapping]

    process.call(command,
                 terminal_verbose=True,
                 show_command=True)


def rename_mapper(current: str, new: str) -> None:
    """Rename a mapper

    Args:
        old (str): current name
        new (str): new name
    """

    command: List[str] = [
        'dmsetup',
        'rename',
        current,
        new
    ]

    _, err, rc = process.call(command,
                              terminal_verbose=True,
                              show_command=True)
    if rc:
        raise RuntimeError(f"Can't rename mapper '{current}' to '{new}': {err}")


def luks_open(key: str,
              device: str,
              mapping: str,
              with_tpm: int = 0) -> None:
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    .. note: ceph-disk will require an additional b64decode call for this to work

    :param key: dmcrypt secret key
    :param device: absolute path to device
    :param mapping: mapping name used to correlate device. Usually a UUID
    """
    command: List[str] = []
    if with_tpm:
        command = ['/usr/lib/systemd/systemd-cryptsetup',
                   'attach',
                   mapping,
                   device,
                   '-',
                   'tpm2-device=auto,discard']
        if bypass_workqueue(device):
            command[-1] += ',no-read-workqueue,no-write-workqueue'
    else:
        command = [
            'cryptsetup',
            '--key-size',
            get_key_size_from_conf(),
            '--key-file',
            '-',
            '--allow-discards',  # allow discards (aka TRIM) requests for device
            'luksOpen',
            device,
            mapping,
        ]

        if bypass_workqueue(device):
            command.extend(['--perf-no_read_workqueue',
                            '--perf-no_write_workqueue'])

    process.call(command,
                 run_on_host=with_tpm,
                 stdin=key,
                 terminal_verbose=True,
                 show_command=True)


def dmcrypt_close(mapping, skip_path_check=False):
    """
    Encrypt (close) a device, previously decrypted with cryptsetup

    :param mapping: mapping name or path used to correlate device.
    :param skip_path_check: whether we need path presence validation.
    """
    if not skip_path_check and not os.path.exists(mapping):
        logger.debug('device mapper path does not exist %s' % mapping)
        logger.debug('will skip cryptsetup removal')
        return
    # don't be strict about the remove call, but still warn on the terminal if it fails
    process.run(['cryptsetup', 'remove', mapping], stop_on_error=False)

def get_dmcrypt_key(osd_id, osd_fsid, lockbox_keyring=None):
    """
    Retrieve the dmcrypt (secret) key stored initially on the monitor. The key
    is sent initially with JSON, and the Monitor then mangles the name to
    ``dm-crypt/osd/<fsid>/luks``

    The ``lockbox.keyring`` file is required for this operation, and it is
    assumed it will exist on the path for the same OSD that is being activated.
    To support scanning, it is optionally configurable to a custom location
    (e.g. inside a lockbox partition mounted in a temporary location)
    """
    if lockbox_keyring is None:
        lockbox_keyring = '/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (conf.cluster, osd_id)
    name = 'client.osd-lockbox.%s' % osd_fsid
    config_key = 'dm-crypt/osd/%s/luks' % osd_fsid

    mlogger.info(f'Running ceph config-key get {config_key}')
    stdout, stderr, returncode = process.call(
        [
            'ceph',
            '--cluster', conf.cluster,
            '--name', name,
            '--keyring', lockbox_keyring,
            'config-key',
            'get',
            config_key
        ],
        show_command=True,
        logfile_verbose=False
    )
    if returncode != 0:
        raise RuntimeError('Unable to retrieve dmcrypt secret')
    return ' '.join(stdout).strip()


def write_lockbox_keyring(osd_id, osd_fsid, secret):
    """
    Helper to write the lockbox keyring. This is needed because the bluestore OSD will
    not persist the keyring.

    For bluestore: A tmpfs filesystem is mounted, so the path can get written
    to, but the files are ephemeral, which requires this file to be created
    every time it is activated.
    """
    if os.path.exists('/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (conf.cluster, osd_id)):
        return

    name = 'client.osd-lockbox.%s' % osd_fsid
    write_keyring(
        osd_id,
        secret,
        keyring_name='lockbox.keyring',
        name=name
    )


def status(device):
    """
    Capture the metadata information of a possibly encrypted device, returning
    a dictionary with all the values found (if any).

    An encrypted device will contain information about a device. Example
    successful output looks like::

        $ cryptsetup status /dev/mapper/ed6b5a26-eafe-4cd4-87e3-422ff61e26c4
        /dev/mapper/ed6b5a26-eafe-4cd4-87e3-422ff61e26c4 is active and is in use.
          type:    LUKS1
          cipher:  aes-xts-plain64
          keysize: 256 bits
          device:  /dev/sdc2
          offset:  4096 sectors
          size:    20740063 sectors
          mode:    read/write

    As long as the mapper device is in 'open' state, the ``status`` call will work.

    :param device: Absolute path or UUID of the device mapper
    """
    command = [
        'cryptsetup',
        'status',
        device,
    ]
    out, err, code = process.call(command, show_command=True, verbose_on_failure=False)

    metadata = {}
    if code != 0:
        logger.warning('failed to detect device mapper information')
        return metadata
    for line in out:
        # get rid of lines that might not be useful to construct the report:
        if not line.startswith(' '):
            continue
        try:
            column, value = line.split(': ')
        except ValueError:
            continue
        metadata[column.strip()] = value.strip().strip('"')
    return metadata


def legacy_encrypted(device):
    """
    Detect if a device was encrypted with ceph-disk or not. In the case of
    encrypted devices, include the type of encryption (LUKS, or PLAIN), and
    infer what the lockbox partition is.

    This function assumes that ``device`` will be a partition.
    """
    disk_meta = {}
    if os.path.isdir(device):
        mounts = system.Mounts(paths=True).get_mounts()
        # yes, rebind the device variable here because a directory isn't going
        # to help with parsing
        device = mounts.get(device, [None])[0]
        if not device:
            raise RuntimeError('unable to determine the device mounted at %s' % device)
    metadata = {'encrypted': False, 'type': None, 'lockbox': '', 'device': device}
    # check if the device is online/decrypted first
    active_mapper = status(device)
    if active_mapper:
        # normalize a bit to ensure same values regardless of source
        metadata['type'] = active_mapper['type'].lower().strip('12')  # turn LUKS1 or LUKS2 into luks
        metadata['encrypted'] = True if metadata['type'] in ['plain', 'luks'] else False
        # The true device is now available to this function, so it gets
        # re-assigned here for the lockbox checks to succeed (it is not
        # possible to guess partitions from a device mapper device otherwise
        device = active_mapper.get('device', device)
        metadata['device'] = device
    else:
        uuid = get_part_entry_type(device)
        guid_match = constants.ceph_disk_guids.get(uuid, {})
        encrypted_guid = guid_match.get('encrypted', False)
        if encrypted_guid:
            metadata['encrypted'] = True
            metadata['type'] = guid_match['encryption_type']

    # Lets find the lockbox location now, to do this, we need to find out the
    # parent device name for the device so that we can query all of its
    # associated devices and *then* look for one that has the 'lockbox' label
    # on it. Thanks for being awesome ceph-disk
    if not device == 'tmpfs':
        disk_meta = lsblk(device, abspath=True)
    if not disk_meta:
        return metadata
    parent_device = disk_meta['PKNAME']
    # With the parent device set, we can now look for the lockbox listing associated devices
    devices = [Device(i['NAME']) for i in device_family(parent_device)]
    for d in devices:
        if d.ceph_disk.type == 'lockbox':
            metadata['lockbox'] = d.path
            break
    return metadata

def prepare_dmcrypt(key, device, mapping):
    """
    Helper for devices that are encrypted. The operations needed for
    block, db, wal, or data/journal devices are all the same
    """
    if not device:
        return ''
    # format data device
    luks_format(
        key,
        device
    )
    luks_open(
        key,
        device,
        mapping
    )
    return '/dev/mapper/%s' % mapping


class CephLuks2:
    def __init__(self, device: str) -> None:
        self.device: str = device
        self.osd_fsid: str = ''
        if self.is_ceph_encrypted:
            self.osd_fsid = self.get_osd_fsid()

    @property
    def has_luks2_signature(self) -> bool:
        try:
            return _dd_read(self.device, 4) == 'LUKS'
        except Exception as e:
            raise RuntimeError(e)

    @property
    def is_ceph_encrypted(self) -> bool:
        """Check whether a device is used for a Ceph encrypted OSD

        Args:
            device (str): The path of the device being checked.

        Returns:
            bool: `True` if the device is used by an encrypted Ceph OSD, else `False`.
        """
        result: bool = False
        try:
            result = self.has_luks2_signature and 'ceph_fsid=' in self.get_subsystem()
        except RuntimeError:
            pass
        return result

    def config_luks2(self, config: Dict[str, str]) -> None:
        """Set the subsystem of a LUKS2 device

        Args:
            config (str): The config to apply to the LUKS2 device.

        Raises:
            RuntimeError: If it can't set LUKS2 configuration.
        """
        if not (0 < len(config) <= 2):
            raise RuntimeError(f'Invalid config for LUKS2 device {self.device}')

        valid_keys = ['label', 'subsystem']
        if not all(key in valid_keys for key in config.keys()):
            raise RuntimeError(f'LUKS2 config for device {self.device} can only be "label" and/or "subsystem".')

        command: List[str] = ['cryptsetup', 'config',
                              self.device]
        for k, v in config.items():
                command.extend([f'--{k}', v])
        _, err, rc = process.call(command, verbose_on_failure=False)
        if rc:
            raise RuntimeError(f"Can't set luks2 config to {self.device}:\n{err}")

    def get_label(self) -> str:
        """Get the label of a LUKS2 device

        Args:
            device (str): The device to get the LUKS label from.

        Returns:
            str: The LUKS2 label of the device.
        """
        result: str = ''
        try:
            result = _dd_read(self.device, 48, 24)
        except Exception:
            raise RuntimeError(f"Can't get luks2 label from {self.device}")
        return result

    def get_osd_fsid(self) -> str:
        """Get the osd fsid.

        Returns:
            str: The OSD fsid
        """

        result: str = ''
        try:
            subsystem = self.get_subsystem()
            result = subsystem.split('=')[1]
        except IndexError:
            logger.debug(f"LUKS2 device {self.device} doesn't have ceph osd fsid detail. Please check LUKS2 label for this device.")
        return result

    def get_subsystem(self) -> str:
        """Get the subsystem of a LUKS2 device

        Args:
            device (str): The device to get the LUKS subsystem from.

        Returns:
            str: The LUKS2 subsystem of the device.
        """
        result: str = ''
        try:
            result = _dd_read(self.device, 48, 208)
        except Exception as e:
            raise RuntimeError(f"Can't get luks2 label from {self.device}:\n{e}")
        return result

    def get_json_area(self) -> Dict[str, Any]:
        """Retrieve the LUKS2 JSON configuration area from a given device.

        This function reads the LUKS2 JSON configuration area from the specified 'device'.
        It first checks if the device contains a LUKS2 signature. If not, an empty dictionary
        is returned. If a LUKS2 signature is found, it reads the JSON configuration area
        starting from byte offset 4096 (4 KB) and extracts the configuration data.

        Args:
            device (str): The path to the device.

        Raises:
            RuntimeError: If the LUKS2 JSON area on the device is invalid or cannot be decoded.

        Returns:
            Dict[str, Any]: A dictionary containing the extracted LUKS2 JSON configuration data.
        """
        result: Dict[str, Any] = {}
        try:
            data: str = _dd_read(self.device, 12288, 4096)
            result = json.loads(data)
        except json.JSONDecodeError:
            msg: str = f"LUKS2 json area for device {self.device} seems invalid."
            raise RuntimeError(msg)
        except Exception:
            raise

        return result

    @property
    def is_tpm2_enrolled(self) -> bool:
        """Check if a given device is enrolled with TPM2.

        This function checks if the specified 'device' is enrolled with TPM2.
        It first determines if the device is a LUKS encrypted volume by checking
        its filesystem type using lsblk. If the filesystem type is 'crypto_LUKS',
        it extracts the LUKS2 JSON configuration area from the device using the
        'get_luks2_json_area' function. If the JSON area contains a 'systemd-tpm2'
        token, it indicates that the device is enrolled with TPM2.

        Args:
            device (str): The path to the device.

        Returns:
            bool: True if the device is enrolled with TPM2, False otherwise.
        """
        if lsblk(self.device).get('FSTYPE', '') == 'crypto_LUKS':
            json_area: Dict[str, Any] = self.get_json_area()
            if 'tokens' in json_area.keys():
                for token in json_area['tokens'].keys():
                    if json_area['tokens'][token].get('type', '') == 'systemd-tpm2':
                        return True
        return False

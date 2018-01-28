import base64
import os
import logging
from ceph_volume import process, conf
from .prepare import write_keyring

logger = logging.getLogger(__name__)


def create_dmcrypt_key():
    """
    Create the secret dm-crypt key used to decrypt a device.
    """
    # get the customizable dmcrypt key size (in bits) from ceph.conf fallback
    # to the default of 1024
    dmcrypt_key_size = conf.ceph.get_safe(
        'osd',
        'osd_dmcrypt_key_size',
        default=1024,
    )
    # The size of the key is defined in bits, so we must transform that
    # value to bytes (dividing by 8) because we read in bytes, not bits
    random_string = os.urandom(dmcrypt_key_size / 8)
    key = base64.b64encode(random_string).decode('utf-8')
    return key


def luks_format(key, device):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    :param key: dmcrypt secret key, will be used for decrypting
    :param device: Absolute path to device
    """
    command = [
        'cryptsetup',
        '--batch-mode', # do not prompt
        '--key-file', # misnomer, should be key
        '-',          # because we indicate stdin for the key here
        'luksFormat',
        device,
    ]
    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def luks_open(key, device, mapping):
    """
    Decrypt (open) an encrypted device, previously prepared with cryptsetup

    :param key: dmcrypt secret key
    :param device: absolute path to device
    :param mapping: mapping name used to correlate device. Usually a UUID
    """
    command = [
        'cryptsetup',
        '--key-file',
        '-',
        'luksOpen',
        device,
        mapping,
    ]
    process.call(command, stdin=key, terminal_verbose=True, show_command=True)


def dmcrypt_close(mapping):
    """
    Encrypt (close) a device, previously decrypted with cryptsetup

    :param mapping:
    """
    process.run(['cryptsetup', 'remove', mapping])


def get_dmcrypt_key(osd_id, osd_fsid):
    """
    Retrieve the dmcrypt (secret) key stored initially on the monitor. The key
    is sent initially with JSON, and the Monitor then mangles the name to
    ``dm-crypt/osd/<fsid>/luks``

    The ``lockbox.keyring`` file is required for this operation, and should
    exist on the path for the same OSD that is being activated.
    """
    name = 'client.osd-lockbox.%s' % osd_fsid
    lockbox_keyring = '/var/lib/ceph/osd/%s-%s/lockbox.keyring' % (conf.cluster, osd_id)
    config_key = 'dm-crypt/osd/%s/luks' % osd_fsid

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
        show_command=True
    )
    if returncode != 0:
        raise RuntimeError('Unable to retrieve dmcrypt secret')
    return ' '.join(stdout).strip()


def write_lockbox_keyring(osd_id, osd_fsid, secret):
    """
    Helper to write the lockbox keyring. This is needed because the bluestore OSD will
    not persist the keyring, and it can't be stored in the data device for filestore because
    at the time this is needed, the device is encrypted.

    For bluestore: A tmpfs filesystem is mounted, so the path can get written
    to, but the files are ephemeral, which requires this file to be created
    every time it is activated.
    For filestore: The path for the OSD would exist at this point even if no
    OSD data device is mounted, so the keyring is written to fetch the key, and
    then the data device is mounted on that directory, making the keyring
    "dissapear".
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

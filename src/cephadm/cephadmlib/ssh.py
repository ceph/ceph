# ssh.py - functions related to using/testing ssh connections from cephadm

import logging
import os
import pwd
import shutil
import tempfile
import uuid

from typing import Tuple

from cephadmlib.call_wrappers import call
from cephadmlib.constants import DEFAULT_MODE
from cephadmlib.context import CephadmContext
from cephadmlib.file_utils import makedirs, pathify
from cephadmlib.exceptions import Error
from cephadmlib.net_utils import get_hostname

logger = logging.getLogger()


def get_ssh_vars(ssh_user: str) -> Tuple[int, int, str]:
    try:
        s_pwd = pwd.getpwnam(ssh_user)
    except KeyError:
        raise Error('Cannot find uid/gid for ssh-user: %s' % (ssh_user))

    ssh_uid = s_pwd.pw_uid
    ssh_gid = s_pwd.pw_gid
    ssh_dir = os.path.join(s_pwd.pw_dir, '.ssh')
    return ssh_uid, ssh_gid, ssh_dir


def authorize_ssh_key(ssh_pub_key: str, ssh_user: str) -> bool:
    """Authorize the public key for the provided ssh user"""

    def key_in_file(path: str, key: str) -> bool:
        if not os.path.exists(path):
            return False
        with open(path) as f:
            lines = f.readlines()
            for line in lines:
                if line.strip() == key.strip():
                    return True
        return False

    logger.info(f'Adding key to {ssh_user}@localhost authorized_keys...')
    if ssh_pub_key is None or ssh_pub_key.isspace():
        raise Error('Trying to authorize an empty ssh key')

    ssh_pub_key = ssh_pub_key.strip()
    ssh_uid, ssh_gid, ssh_dir = get_ssh_vars(ssh_user)
    if not os.path.exists(ssh_dir):
        makedirs(ssh_dir, ssh_uid, ssh_gid, 0o700)

    auth_keys_file = '%s/authorized_keys' % ssh_dir
    if key_in_file(auth_keys_file, ssh_pub_key):
        logger.info(f'key already in {ssh_user}@localhost authorized_keys...')
        return False

    add_newline = False
    if os.path.exists(auth_keys_file):
        with open(auth_keys_file, 'r') as f:
            f.seek(0, os.SEEK_END)
            if f.tell() > 0:
                f.seek(f.tell() - 1, os.SEEK_SET)  # go to last char
                if f.read() != '\n':
                    add_newline = True

    with open(auth_keys_file, 'a') as f:
        os.fchown(f.fileno(), ssh_uid, ssh_gid)  # just in case we created it
        os.fchmod(f.fileno(), DEFAULT_MODE)  # just in case we created it
        if add_newline:
            f.write('\n')
        f.write(ssh_pub_key + '\n')

    return True


def revoke_ssh_key(key: str, ssh_user: str) -> None:
    """Revoke the public key authorization for the ssh user"""
    ssh_uid, ssh_gid, ssh_dir = get_ssh_vars(ssh_user)
    auth_keys_file = '%s/authorized_keys' % ssh_dir
    deleted = False
    if os.path.exists(auth_keys_file):
        with open(auth_keys_file, 'r') as f:
            lines = f.readlines()
        _, filename = tempfile.mkstemp()
        with open(filename, 'w') as f:
            os.fchown(f.fileno(), ssh_uid, ssh_gid)
            os.fchmod(
                f.fileno(), DEFAULT_MODE
            )  # secure access to the keys file
            for line in lines:
                if line.strip() == key.strip():
                    deleted = True
                else:
                    f.write(line)

    if deleted:
        shutil.move(filename, auth_keys_file)
    else:
        logger.warning('Cannot find the ssh key to be deleted')


def check_ssh_connectivity(ctx: CephadmContext) -> None:
    def cmd_is_available(cmd: str) -> bool:
        if shutil.which(cmd) is None:
            logger.warning(f'Command not found: {cmd}')
            return False
        return True

    if not cmd_is_available('ssh') or not cmd_is_available('ssh-keygen'):
        logger.warning('Cannot check ssh connectivity. Skipping...')
        return

    ssh_priv_key_path = ''
    ssh_pub_key_path = ''
    ssh_signed_cert_path = ''
    if ctx.ssh_private_key and ctx.ssh_public_key:
        # let's use the keys provided by the user
        ssh_priv_key_path = pathify(ctx.ssh_private_key.name)
        ssh_pub_key_path = pathify(ctx.ssh_public_key.name)
    elif ctx.ssh_private_key and ctx.ssh_signed_cert:
        # CA signed keys use case
        ssh_priv_key_path = pathify(ctx.ssh_private_key.name)
        ssh_signed_cert_path = pathify(ctx.ssh_signed_cert.name)
    else:
        # no custom keys, let's generate some random keys just for this check
        ssh_priv_key_path = f'/tmp/ssh_key_{uuid.uuid1()}'
        ssh_pub_key_path = f'{ssh_priv_key_path}.pub'
        ssh_key_gen_cmd = [
            'ssh-keygen',
            '-q',
            '-t',
            'rsa',
            '-N',
            '',
            '-C',
            '',
            '-f',
            ssh_priv_key_path,
        ]
        _, _, code = call(ctx, ssh_key_gen_cmd)
        if code != 0:
            logger.warning('Cannot generate keys to check ssh connectivity.')
            return

    if ssh_signed_cert_path:
        logger.info(
            'Verification for CA signed keys authentication not implemented. Skipping ...'
        )
    elif ssh_pub_key_path:
        logger.info(
            'Verifying ssh connectivity using standard pubkey authentication ...'
        )
        with open(ssh_pub_key_path, 'r') as f:
            key = f.read().strip()
        new_key = authorize_ssh_key(key, ctx.ssh_user)
        ssh_cfg_file_arg = (
            ['-F', pathify(ctx.ssh_config.name)] if ctx.ssh_config else []
        )
        _, _, code = call(
            ctx,
            [
                'ssh',
                '-o StrictHostKeyChecking=no',
                *ssh_cfg_file_arg,
                '-i',
                ssh_priv_key_path,
                '-o PasswordAuthentication=no',
                f'{ctx.ssh_user}@{get_hostname()}',
                'sudo echo',
            ],
        )

        # we only remove the key if it's a new one. In case the user has provided
        # some already existing key then we don't alter authorized_keys file
        if new_key:
            revoke_ssh_key(key, ctx.ssh_user)

        pub_key_msg = (
            '- The public key file configured by --ssh-public-key is valid\n'
            if ctx.ssh_public_key
            else ''
        )
        prv_key_msg = (
            '- The private key file configured by --ssh-private-key is valid\n'
            if ctx.ssh_private_key
            else ''
        )
        ssh_cfg_msg = (
            '- The ssh configuration file configured by --ssh-config is valid\n'
            if ctx.ssh_config
            else ''
        )
        err_msg = f"""
** Please verify your user's ssh configuration and make sure:
- User {ctx.ssh_user} must have passwordless sudo access
{pub_key_msg}{prv_key_msg}{ssh_cfg_msg}
"""
        if code != 0:
            raise Error(err_msg)

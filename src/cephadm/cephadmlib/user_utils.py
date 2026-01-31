# user_utils.py - user management utility functions

import logging
import os
import pwd
from typing import Tuple, Optional

from .call_wrappers import call, CallVerbosity
from .context import CephadmContext
from .exceptions import Error, TimeoutExpired
from .exe_utils import find_program
from .file_utils import write_new
from .ssh import authorize_ssh_key
from .packagers import create_packager

logger = logging.getLogger()


def validate_user_exists(username: str) -> Tuple[int, int, str]:
    """Validate that a user exists and return their uid, gid, and home directory.
    Args:
        username: The username to validate
    Returns:
        Tuple of (uid, gid, home_directory)
    Raises:
        Error: If the user does not exist
    """
    try:
        pwd_entry = pwd.getpwnam(username)
        return pwd_entry.pw_uid, pwd_entry.pw_gid, pwd_entry.pw_dir
    except KeyError:
        raise Error(
            f'User {username} does not exist on this host. '
            f'Please create the user first: useradd -m -s /bin/bash {username}'
        )


def setup_sudoers(
    ctx: CephadmContext, username: str, sudoers_content: Optional[str] = None
) -> None:
    """Setup sudoers for a user with custom or default permissions."""
    sudoers_file = f'/etc/sudoers.d/{username}'
    if sudoers_content is None:
        sudoers_content = f'{username} ALL=(ALL) NOPASSWD: ALL\n'

    # Ensure content ends with newline
    if not sudoers_content.endswith('\n'):
        sudoers_content += '\n'

    logger.info('Setting up sudoers for user %s', username)
    try:
        # Write sudoers file with proper permissions
        with write_new(sudoers_file, owner=(0, 0), perms=0o440) as fh:
            fh.write(sudoers_content)

        # Validate sudoers syntax
        visudo_cmd = find_program('visudo')
        _out, _err, code = call(
            ctx,
            [visudo_cmd, '-c', '-f', sudoers_file],
            verbosity=CallVerbosity.DEBUG,
        )
        if code != 0:
            try:
                os.remove(sudoers_file)
            except OSError:
                pass
            raise Error(f'Invalid sudoers syntax: {_err}')
        logger.info('Successfully configured sudoers for user %s', username)
    except Error:
        raise
    except Exception as e:
        msg = f'Failed to setup sudoers for user {username}: {e}'
        logger.exception(msg)
        raise Error(msg)


def setup_sudoers_restricted(
    ctx: CephadmContext, username: str, allowed_command: str
) -> None:
    """Setup sudoers with restricted permissions for a specific command.
    Args:
        ctx: CephadmContext
        username: Username to configure sudoers for
        allowed_command: Full path to the command that user can run with sudo
    """
    sudoers_content = f'{username} ALL=(ALL) NOPASSWD: {allowed_command}'
    setup_sudoers(ctx, username, sudoers_content)


def setup_ssh_user(
    ctx: CephadmContext, username: str, ssh_pub_key: str
) -> None:
    """Setup SSH user with passwordless sudo and SSH key authorization.
    This function performs the following operations:
    1. Validates that the user exists on the system
    2. Sets up passwordless sudo for the user (skipped for root)
    3. Authorizes the SSH public key for the user
    """
    if not ssh_pub_key or ssh_pub_key.isspace():
        raise Error('SSH public key is required and cannot be empty')

    logger.info('Setting up SSH user %s on this host', username)

    # Validate user exists (will raise Error if not)
    validate_user_exists(username)

    # Setup sudoers (skip for root user)
    if username != 'root':
        setup_sudoers(ctx, username)
    else:
        logger.debug('Skipping sudoers setup for root user')

    # Setup SSH key using existing function from ssh.py
    try:
        authorize_ssh_key(ssh_pub_key, username)
        logger.info('Successfully authorized SSH key for user %s', username)
    except Exception as e:
        msg = f'Failed to authorize SSH key for user {username}: {e}'
        logger.exception(msg)
        raise Error(msg)

    logger.info('Successfully configured SSH user %s on this host', username)


def _check_cephadm_version(ctx: CephadmContext) -> Tuple[Optional[str], bool]:
    """Check if cephadm is installed and return its version.
    Returns:
        Tuple of (version_string_or_none, is_available)
    """
    try:
        result_stdout, result_stderr, result_code = call(
            ctx, ['cephadm', 'version']
        )
        if result_code == 0:
            version = result_stdout.strip()
            logger.info('Found installed cephadm: %s', version)
            return version, True
        else:
            logger.info('cephadm command failed, package not available')
            return None, False
    except (TimeoutExpired, FileNotFoundError) as e:
        logger.exception('cephadm not found: %s', e)
        return None, False


def install_or_upgrade_cephadm(
    ctx: CephadmContext, requested_version: Optional[str] = None
) -> Tuple[bool, str]:
    """Install or upgrade cephadm package to match a specific version.
    Returns:
        Tuple of (success, message)
    """
    try:
        current_version, is_available = _check_cephadm_version(ctx)
        if is_available and current_version:
            if requested_version and requested_version not in current_version:
                logger.info(
                    'Version mismatch: installed=%s, requested=%s',
                    current_version,
                    requested_version,
                )
                needs_install = True
            elif requested_version:
                logger.debug(
                    'cephadm version %s already installed', requested_version
                )
                return True, f'cephadm {requested_version} already installed'
            else:
                logger.info(
                    'cephadm installed, no specific version requested'
                )
                return True, f'cephadm already installed: {current_version}'
        else:
            needs_install = True

        # Install or upgrade if needed
        if needs_install:
            logger.info('Installing/upgrading cephadm package...')
            pkg = create_packager(ctx, version=requested_version)
            pkg.install(['cephadm'])

            # Verify installation
            new_version, install_success = _check_cephadm_version(ctx)
            if install_success and new_version:
                logger.info(f'Successfully installed cephadm: {new_version}')
                # Check version match if requested
                if requested_version and requested_version not in new_version:
                    logger.warning(
                        'Installed version %s does not match requested %s',
                        new_version,
                        requested_version,
                    )
                    return (
                        True,
                        f'cephadm installed/upgraded: {new_version} (requested {requested_version} not available)',
                    )
                return True, f'cephadm installed/upgraded: {new_version}'
            else:
                logger.warning(
                    'cephadm package installed but verification failed'
                )
                return (
                    True,
                    'cephadm package installed (verification unavailable)',
                )
        return True, 'cephadm operation completed'
    except Exception as e:
        error_msg = f'Failed to install/upgrade cephadm: {e}'
        logger.exception(error_msg)
        return False, error_msg

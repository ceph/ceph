#!/usr/bin/env python3
"""
Cephadm Invoker - a wrapper intended for executing cephadm commands with limited sudo priviliges

This script validates the cephadm binary hash before execution and provides
a secure way to run cephadm commands and deploy binaries.

Usage:
    cephadm_invoker.py run <binary> [args...]
    cephadm_invoker.py deploy_binary <temp_file> <final_path>
    cephadm_invoker.py check_binary <cephadm_binary_path>

Exit Codes:
    0: Success
    1: General error (file not found, permission issues, etc.)
    2: Binary hash mismatch or file doesn't exist (triggers redeployment)
    126: Permission denied during execution
"""

import argparse
import datetime
import fcntl
import hashlib
import logging
import logging.handlers
import os
import pathlib
import shutil
import sys
from typing import List, Optional, Tuple, IO


logger = logging.getLogger('cephadm_invoker')


def setup_logging() -> None:
    """
    Configure logging to output to both stdout and syslog.
    If syslog is unavailable, continues with console logging only.
    """
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    try:
        syslog_handler = logging.handlers.SysLogHandler(address='/dev/log')
        syslog_handler.setLevel(logging.INFO)
        syslog_handler.setFormatter(formatter)
        logger.addHandler(syslog_handler)
    except (OSError, ImportError):
        pass


def calculate_hash(content: bytes) -> str:
    """
    Calculate SHA256 hash of binary content.
    """
    return hashlib.sha256(content).hexdigest()


def disable_cloexec(fh: IO[bytes]) -> None:
    """
    Disable the CLOEXEC flag on a file descriptor so it remains open across exec().
    """
    fd = fh.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    fcntl.fcntl(fd, fcntl.F_SETFD, flags & ~fcntl.FD_CLOEXEC)


def extract_hash_from_path(path: str) -> Optional[str]:
    """
    Extract the expected hash from cephadm binary path.
    Expected path format: /var/lib/ceph/{fsid}/cephadm.{hash}
    """
    basename = pathlib.Path(path).name
    if basename.startswith('cephadm.') and len(basename) > 8:
        return basename[8:]  # Extract hash after 'cephadm.' prefix
    return None


def verify_binary_hash(fh: IO[bytes], expected_hash: str) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Verify that the cephadm binary hash matches the expected hash.
    Returns:
        Tuple of (is_valid, expected_hash, actual_hash)
    """
    try:
        content = fh.read()
        actual_hash = calculate_hash(content)

        is_valid = actual_hash == expected_hash
        return (is_valid, expected_hash, actual_hash)

    except (IOError, OSError) as e:
        logger.error('Error reading cephadm binary: %s', e)
        return (False, None, None)


def execute_cephadm(fd: int, args: List[str]) -> None:
    """
    Execute cephadm binary using os.execve with file descriptor (replaces current process).
    Uses file descriptor to prevent race conditions between verification and execution.
    Exit codes:
        2: Binary not found (triggers redeployment)
        126: Permission denied
        1: OS-specific error code
    """
    try:
        os.execve(fd, args, os.environ)
    except FileNotFoundError:
        logger.error('Cephadm binary file descriptor %d not found', fd)
        sys.exit(2)
    except PermissionError:
        logger.error('Permission denied executing cephadm with fd: %d', fd)
        sys.exit(126)
    except OSError as e:
        logger.error(
            'Failed to execute cephadm (fd=%d): errno=%s (%s)',
            fd,
            e.errno,
            e.strerror,
        )
        sys.exit(1)


def verify_and_execute_cephadm_binary(binary_path: str, cephadm_args: List[str]) -> None:
    """
    verify, and execute cephadm binary with hash validation.
    """
    fh = None
    try:
        expected_hash = extract_hash_from_path(binary_path)
        if not expected_hash:
            logger.error('Could not extract hash from binary path: %s', binary_path)
            sys.exit(1)

        fh = open(binary_path, 'rb')

        is_valid, expected_hash, actual_hash = verify_binary_hash(fh, expected_hash)
        if is_valid:
            # Disable CLOEXEC so the FD stays open across exec
            disable_cloexec(fh)
            execute_cephadm(fh.fileno(), [binary_path] + cephadm_args)
            sys.exit(0)

        if actual_hash is None:
            logger.error('Failed to read or hash binary at: %s', binary_path)
            sys.exit(2)
        else:
            # Hash mismatch - backup the corrupted binary
            logger.error('Binary hash mismatch at: %s', binary_path)
            logger.error('Expected hash (from filename): %s', expected_hash)
            logger.error('Actual hash (calculated): %s', actual_hash)

            try:
                timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                backup_path = f'{binary_path}_hash_mismatch_{timestamp}'
                os.rename(binary_path, backup_path)
                logger.info('Moved corrupted binary to: %s', backup_path)
            except OSError as e:
                logger.error('Could not backup corrupted binary: %s', e)

            logger.info('Returning exit code 2 to trigger binary redeployment')
            sys.exit(2)

    except (IOError, OSError) as e:
        logger.error('Error opening cephadm binary at %s: %s', binary_path, e)
        sys.exit(2)
    finally:
        if fh is not None:
            try:
                fh.close()
            except OSError:
                pass


def command_run(args: argparse.Namespace) -> int:
    """
    Run cephadm binary with arguments after hash verification.
    """
    verify_and_execute_cephadm_binary(args.binary, args.args)
    return 0


def command_deploy_binary(args: argparse.Namespace) -> int:
    """
    Deploy cephadm binary from temporary file to final location.
    Performs deployment with proper permissions and directory creation:
    1. Validates temp file exists
    2. Creates destination directory if needed
    3. Sets executable permissions (0o755)
    4. Moves file to final location atomically with locking
    """
    temp_file = args.temp_file
    final_path = args.final_path

    if not os.path.isfile(temp_file):
        logger.error('Temporary file does not exist: %s', temp_file)
        return 1

    final_dir = pathlib.Path(final_path).parent
    try:
        final_dir.mkdir(parents=True, exist_ok=True)
        logger.debug('Created destination directory: %s', final_dir)
    except OSError as e:
        logger.error('Failed to create directory %s: %s', final_dir, e)
        return 1

    try:
        os.chmod(temp_file, 0o755)
        logger.debug('Set executable permissions (0o755) on: %s', temp_file)
    except OSError as e:
        logger.error('Failed to set permissions on %s: %s', temp_file, e)
        return 1

    lock_file = f'{final_path}.lock'
    lock_fd = None
    try:
        # Create lock file and acquire exclusive lock
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_RDWR, 0o644)
        fcntl.flock(lock_fd, fcntl.LOCK_EX)

        if os.path.exists(final_path):
            logger.info('Binary already exists at %s, skipping deployment', final_path)
            return 0

        shutil.move(temp_file, final_path)
        logger.info('Successfully deployed cephadm binary to: %s', final_path)
        return 0

    except OSError as e:
        logger.error('Failed to deploy %s to %s: %s', temp_file, final_path, e)
        return 1
    finally:
        if lock_fd is not None:
            try:
                os.close(lock_fd)
            except OSError:
                pass
        try:
            os.unlink(lock_file)
        except OSError:
            pass


def command_check_binary(args: argparse.Namespace) -> int:
    """
    Check if a file exists.
    Exit codes:
        0: File exists
        2: File does not exist (signals need for deployment)
    """
    if pathlib.Path(args.cephadm_binary_path).is_file():
        logger.debug('File exists: %s', args.cephadm_binary_path)
        return 0
    else:
        logger.debug('File does not exist: %s', args.cephadm_binary_path)
        return 2


def create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Cephadm Invoker - A secure wrapper for executing cephadm commands',
        prog='cephadm_invoker.py'
    )
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    run_parser = subparsers.add_parser(
        'run',
        help='Run cephadm binary with arguments after hash verification'
    )
    run_parser.add_argument(
        'binary',
        help='Path to the cephadm binary (must include hash in filename)'
    )
    run_parser.add_argument(
        'args',
        nargs=argparse.REMAINDER,
        help='Arguments to pass to cephadm'
    )
    run_parser.set_defaults(func=command_run)

    deploy_parser = subparsers.add_parser(
        'deploy_binary',
        help='Deploy cephadm binary from temp file to final location'
    )
    deploy_parser.add_argument(
        'temp_file',
        help='Path to temporary cephadm binary file'
    )
    deploy_parser.add_argument(
        'final_path',
        help='Final destination path for cephadm binary'
    )
    deploy_parser.set_defaults(func=command_deploy_binary)

    check_parser = subparsers.add_parser(
        'check_binary',
        help='Check if a cephadm binary exists (exit 0 if exists, 2 if not)'
    )
    check_parser.add_argument(
        'cephadm_binary_path',
        help='Path to cephadm binary to check'
    )
    check_parser.set_defaults(func=command_check_binary)

    return parser


def main() -> int:
    """
    Main entry point - parses arguments and dispatches to appropriate handler.
    """
    setup_logging()
    parser = create_argument_parser()
    args = parser.parse_args()

    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    try:
        return args.func(args)
    except SystemExit:
        raise
    except Exception as e:
        logger.error('Error executing command %s: %s', args.command, e)
        return 1


if __name__ == '__main__':
    sys.exit(main())

"""Utilities for RGW integration with SMB."""

from typing import Protocol, Tuple, runtime_checkable

import json
import logging

log = logging.getLogger(__name__)


@runtime_checkable
class ToolExecer(Protocol):
    """Protocol for executing tools (e.g., radosgw-admin)."""

    def tool_exec(self, cmd: list[str]) -> Tuple[int, str, str]:
        """Execute a tool and return (return_code, stdout, stderr)."""
        ...


def _fetch_bucket_stats(executor: ToolExecer, bucket: str) -> dict:
    """
    Fetch RGW bucket statistics.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
    Returns:
        Parsed JSON bucket stats dictionary
    Raises:
        ValueError: If bucket stats cannot be fetched or parsed
    """
    log.debug(f"Fetching stats for bucket {bucket}")
    ret, out, err = executor.tool_exec(
        ['radosgw-admin', 'bucket', 'stats', '--bucket', bucket]
    )
    if ret:
        raise ValueError(f'Failed to fetch stats for bucket {bucket}: {err}')

    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise ValueError(f'Failed to parse bucket stats for {bucket}: {e}')


def _get_rgw_owner(executor: ToolExecer, bucket: str) -> str:
    """
    Fetch RGW user bucket owner.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
    Returns:
        user_id: RGW user ID that owns bucket.
    Raises:
        ValueError: If bucket owner cannot be determined
    """

    try:
        stats = _fetch_bucket_stats(executor, bucket)
        user_id = stats.get('owner', '')
        if not user_id:
            raise ValueError(f'No owner found for bucket {bucket}')

        log.debug(f"Bucket {bucket} is owned by user {user_id}")
        return user_id

    except ValueError:
        raise


def _get_rgw_creds(executor: ToolExecer, user_id: str) -> Tuple[str, str]:
    """
    Fetch RGW user credentials.
    Args:
        executor: Any object with tool_exec() method
        user_id: The RGW user ID
    Returns:
        Tuple of (access_key_id, secret_access_key)
    Raises:
        ValueError: If credentials cannot be fetched
    """
    log.debug(f"Fetching credentials for user {user_id}")
    ret, out, err = executor.tool_exec(
        ['radosgw-admin', 'user', 'info', '--uid', user_id]
    )
    if ret:
        raise ValueError(
            f'Failed to fetch credentials for user {user_id}: {err}'
        )

    try:
        j = json.loads(out)
        keys = j.get('keys', [])
        if not keys:
            raise ValueError(f'No keys found for user {user_id}')

        access_key = keys[0].get('access_key', '')
        secret_key = keys[0].get('secret_key', '')

        if not access_key or not secret_key:
            raise ValueError(
                f'Invalid credentials for user {user_id}: '
                f'access_key={bool(access_key)}, secret_key={bool(secret_key)}'
            )

        log.debug(f"Successfully fetched credentials for user {user_id}")
        return access_key, secret_key

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        raise ValueError(f'Failed to parse user info for {user_id}: {e}')


def fetch_rgw_credentials(
    executor: ToolExecer,
    bucket: str,
    user_id: str = '',
) -> Tuple[str, str, str]:
    """
    Fetch RGW user credentials for a bucket.

    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
        user_id: Optional RGW user ID. If not provided, fetches bucket owner.

    Returns:
        Tuple of (user_id, access_key_id, secret_access_key)

    Raises:
        ValueError: If bucket owner cannot be determined or credentials not found
    """
    if not user_id:
        user_id = _get_rgw_owner(executor, bucket)

    access_key, secret_key = _get_rgw_creds(executor, user_id)
    return user_id, access_key, secret_key


def validate_rgw_bucket(executor: ToolExecer, bucket: str) -> bool:
    """
    Validate that an RGW bucket exists.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
    Returns:
        True if bucket exists, False otherwise
    """
    log.debug(f"Validating bucket {bucket}")
    try:
        stats = _fetch_bucket_stats(executor, bucket)
        # If we can parse the output and it has an owner, bucket exists
        return bool(stats.get('owner'))
    except ValueError as e:
        log.debug(f"Bucket {bucket} validation failed: {e}")
        return False

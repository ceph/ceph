"""Utilities for RGW integration with SMB.

This module encapsulates all tenant-aware RGW operations. The tenant is extracted
from user_id format "tenant$user" and used internally in radosgw-admin commands.
Callers should pass user_id with or without tenant prefix; this module handles
tenant extraction and returns user_id in the same format as input.
"""

from typing import Optional, Protocol, Tuple, runtime_checkable

import json
import logging

log = logging.getLogger(__name__)


@runtime_checkable
class ToolExecer(Protocol):
    """Protocol for executing tools (e.g., radosgw-admin)."""

    def tool_exec(self, cmd: list[str]) -> Tuple[int, str, str]:
        """Execute a tool and return (return_code, stdout, stderr)."""
        ...


def _split_tenant_user_id(user_id: str) -> Tuple[str, str]:
    """
    Split tenant name from user_id if present.
    Args:
        user_id: RGW user ID in format "tenant$user" or just "user"
    Returns:
        Tuple of (tenant_name, user_id_without_tenant)
        Returns empty string for tenant if no tenant prefix is present
    """
    if '$' in user_id:
        parts = user_id.split('$', 1)
        return parts[0], parts[1]
    return "", user_id


def _format_bucket_name(bucket: str, tenant: Optional[str] = None) -> str:
    """
    Format bucket name with tenant prefix if tenant is specified.
    Args:
        bucket: The bucket name
        tenant: Optional tenant name
    Returns:
        Formatted bucket name (tenant/bucket or bucket)
    """
    if tenant:
        return f"{tenant}/{bucket}"
    return bucket


def _fetch_bucket_stats(
    executor: ToolExecer, bucket: str, tenant: Optional[str] = None
) -> dict:
    """
    Fetch RGW bucket statistics.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name (should not include tenant prefix)
        tenant: Optional tenant name
    Returns:
        Parsed JSON bucket stats dictionary
    Raises:
        ValueError: If bucket stats cannot be fetched or parsed
    """
    formatted_bucket = _format_bucket_name(bucket, tenant)
    log.debug(f"Fetching stats for bucket {formatted_bucket}")
    ret, out, err = executor.tool_exec(
        ['radosgw-admin', 'bucket', 'stats', '--bucket', formatted_bucket]
    )
    if ret:
        error_msg = (
            f'Failed to fetch stats for bucket {formatted_bucket}: {err}'
        )
        # Provide helpful hint if bucket not found and no tenant was specified
        if tenant is None:
            error_msg += ' If this is a tenant-aware bucket, please provide user_id parameter to identify the correct bucket location.'
        raise ValueError(error_msg)

    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise ValueError(
            f'Failed to parse bucket stats for {formatted_bucket}: {e}'
        )


def _get_rgw_owner(
    executor: ToolExecer, bucket: str, tenant: Optional[str] = None
) -> str:
    """
    Fetch RGW user bucket owner.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
        tenant: Optional tenant name
    Returns:
        user_id: RGW user ID that owns bucket (may include tenant prefix if tenant-aware)
    Raises:
        ValueError: If bucket owner cannot be determined
    """

    try:
        stats = _fetch_bucket_stats(executor, bucket, tenant)
        user_id = stats.get('owner', '')
        if not user_id:
            formatted_bucket = _format_bucket_name(bucket, tenant)
            raise ValueError(f'No owner found for bucket {formatted_bucket}')

        formatted_bucket = _format_bucket_name(bucket, tenant)
        log.debug(f"Bucket {formatted_bucket} is owned by user {user_id}")
        return user_id

    except ValueError:
        raise


def _get_rgw_creds(
    executor: ToolExecer, user_id: str, tenant: Optional[str] = None
) -> Tuple[str, str]:
    """
    Fetch RGW user credentials.
    Args:
        executor: Any object with tool_exec() method
        user_id: The RGW user ID (without tenant prefix)
        tenant: Optional tenant name for multi-tenant setup
    Returns:
        Tuple of (access_key_id, secret_access_key)
    Raises:
        ValueError: If credentials cannot be fetched
    """
    log.debug(f"Fetching credentials for user {user_id}")
    cmd = ['radosgw-admin', 'user', 'info', '--uid', user_id]
    if tenant:
        cmd.extend(['--tenant', tenant])

    ret, out, err = executor.tool_exec(cmd)
    if ret:
        error_msg = f'Failed to fetch credentials for user {user_id}'
        if tenant:
            error_msg += f' in tenant {tenant}'
        error_msg += f': {err}'
        raise ValueError(error_msg)

    try:
        j = json.loads(out)
        keys = j.get('keys', [])
        if not keys:
            error_msg = f'No keys found for user {user_id}'
            if tenant:
                error_msg += f' in tenant {tenant}'
            raise ValueError(error_msg)

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

    This is the primary public interface for tenant-aware credential fetching.
    All tenant logic is encapsulated here - callers should not be aware of tenant.

    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
        user_id: Optional RGW user ID. Can be in format "tenant$user" or just "user".
                 If normal user not provided, fetches bucket owner and extract user.
                 If tenant user not provided, report error.

    Returns:
        Tuple of (user_id, access_key_id, secret_access_key)
        user_id is returned in same format as input (with tenant$ prefix if it came in that way)

    Raises:
        ValueError: If bucket is invalid or bucket owner cannot be determined
        or credentials not found
    """
    # Validate that bucket name doesn't contain "/"
    if '/' in bucket:
        raise ValueError(
            f"Invalid bucket name '{bucket}': bucket name should not contain /."
        )

    if not user_id:
        user_id = _get_rgw_owner(executor, bucket, None)

    # Split tenant and user_id_only from user_id
    tenant, user_id_only = _split_tenant_user_id(user_id)

    access_key, secret_key = _get_rgw_creds(executor, user_id_only, tenant)

    log.debug(f"Validating fetch_rgw_credentials user_id {user_id}")
    return user_id, access_key, secret_key


def validate_rgw_bucket(
    executor: ToolExecer, bucket: str, user_id: str
) -> bool:
    """
    Validate that an RGW bucket exists.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
        user_id: RGW user ID (format: "tenant$user" or "user")
    Returns:
        True if bucket exists, False otherwise
    """
    # Fetch tenant from user_id if present
    tenant, _ = _split_tenant_user_id(user_id)

    formatted_bucket = _format_bucket_name(bucket, tenant)
    log.debug(f"Validating bucket {formatted_bucket}")
    try:
        stats = _fetch_bucket_stats(executor, bucket, tenant)
        # If we can parse the output and it has an owner, bucket exists
        return bool(stats.get('owner'))
    except ValueError as e:
        log.debug(f"Bucket {formatted_bucket} validation failed: {e}")
        return False

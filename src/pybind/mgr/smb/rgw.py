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


def _split_tenant_resource(
    name: str, separator: str = '$'
) -> Tuple[str, str]:
    """Split tenant and resource name by separator.

    Validates that both tenant and resource parts are non-empty and non-whitespace.

    Args:
        name: The full name potentially containing tenant prefix (e.g., "tenantA$user1")
        separator: The separator character ('$' for users, '/' for buckets)

    Returns:
        Tuple of (tenant, resource_name). If separator not found, returns ('', name).
        If name is empty, returns ('', '').

    Raises:
        ValueError: If separator found but tenant or resource is empty/whitespace (invalid format)
    """
    if not name or separator not in name:
        return '', name

    parts = name.split(separator, 1)
    tenant, resource = parts[0], parts[1]

    # Validate both parts are non-empty and non-whitespace
    if (
        not tenant
        or not tenant.strip()
        or not resource
        or not resource.strip()
    ):
        raise ValueError(
            f"Invalid {separator!r}-separated format in '{name}': "
            f"tenant={tenant!r}, resource={resource!r} "
            f"(both must be non-empty and non-whitespace)"
        )

    return tenant, resource


def _find_bucket_with_tenant(executor: ToolExecer, bucket: str) -> str:
    """
    Find the full bucket name including tenant prefix if it exists.
    Returns:
        Full bucket name with tenant prefix (e.g., "tenantA/bucket") or
        original bucket name if no tenant prefix found
    Raises:
        ValueError: If bucket list cannot be fetched or parsed
    """
    log.debug(f"Searching for bucket {bucket} in bucket list")
    ret, out, err = executor.tool_exec(['radosgw-admin', 'bucket', 'list'])
    if ret:
        raise ValueError(f'Failed to list buckets: {err}')
    try:
        bucket_list = json.loads(out)
        if not isinstance(bucket_list, list):
            raise ValueError(
                f'Unexpected bucket list format: {type(bucket_list)}'
            )
        # Search for exact match first
        if bucket in bucket_list:
            log.debug(f"Found exact match for bucket {bucket}")
            return bucket
        # Search for tenant-prefixed match (tenant/bucket)
        for full_bucket_name in bucket_list:
            if isinstance(full_bucket_name, str) and '/' in full_bucket_name:
                try:
                    tenant, resource_bucket = _split_tenant_resource(
                        full_bucket_name, '/'
                    )
                    if resource_bucket == bucket:
                        log.debug(
                            f"Found tenant-prefixed bucket: {full_bucket_name}"
                        )
                        return full_bucket_name
                except ValueError:
                    log.debug(
                        f"Skipping malformed bucket name: {full_bucket_name}"
                    )
                    continue
        # Bucket not found in list
        log.debug(f"Bucket {bucket} not found in bucket list")
        return bucket  # Return original name to let caller handle the error
    except json.JSONDecodeError as e:
        raise ValueError(f'Failed to parse bucket list: {e}')


def _fetch_bucket_stats(executor: ToolExecer, bucket: str) -> dict:
    """
    Fetch RGW bucket statistics.
    Handles buckets with tenant prefixes (e.g., "tenantA/bucket").

    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name (with or without tenant prefix)
    Returns:
        Parsed JSON bucket stats dictionary
    Raises:
        ValueError: If bucket stats cannot be fetched or parsed
    """
    log.debug(f"Fetching stats for bucket {bucket}")
    # Try to resolve bucket name with tenant prefix if needed
    try:
        resolved_bucket = _find_bucket_with_tenant(executor, bucket)
        log.debug(f"Resolved bucket name: {resolved_bucket}")
    except ValueError as e:
        log.warning(
            f"Failed to resolve bucket name via bucket list: {e}. Using original name."
        )
        resolved_bucket = bucket
    # Fetch bucket stats using resolved name
    ret, out, err = executor.tool_exec(
        ['radosgw-admin', 'bucket', 'stats', '--bucket', resolved_bucket]
    )
    if ret:
        raise ValueError(
            f'Failed to fetch stats for bucket {resolved_bucket}: {err}'
        )

    try:
        return json.loads(out)
    except json.JSONDecodeError as e:
        raise ValueError(
            f'Failed to parse bucket stats for {resolved_bucket}: {e}'
        )


def _find_user_with_tenant(
    executor: ToolExecer, user_id: str
) -> Tuple[str, str]:
    """
    Find the tenant for a user if it exists.
    Args:
        executor: Any object with tool_exec() method
        user_id: The RGW user ID (without tenant prefix)
    Returns:
        Tuple of (tenant, user_id) where tenant is empty string if no tenant found
    Raises:
        ValueError: If user list cannot be fetched or parsed
    """
    log.debug(f"Searching for user {user_id} in user list")
    ret, out, err = executor.tool_exec(['radosgw-admin', 'user', 'list'])
    if ret:
        raise ValueError(f'Failed to list users: {err}')
    try:
        user_list = json.loads(out)
        if not isinstance(user_list, list):
            raise ValueError(
                f'Unexpected user list format: {type(user_list)}'
            )
        # Search for exact match first
        if user_id in user_list:
            log.debug(f"Found exact match for user {user_id}")
            return '', user_id
        # Search for tenant-prefixed match (tenant$user)
        for full_user_name in user_list:
            if isinstance(full_user_name, str) and '$' in full_user_name:
                try:
                    tenant, resource_user = _split_tenant_resource(
                        full_user_name
                    )
                    if resource_user == user_id:
                        log.debug(
                            f"Found tenant-aware user: {full_user_name}"
                        )
                        return tenant, user_id
                except ValueError:
                    log.debug(
                        f"Skipping malformed user name: {full_user_name}"
                    )
                    continue
        # User not found in list
        log.debug(f"User {user_id} not found in user list")
        return (
            '',
            user_id,
        )  # Return original name to let caller handle the error
    except json.JSONDecodeError as e:
        raise ValueError(f'Failed to parse user list: {e}')


def _get_rgw_owner(executor: ToolExecer, bucket: str) -> str:
    """
    Fetch RGW user bucket owner.
    Args:
        executor: Any object with tool_exec() method
        bucket: The RGW bucket name
    Returns:
        user_id: RGW user ID that owns bucket (without tenant prefix).
        For tenant-aware users like "tenantA$user1", returns just "user1".
    Raises:
        ValueError: If bucket owner cannot be determined
    """
    stats = _fetch_bucket_stats(executor, bucket)
    user_id = stats.get('owner', '')
    if not user_id:
        raise ValueError(f'No owner found for bucket {bucket}')
    # Extract username from tenant-aware format (tenant$user)
    if '$' in user_id:
        try:
            tenant, resource_user = _split_tenant_resource(user_id)
            user_id = resource_user
            log.debug(
                f"Extracted username from tenant-aware owner: {user_id}"
            )
        except ValueError as e:
            log.warning(f"Failed to parse owner '{user_id}': {e}")
            raise ValueError(
                f"Malformed owner format for bucket {bucket}: {e}"
            )
    log.debug(f"Bucket {bucket} is owned by user {user_id}")
    return user_id


def _get_rgw_creds(executor: ToolExecer, user_id: str) -> Tuple[str, str]:
    """
    Fetch RGW user credentials.
    Handles tenant-aware users (e.g., "tenantA$user1").
    Args:
        executor: Any object with tool_exec() method
        user_id: The RGW user ID (with or without tenant prefix)
    Returns:
        Tuple of (access_key_id, secret_access_key)
    Raises:
        ValueError: If credentials cannot be fetched
    """
    log.debug(f"Fetching credentials for user {user_id}")
    # Try to resolve user with tenant if needed
    try:
        tenant, resolved_user_id = _find_user_with_tenant(executor, user_id)
        log.debug(
            f"Resolved user: tenant='{tenant}', user_id='{resolved_user_id}'"
        )
    except ValueError as e:
        log.warning(
            f"Failed to resolve user via user list: {e}. Using original name."
        )
        tenant, resolved_user_id = '', user_id
    # Build command with tenant flag if tenant exists
    cmd = ['radosgw-admin', 'user', 'info', '--uid', resolved_user_id]
    if tenant:
        # Validate tenant is non-empty and non-whitespace
        if not tenant.strip():
            raise ValueError(
                f'Invalid tenant value for user {resolved_user_id}: '
                f'tenant is empty or whitespace only'
            )
        cmd.extend(['--tenant', tenant])
    ret, out, err = executor.tool_exec(cmd)
    if ret:
        raise ValueError(
            f'Failed to fetch credentials for user {resolved_user_id}: {err}'
        )
    try:
        j = json.loads(out)
        keys = j.get('keys', [])
        if not keys:
            raise ValueError(f'No keys found for user {resolved_user_id}')
        access_key = keys[0].get('access_key', '')
        secret_key = keys[0].get('secret_key', '')
        if not access_key or not secret_key:
            raise ValueError(
                f'Invalid credentials for user {resolved_user_id}: '
                f'access_key={bool(access_key)}, secret_key={bool(secret_key)}'
            )
        log.debug(
            f"Successfully fetched credentials for user {resolved_user_id}"
        )
        return access_key, secret_key
    except (json.JSONDecodeError, KeyError, IndexError) as e:
        raise ValueError(
            f'Failed to parse user info for {resolved_user_id}: {e}'
        )


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

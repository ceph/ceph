"""Utilities for RGW integration with SMB."""

import json
import logging
from typing import TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from .module import Module

log = logging.getLogger(__name__)


def fetch_rgw_credentials(
    mgr: 'Module',
    bucket: str,
    user_id: str = '',
) -> Tuple[str, str, str]:
    """
    Fetch RGW user credentials for a bucket.

    Args:
        mgr: The SMB manager module instance
        bucket: The RGW bucket name
        user_id: Optional RGW user ID. If not provided, fetches bucket owner.

    Returns:
        Tuple of (user_id, access_key_id, secret_access_key)

    Raises:
        ValueError: If bucket owner cannot be determined or credentials not found
    """
    if not user_id:
        # Fetch bucket owner
        log.debug(f"Fetching owner for bucket {bucket}")
        ret, out, err = mgr.tool_exec(
            ['radosgw-admin', 'bucket', 'stats', '--bucket', bucket]
        )
        if ret:
            raise ValueError(
                f'Failed to fetch owner for bucket {bucket}: {err}'
            )

        try:
            j = json.loads(out)
            user_id = j.get('owner', '')
            if not user_id:
                raise ValueError(f'No owner found for bucket {bucket}')
        except (json.JSONDecodeError, KeyError) as e:
            raise ValueError(
                f'Failed to parse bucket stats for {bucket}: {e}'
            )

        log.info(f"Bucket {bucket} is owned by user {user_id}")

    # Fetch user credentials
    log.debug(f"Fetching credentials for user {user_id}")
    ret, out, err = mgr.tool_exec(
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

        log.info(f"Successfully fetched credentials for user {user_id}")
        return user_id, access_key, secret_key

    except (json.JSONDecodeError, KeyError, IndexError) as e:
        raise ValueError(f'Failed to parse user info for {user_id}: {e}')


def validate_rgw_bucket(mgr: 'Module', bucket: str) -> bool:
    """
    Validate that an RGW bucket exists.

    Args:
        mgr: The SMB manager module instance
        bucket: The RGW bucket name

    Returns:
        True if bucket exists, False otherwise
    """
    log.debug(f"Validating bucket {bucket}")
    ret, out, err = mgr.tool_exec(
        ['radosgw-admin', 'bucket', 'stats', '--bucket', bucket]
    )
    if ret:
        log.warning(
            f"Bucket {bucket} does not exist or is not accessible: {err}"
        )
        return False

    try:
        j = json.loads(out)
        # If we can parse the output and it has an owner, bucket exists
        return bool(j.get('owner'))
    except (json.JSONDecodeError, KeyError):
        log.warning(f"Failed to parse bucket stats for {bucket}")
        return False

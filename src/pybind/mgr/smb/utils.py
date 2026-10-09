"""Assorted utility functions for smb mgr module."""
from typing import List, Optional, TypeVar

import hashlib
import random
import string

T = TypeVar('T')


def one(lst: List[T]) -> T:
    """Given a list, ensure that the list contains exactly one item and return
    it.  A ValueError will be raised in the case that the list does not contain
    exactly one item.
    """
    if len(lst) != 1:
        raise ValueError("list does not contain exactly one element")
    return lst[0]


class IsNoneError(ValueError):
    """A ValueError subclass raised by ``checked`` function."""

    pass


def checked(v: Optional[T]) -> T:
    """Ensures the provided value is not a None or raises a IsNoneError.
    Intended use is similar to an `assert v is not None` but more usable in
    one-liners and list/dict/etc comprehensions.
    """
    if v is None:
        raise IsNoneError('value is None')
    return v


def ynbool(value: bool) -> str:
    """Convert a bool to an smb.conf-style boolean string."""
    return 'Yes' if value else 'No'


def rand_name(prefix: str, max_len: int = 18, suffix_len: int = 8) -> str:
    trunc = prefix[: (max_len - suffix_len)]
    suffix = ''.join(
        random.choice(string.ascii_lowercase) for _ in range(suffix_len)
    )
    return f'{trunc}{suffix}'


def compute_credential_hash(cluster_id: str, user_id: str) -> str:
    """Compute a deterministic hash for RGW credential identity.

    This function creates a stable credential ID based on the cluster and
    RGW user, enabling credential reuse and in-place updates across multiple
    shares that access the same RGW user within the same cluster. Keys are
    stored in the credential and updated when they change.

    Args:
        cluster_id: SMB cluster ID (ensures isolation across clusters)
        user_id: RGW user ID (e.g., "tenant$user" or "user")

    Returns:
        Hash-based credential ID in format "cred-<hash>" where hash is
        12 hex characters from SHA256.
    """
    identity_str = f"{cluster_id}:{user_id}"
    hash_digest = hashlib.sha256(identity_str.encode()).hexdigest()[:12]
    return f"cred-{hash_digest}"

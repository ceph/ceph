# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional, TYPE_CHECKING
from ceph_secrets_types import SecretScope


if TYPE_CHECKING:
    # Import only for type-checkers to avoid runtime import cycles
    from .secret_store import SecretRecord


class SecretStorageBackend(ABC):
    """
    Abstract base class for ceph secrets storage backends.

    Backends store secret instances addressed by:
      (namespace, scope, target, name)

    For custom scope, target is empty and name stores the free-form path suffix.

    Epoch:
      Each namespace has an independent monotonic epoch counter that is bumped
      on every set and on rm only when an existing secret is actually removed.
      Consumers can use get_epoch(namespace) as a cheap change-detector to
      avoid re-fetching secrets when nothing changed in their namespace.

    Implementations:
      - SecretStoreMon  (Mon KV store)
      - Vault backend (future)
    """

    @abstractmethod
    def get(self, namespace: str, scope: SecretScope, target: str, name: str) -> Optional["SecretRecord"]:
        ...

    @abstractmethod
    def set(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
        data: str,
        user_made: bool = True,
        editable: bool = True,
    ) -> "SecretRecord":
        ...

    @abstractmethod
    def rm(self, namespace: str, scope: SecretScope, target: str, name: str) -> bool:
        ...

    @abstractmethod
    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
    ) -> List["SecretRecord"]:
        ...

    @abstractmethod
    def get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace*."""
        ...

    @abstractmethod
    def bump_epoch(self, namespace: str) -> int:
        """Increment and persist the epoch for *namespace*; return the new value."""
        ...

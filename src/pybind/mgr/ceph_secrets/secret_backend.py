# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple
from typing_extensions import Protocol
from ceph_secrets_types import SecretScope


if TYPE_CHECKING:
    # Import only for type-checkers to avoid runtime import cycles
    from .secret_store import SecretRecord, BadSecretRecord


class SecretStorageBackend(Protocol):
    """
    Storage backend interface for ceph secrets.

    Backends store secret instances addressed by:
      (namespace, scope, target, name)

    Implementations:
      - SecretStoreMon (Mon KV store)
      - (future) Vault backend
    """

    def get(self, namespace: str, scope: SecretScope, target: str, name: str) -> Optional["SecretRecord"]:
        ...

    def set(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
        data: Dict[str, Any],
        secret_type: str = "Opaque",
        user_made: bool = True,
        editable: bool = True,
    ) -> "SecretRecord":
        ...

    def rm(self, namespace: str, scope: SecretScope, target: str, name: str) -> bool:
        ...

    def ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[SecretScope] = None,
        target: Optional[str] = None,
    ) -> Tuple[List["SecretRecord"], List["BadSecretRecord"]]:
        ...

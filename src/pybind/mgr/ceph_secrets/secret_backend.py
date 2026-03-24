# -*- coding: utf-8 -*-
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TYPE_CHECKING, Tuple
from ceph_secrets_types import SecretScope


if TYPE_CHECKING:
    # Import only for type-checkers to avoid runtime import cycles
    from .secret_store import SecretRecord, BadSecretRecord


class SecretStorageBackend(ABC):
    """
    Abstract base class for ceph secrets storage backends.

    Backends store secret instances addressed by:
      (namespace, scope, target, name)

    Epoch:
      Each namespace has an independent monotonic epoch counter that is bumped
      on every mutation (set/rm).  Consumers can use get_epoch(namespace) as a
      cheap change-detector to avoid re-fetching secrets when nothing changed
      in their namespace.

    Registration:
      Concrete backends self-register by subclassing with a backend_name keyword
      argument.  The backend is then retrievable via SecretStorageBackend[name]:

        class SecretStoreMon(SecretStorageBackend, backend_name='mon'):
            ...

        backend_cls = SecretStorageBackend['mon']

    Implementations:
      - SecretStoreMon  (Mon KV store, backend_name='mon')
      - Vault backend (future)
    """

    # Registry populated automatically via __init_subclass__
    _registry: Dict[str, type] = {}

    def __init_subclass__(cls, backend_name: str = '', **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if backend_name:
            SecretStorageBackend._registry[backend_name] = cls

    @classmethod
    def __class_getitem__(cls, key: str) -> type:  # type: ignore[override]
        try:
            return cls._registry[key]
        except KeyError:
            available = ', '.join(sorted(cls._registry))
            raise KeyError(
                f"Unknown secrets backend {key!r}. Available: {available}"
            )

    # ------------------------------------------------------------------ CRUD

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
        data: Dict[str, Any],
        secret_type: str = "Opaque",
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
    ) -> Tuple[List["SecretRecord"], List["BadSecretRecord"]]:
        ...

    @abstractmethod
    def get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace*."""
        ...

    @abstractmethod
    def bump_epoch(self, namespace: str) -> int:
        """Increment and persist the epoch for *namespace*; return the new value."""
        ...

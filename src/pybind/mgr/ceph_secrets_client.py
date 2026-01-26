# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from ceph_secrets_types import SecretScope

logger = logging.getLogger(__name__)


class CephSecretsClient:
    """
    Thin client for calling the dedicated secrets mgr-module via mgr.remote().

    This file lives in src/pybind/mgr/ (like secret_types.py) so *any* mgr module
    can import it without creating a new mgr module directory.
    """

    DEFAULT_MODULE = "ceph_secrets"

    def __init__(self, mgr: Any, module: str = DEFAULT_MODULE):
        self.mgr = mgr
        self.module = module

    def _remote(self, method: str, **kwargs: Any) -> Any:
        try:
            return self.mgr.remote(self.module, method, **kwargs)
        except Exception as e:
            raise RuntimeError(
                f"Cannot call secrets mgr-module '{self.module}' (is it enabled?) {e}"
            ) from e

    # ---- epoch ----

    def secret_get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace*.

        Use this as a cheap change-detector: if the epoch is unchanged since
        your last check, no secrets in this namespace have been mutated.
        """
        return self._remote("secret_get_epoch", namespace=namespace)

    # ---- module API wrappers ----

    def secret_get(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
    ) -> Dict[str, Any]:
        return self._remote(
            "secret_get",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    def secret_get_version(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
    ) -> Optional[int]:
        return self._remote(
            "secret_get_version",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    def secret_get_versions(self, refs: List[Dict[str, str]]) -> Dict[str, Optional[int]]:
        return self._remote("secret_get_versions", refs=refs)

    def secret_set(
        self,
        namespace: str,
        scope: SecretScope,
        target: str,
        name: str,
        data: Dict[str, Any],
        secret_type: str = "Opaque",
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        return self._remote(
            "secret_set",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
            data=data,
            secret_type=secret_type,
            user_made=user_made,
            editable=editable,
        )

    def secret_rm(self, namespace: str, scope: SecretScope, target: str, name: str) -> bool:
        return bool(
            self._remote(
                "secret_rm",
                namespace=namespace,
                scope=scope,
                target=target,
                name=name,
            )
        )

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> Any:
        return self._remote("scan_unresolved_refs", obj=obj, namespace=namespace)

    def scan_refs(self, obj: Any, namespace: str) -> Any:
        return self._remote("scan_refs", obj=obj, namespace=namespace)

    def resolve_object(self, obj: Any) -> Any:
        return self._remote("resolve_object", obj=obj)

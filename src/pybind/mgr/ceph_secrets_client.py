# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Protocol, Union

from ceph_secrets_types import SecretScope

logger = logging.getLogger(__name__)

ScopeArg = Union[SecretScope, str]


class MgrRemote(Protocol):
    """Minimal interface required from the mgr object."""
    def remote(self, module: str, method: str, **kwargs: Any) -> Any:
        ...


class CephSecretsClient:
    """Thin client for calling the ceph_secrets mgr module via mgr.remote().

    This file lives in src/pybind/mgr/ alongside ceph_secrets_types.py so
    any mgr module can import it without depending on the ceph_secrets module
    directory directly.

    All methods translate to a single mgr.remote() call and raise RuntimeError
    if the ceph_secrets module is unreachable (e.g. not enabled).

    Typical usage::

        client = CephSecretsClient(self)   # self is a MgrModule instance
        rec = client.secret_get("cephadm", SecretScope.HOST, "node1", "ssh_key")
        if rec:
            version = rec["metadata"]["version"]
    """

    DEFAULT_MODULE = "ceph_secrets"

    def __init__(self, mgr: MgrRemote, module: str = DEFAULT_MODULE) -> None:
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
        """Return the current mutation epoch for *namespace*.

        The epoch is a monotonically increasing integer that is incremented on
        every successful set and on rm only when an existing secret is actually
        removed (an idempotent rm returning not-found does not bump the epoch).  It is
        deliberately per-namespace: a mutation in namespace A does not change
        the epoch of namespace B.

        Use this as a cheap change-detector: cache the epoch value after your
        last sync; if it is unchanged on the next poll, no secrets in this
        namespace have been mutated and you can skip a full refresh.

        Args:
            namespace: The secret namespace to query (e.g. ``"cephadm"``).

        Returns:
            The current epoch as a non-negative integer.  Starts at 0 for a
            namespace that has never been written to.
        """
        return self._remote("secret_get_epoch", namespace=namespace)

    # ---- module API wrappers ----

    def secret_get(
        self,
        namespace: str,
        scope: ScopeArg,
        target: str,
        name: str,
        reveal: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Retrieve a secret record by its full address.

        Returns the secret's metadata and, if *reveal* is True, its data
        payload.  Returns ``None`` if the secret does not exist; raises
        RuntimeError if the module is unreachable.

        The returned dict contains a ``metadata`` object with fields such as ``version``,
        ``created``, and ``updated``, etc.  It intentionally does not include a
        ``ref`` object because the caller already supplied the identity.
        The ``data`` key is only present when *reveal* is True.

        Args:
            namespace: The secret namespace (e.g. ``"cephadm"``).
            scope:     The secret scope — a :class:`SecretScope` value or its
                       string equivalent (``"global"``, ``"service"``,
                       ``"host"``, ``"custom"``).
            target:    The scope target.  Must be non-empty for ``service`` and
                       ``host`` scopes; must be empty for ``global`` and
                       ``custom``.
            name:      The secret name or, for ``custom`` scope, the
                       slash-delimited path (e.g. ``"app/db/password"``).
            reveal:    If True, include the secret's data payload in the
                       response.  Defaults to False to avoid accidental
                       exposure in logs.

        Returns:
            A dict of the form ``{"metadata": {...}}`` plus optional ``data``,
        or ``None`` if not found.
        """
        return self._remote(
            "secret_get",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
            reveal=reveal,
        )

    def secret_get_value(
        self,
        namespace: str,
        scope: ScopeArg,
        target: str,
        name: str,
    ) -> Optional[str]:
        """Return the raw secret data string.

        Returns the stored opaque string directly, without any JSON envelope
        or metadata.  Returns ``None`` if the secret does not exist.

        Use this when you need the secret value itself — for example, to pass
        a password to a subprocess or to resolve a credential at deploy time.
        For metadata inspection or change-detection, use :meth:`secret_get` or
        :meth:`secret_get_version` instead.

        Args:
            namespace: The secret namespace.
            scope:     The secret scope.
            target:    The scope target (empty for ``global`` and ``custom``).
            name:      The secret name or custom path.

        Returns:
            The stored string, or ``None`` if the secret does not exist.
        """
        return self._remote(
            "secret_get_value",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    def secret_get_version(
        self,
        namespace: str,
        scope: ScopeArg,
        target: str,
        name: str,
    ) -> Optional[int]:
        """Return the current version number of a secret.

        A convenience wrapper around :meth:`secret_get` for callers that only
        need to check whether a secret has changed since they last read it,
        without fetching its payload.

        The version is incremented on every successful :meth:`secret_set` call
        for the same address.  The first write produces version 1.

        Args:
            namespace: The secret namespace.
            scope:     The secret scope.
            target:    The scope target (empty for ``global`` and ``custom``).
            name:      The secret name or custom path.

        Returns:
            The current version as a positive integer, or ``None`` if the
            secret does not exist.
        """
        return self._remote(
            "secret_get_version",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
        )

    def secret_get_versions(self, uris: List[str]) -> Dict[str, Optional[int]]:
        """Batch-fetch version numbers for a list of secret URIs.

        More efficient than calling :meth:`secret_get_version` in a loop when
        you need to check many secrets at once (e.g. during a cephadm
        reconciliation pass).

        Each entry in *uris* must be a canonical ``secret:/...`` URI, such as
        one returned by ``SecretRef.to_uri()``. URIs that cannot be parsed are
        skipped and logged at ERROR level on the module side; a missing key in
        the result indicates malformed input rather than a not-found secret.

        Note that :meth:`scan_refs` may also return malformed or embedded
        secret-like strings for validation/reporting. Pass only canonical
        ``secret:/...`` URIs to this method.

        Args:
            uris: A list of canonical secret URIs.  Example::

                    [
                        "secret:/cephadm/host/node1/ssh_key",
                        "secret:/cephadm/global/dashboard_password",
                    ]

        Returns:
            A dict keyed by the input URI mapping to the current version
            integer, or ``None`` if the secret does not exist.
        """
        return self._remote("secret_get_versions", uris=uris)

    def secret_set(
        self,
        namespace: str,
        scope: ScopeArg,
        target: str,
        name: str,
        data: str,
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        """Create or update a secret.

        If a secret at the given address already exists its data is replaced
        and its version is incremented.  If it does not exist it is created at
        version 1.  The ``created`` timestamp is set on first write and never
        changed thereafter; ``updated`` is refreshed on every write.

        Args:
            namespace: The secret namespace.
            scope:     The secret scope.
            target:    The scope target (empty for ``global`` and ``custom``).
            name:      The secret name or custom path.
            data:      The secret payload as an opaque string.  Callers are
                       responsible for any structure within it (e.g.
                       JSON-encoding a dict before storing and decoding after
                       retrieval).
            user_made: Whether this secret was created by a human operator
                       rather than automatically by a Ceph component.  Defaults
                       to True; set to False for programmatically generated
                       secrets.
            editable:  Whether the secret may be updated by automated
                       tooling.  Deletion is always permitted regardless of
                       this flag.  Defaults to True.

        Returns:
            A dict containing the written record's ``metadata`` object.  The
            response intentionally omits ``ref`` and never includes the data
            payload.
        """
        return self._remote(
            "secret_set",
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
            data=data,
            user_made=user_made,
            editable=editable,
        )

    def secret_rm(
        self,
        namespace: str,
        scope: ScopeArg,
        target: str,
        name: str,
    ) -> bool:
        """Remove a secret.

        Idempotent: returns False if the secret did not exist rather than
        raising.  Raises RuntimeError only if the module is unreachable.

        Args:
            namespace: The secret namespace.
            scope:     The secret scope.
            target:    The scope target (empty for ``global`` and ``custom``).
            name:      The secret name or custom path.

        Returns:
            True if the secret existed and was removed; False if it was not
            found.
        """
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
        """Return all unresolved secret URI references found in *obj*.

        Walks *obj* recursively (dicts, lists, strings) and collects every
        secret-like reference that cannot currently be resolved because it is
        missing, malformed, embedded inside a larger string, or cannot be read
        successfully.

        Useful for validation: call this before deploying a configuration
        object to detect missing or invalid secret references early.

        Args:
            obj:       The object to scan.  May be a dict, list, or any
                       JSON-like structure.
            namespace: The namespace context used while scanning/reporting
                       references.

        Returns:
            A collection of unresolved reference strings found in *obj*.
        """
        return self._remote("scan_unresolved_refs", obj=obj, namespace=namespace)

    def scan_refs(self, obj: Any, namespace: str) -> Any:
        """Return all secret URI references found in *obj*.

        Like :meth:`scan_unresolved_refs` but returns every whole-value secret
        URI and every malformed or embedded secret-like reference found while
        scanning. Malformed or embedded entries are returned as their raw string
        value so callers can report them.

        Useful for auditing which secrets a configuration object depends on and
        for surfacing malformed or embedded references before deployment.

        Args:
            obj:       The object to scan.
            namespace: The namespace context for the scan.

        Returns:
            A collection of all secret URI strings found in *obj*.
        """
        return self._remote("scan_refs", obj=obj, namespace=namespace)

    def resolve_object(self, obj: Any) -> Any:
        """Resolve all secret URI references in *obj*.

        Walks *obj* recursively and replaces every whole-value ``secret:/...``
        URI string with the stored opaque string for the referenced secret.
        Surrounding whitespace around a URI reference is ignored, but embedding
        a secret URI inside a larger string is rejected because partial
        substitution is not supported.

        Args:
            obj: The object to resolve. May be a dict, list, or any JSON-like
                  structure containing ``secret:/...`` URI strings.

        Returns:
             A resolved copy of *obj* with all secret URIs replaced by their stored opaque strings.
             Raises RuntimeError if any referenced secret cannot be resolved
             or if the module is unreachable.
        """
        return self._remote("resolve_object", obj=obj)

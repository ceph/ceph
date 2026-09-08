# -*- coding: utf-8 -*-
import functools
from typing import Any, Dict, List, Optional, Callable, Tuple, TypeVar, Union
import errno

from .cli import CephSecretsCLICommand
from object_format import ObjectFormatAdapter, ErrorResponse, Responder
from mgr_module import (
    MgrModule,
    Option
)
from .secret_mgr import SecretMgr
from ceph_secrets_types import (
    CephSecretException,
    CephSecretDataError,
    CephSecretNotFoundError,
    SecretRef,
    SecretScope,
    parse_secret_path,
    parse_secret_uri,
)
from .backends import BACKENDS


_T = TypeVar('_T')


def _handle_secret_errors(fn: Callable[..., _T]) -> Callable[..., _T]:
    """Decorator for CLI handlers: converts CephSecretException into
    ErrorResponse so that Responder / ErrorResponseHandler can catch it."""
    @functools.wraps(fn)
    def wrapper(*args: Any, **kwargs: Any) -> _T:
        try:
            return fn(*args, **kwargs)
        except CephSecretException as e:
            raise ErrorResponse(str(e)) from e
    return wrapper


class Module(MgrModule):
    """Standalone secrets mgr module.

    This module owns the mgr KV-store entries for secrets (namespace: mgr/secrets)
    and provides both:
      - RPC methods for other mgr modules via `remote(...)`
      - CLI commands: `ceph secret ...`

    Storage keys inside the mgr KV store:
      secret data:   secret_store/v1/<namespace>/<scope>/...
      epoch (meta):  secret_store/meta/<namespace>/_epoch

    Epoch is per-namespace: a mutation in namespace A does not affect the epoch
    of namespace B, so consumers only see changes relevant to their namespace.
    Epoch logic lives entirely in the store backend so future backends
    (e.g. Vault) can implement it natively.

    Method organisation:
      - Public methods (no leading underscore): RPC surface called via
        mgr.remote(). Accept individual kwargs for wire-format compatibility.
        Each delegates immediately to the corresponding _secret_* method.
      - Private _secret_* methods: real implementations, operate on SecretRef.
        Called directly by CLI handlers and internal code.
    """
    CLICommand = CephSecretsCLICommand

    MODULE_OPTIONS = [
        Option(
            'secrets_backend',
            type='str',
            default='mon',
            desc='Secrets storage backend. Currently only "mon" (Mon KV store) is supported.',
        ),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        backend_name: str = str(self.get_module_option('secrets_backend'))
        try:
            backend_cls = BACKENDS[backend_name]
        except KeyError as e:
            raise RuntimeError(
                f"Unsupported secrets backend: {backend_name}"
            ) from e
        try:
            self.secret_mgr = SecretMgr(backend_cls(self))
        except Exception as e:
            raise RuntimeError(
                f"Failed to initialize secrets backend '{backend_name}': {e}"
            ) from e

    # ------------------------------------------------------------------ epoch

    def secret_get_epoch(self, namespace: str) -> int:
        """Return the current epoch for *namespace*.

        Consumers (e.g., cephadm) can use this as a cheap change-detector:
        if the epoch hasn't changed since the last check, no secrets in this
        namespace have been mutated.
        """
        return self.secret_mgr.store.get_epoch(namespace)

    # ------------------------------------------------------------------ RPC surface

    def secret_ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[str] = None,
        target: Optional[str] = None,
        show_values: bool = False,
        show_internals: bool = False,
    ) -> Dict[str, Any]:
        sc = SecretScope.from_str(scope) if scope else None
        records = self.secret_mgr.ls(namespace=namespace, scope=sc, target=target)
        out: Dict[str, Any] = {}
        for r in records:
            if r.ref.target:
                key = f'{r.ref.namespace}/{r.ref.scope.value}/{r.ref.target}/{r.ref.name}'
            else:
                key = f'{r.ref.namespace}/{r.ref.scope.value}/{r.ref.name}'
            out[key] = r.to_public_json(
                include_data=bool(show_values),
                include_policy=show_internals,
                include_ref=True,
            )
        return out

    def secret_get(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
        reveal: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """RPC surface — called via mgr.remote(). Internal code uses _secret_get()."""
        return self._secret_get(
            self.secret_mgr.make_ref(namespace, scope, target, name),
            reveal=reveal,
        )

    def secret_get_value(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
    ) -> Optional[str]:
        """RPC surface — return the raw secret data string, or None if not found.

        Called via mgr.remote(). Internal code uses _secret_get_value().
        """
        return self._secret_get_value(
            self.secret_mgr.make_ref(namespace, scope, target, name)
        )

    def secret_get_version(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
    ) -> Optional[int]:
        """RPC surface — called via mgr.remote(). Internal code uses _secret_get_version()."""
        return self._secret_get_version(
            self.secret_mgr.make_ref(namespace, scope, target, name)
        )

    def secret_get_versions(self, uris: List[str]) -> Dict[str, Optional[int]]:
        """Batch-fetch version numbers for a list of secret URIs.

        Each entry in *uris* must be a canonical ``secret:/...`` URI, such as
        one returned by ``SecretRef.to_uri()``. URIs that cannot be parsed are
        skipped and logged at ERROR level; a missing key in the result indicates
        malformed input rather than a not-found secret.

        Note that ``scan_refs()`` may also return malformed or embedded
        secret-like strings for validation/reporting. Callers should pass only
        canonical ``secret:/...`` URIs to this method.

        Returns a dict keyed by the input URI mapping to the current version
        integer, or ``None`` if the secret does not exist.
        """
        out: Dict[str, Optional[int]] = {}
        for uri in uris or []:
            try:
                ref = parse_secret_uri(uri)
            except CephSecretException:
                self.log.error(
                    "secret_get_versions: skipping invalid URI %r",
                    uri, exc_info=True
                )
                continue
            out[uri] = self._secret_get_version(ref)  # CephSecretDataError propagates
        return out

    def secret_set(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
        data: str,
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        """RPC surface — called via mgr.remote(). Internal code uses _secret_set()."""
        return self._secret_set(
            self.secret_mgr.make_ref(namespace, scope, target, name),
            data=data,
            user_made=user_made,
            editable=editable,
        )

    def secret_rm(
        self,
        namespace: str,
        scope: Union[SecretScope, str],
        target: str,
        name: str,
    ) -> bool:
        """RPC surface — called via mgr.remote(). Internal code uses _secret_rm()."""
        return self._secret_rm(self.secret_mgr.make_ref(namespace, scope, target, name))

    def resolve_object(self, obj: Any) -> Any:
        return self.secret_mgr.resolve_object(obj)

    def scan_refs(self, obj: Any, namespace: str) -> List[str]:
        return sorted({u.to_uri() for u in
                       self.secret_mgr.scan_refs(obj, namespace)})

    def scan_unresolved_refs(self, obj: Any, namespace: str) -> List[str]:
        return sorted({u.to_uri() for u in
                       self.secret_mgr.scan_unresolved_refs(obj, namespace)})

    # ------------------------------------------------------------------ ref-based implementations

    def _secret_get(self, ref: SecretRef, reveal: bool = False) -> Optional[Dict[str, Any]]:
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretDataError:
            # Corruption is not the same as absence; let callers/CLI report a
            # data error instead of returning the same sentinel as "not found".
            raise
        except CephSecretNotFoundError:
            return None
        return rec.to_public_json(include_data=reveal, include_policy=False, include_ref=False)

    def _secret_get_value(self, ref: SecretRef) -> Optional[str]:
        """Return the raw data string for a secret, or None if not found."""
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretDataError:
            raise
        except CephSecretNotFoundError:
            return None
        return rec.data

    def _secret_get_version(self, ref: SecretRef) -> Optional[int]:
        try:
            rec = self.secret_mgr.get(ref)
        except CephSecretDataError:
            raise
        except CephSecretNotFoundError:
            return None
        return rec.metadata.version

    def _secret_set(
        self,
        ref: SecretRef,
        data: str,
        user_made: bool = True,
        editable: bool = True,
    ) -> Dict[str, Any]:
        rec = self.secret_mgr.set(
            namespace=ref.namespace,
            scope=ref.scope,
            target=ref.target,
            name=ref.name,
            data=data,
            user_made=user_made,
            editable=editable,
        )
        return rec.to_public_json(include_data=False, include_policy=False, include_ref=False)

    def _secret_rm(self, ref: SecretRef) -> bool:
        return self.secret_mgr.rm(ref.namespace, ref.scope, ref.target, ref.name)

    # ------------------------------------------------------------------ CLI commands

    @CephSecretsCLICommand.Read('secret ls')
    @Responder(functools.partial(ObjectFormatAdapter, compatible=True))
    @_handle_secret_errors
    def _cli_secret_ls(
        self,
        namespace: Optional[str] = None,
        scope: Optional[str] = None,
        sec_target: Optional[str] = None,
        reveal: bool = False,
        show_internals: bool = False,
    ) -> Dict[str, Any]:
        return self.secret_ls(
            namespace=namespace,
            scope=scope,
            target=sec_target,
            show_values=reveal,
            show_internals=show_internals,
        )

    @CephSecretsCLICommand.Read('secret get')
    @Responder(functools.partial(ObjectFormatAdapter, compatible=True))
    @_handle_secret_errors
    def _cli_secret_get_by_path(
        self,
        path: str,
        reveal: bool = False,
    ) -> Dict[str, Any]:
        ref = parse_secret_path(path)
        res = self._secret_get(ref, reveal=reveal)
        if res is None:
            raise ErrorResponse('secret error: not found', return_value=-errno.ENOENT)
        return res

    @CephSecretsCLICommand.Read('secret get-value')
    @_handle_secret_errors
    def _cli_secret_get_value_by_path(
        self,
        path: str,
    ) -> Tuple[int, str, str]:
        """Return the raw secret data string for the given path.

        Unlike ``secret get --reveal``, this command outputs the secret value
        directly as a plain string with no JSON envelope, making it suitable
        for use in shell scripts and pipelines.
        """
        ref = parse_secret_path(path)
        value = self._secret_get_value(ref)
        if value is None:
            raise ErrorResponse('secret error: not found', return_value=-errno.ENOENT)
        return 0, value, ''

    @CephSecretsCLICommand.Write('secret set')
    @Responder(functools.partial(ObjectFormatAdapter, compatible=True))
    @_handle_secret_errors
    def _cli_secret_set_by_path(
        self,
        path: str,
        inbuf: Optional[str] = None,
    ) -> Dict[str, Any]:
        if inbuf is None:
            raise ErrorResponse('secret error: use -i to provide secret data')
        if inbuf == '':
            raise ErrorResponse('secret error: secret data must not be empty')
        ref = parse_secret_path(path)
        return self._secret_set(ref, data=inbuf)

    @CephSecretsCLICommand.Write('secret rm')
    @Responder(functools.partial(ObjectFormatAdapter, compatible=True))
    @_handle_secret_errors
    def _cli_secret_rm_by_path(
        self,
        path: str,
    ) -> Dict[str, Any]:
        ref = parse_secret_path(path)
        existed = self._secret_rm(ref)
        return {'status': 'removed' if existed else 'not_found'}

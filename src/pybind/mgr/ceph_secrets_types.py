# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Tuple
from typing_extensions import Protocol, Hashable
from urllib.parse import ParseResult, urlparse, parse_qs, unquote, urlencode


# Internal URI scheme for secret references.
# We use the `scheme://authority/path` form; the authority component is the namespace (not a network host).
SECRET_SCHEME = 'secret'
SECRET_URI_SCHEME = f'{SECRET_SCHEME}://'


class CephSecretException(Exception):
    pass

class CephSecretDataError(CephSecretException):
    pass

class SecretScope(str, Enum):
    GLOBAL = 'global'
    SERVICE = 'service'
    HOST = 'host'

    @classmethod
    def from_str(cls, s: str) -> 'SecretScope':
        try:
            return SecretScope(s)
        except Exception as e:
            allowed = ', '.join(x.value for x in SecretScope)
            raise CephSecretException(
                f'Invalid secret scope {s!r}. Expected one of: {allowed}'
            ) from e


_SCOPE_VALUES = frozenset(s.value for s in SecretScope)


class SecretURI(Protocol, Hashable):
    def to_uri(self) -> str: ...


def _extract_key_param(parsed_url: ParseResult) -> Optional[str]:
    """
    Extract the optional data key from the query string (?key=<value>).

    Policy:
      - Fragment form ('#...') is intentionally not supported; it is rejected upstream.
      - Reject multiple 'key=' values.
      - Reject an explicitly empty key (e.g. '?key=').

    Note: keep_blank_values=True is required so that '?key=' is preserved by parse_qs
    and can be detected and rejected. Without it, parse_qs silently drops the empty
    value and the function would return None, treating '?key=' as if no key was given.
    """
    q = parse_qs(parsed_url.query or '', keep_blank_values=True)
    vals = q.get('key')

    if vals is None:
        return None

    if len(vals) != 1:
        raise CephSecretException(
            f'Invalid secret uri: multiple key parameters are not allowed: {vals!r}'
        )

    key = (vals[0] or '').strip()
    if not key:
        raise CephSecretException('Invalid secret uri: key parameter must not be empty')
    return key


def _validate_components_no_slash(
    *,
    uri: str,
    namespace: str,
    scope: SecretScope,
    target: str,
    name: str,
    key: Optional[str],
) -> None:
    if not namespace:
        raise CephSecretException(f'Invalid secret uri {uri!r}: namespace must not be empty')
    if not name:
        raise CephSecretException(f'Invalid secret uri {uri!r}: name must not be empty')
    if scope != SecretScope.GLOBAL and not target:
        raise CephSecretException(f'Invalid secret uri {uri!r}: target must not be empty')
    if key is not None and not key.strip():
        raise CephSecretException(f'Invalid secret uri {uri!r}: key must not be empty if specified')

    for label, val in (
        ('namespace', namespace),
        ('target', target),
        ('name', name),
        ('key', key or ''),
    ):
        if val and '/' in val:
            raise CephSecretException(f'Invalid secret uri {uri!r}: {label!r} must not contain \'/\'')


@dataclass(frozen=True)
class SecretRef:
    namespace: str
    scope: SecretScope
    target: str
    name: str
    key: Optional[str] = None

    def __post_init__(self) -> None:
        try:
            _validate_components_no_slash(
                uri='<SecretRef>',
                namespace=self.namespace,
                scope=self.scope,
                target=self.target,
                name=self.name,
                key=self.key,
            )
        except CephSecretException as e:
            raise ValueError(str(e)) from e

    def ident(self) -> Tuple[str, str, str, str]:
        return (self.namespace, self.scope.value, self.target, self.name)

    def to_uri(self) -> str:
        # Components are already validated to not contain '/'
        if self.scope == SecretScope.GLOBAL:
            base = f'{SECRET_URI_SCHEME}{self.namespace}/{self.scope.value}/{self.name}'
        else:
            base = f'{SECRET_URI_SCHEME}{self.namespace}/{self.scope.value}/{self.target}/{self.name}'

        if self.key is not None:
            return f'{base}?{urlencode({"key": self.key})}'
        return base


@dataclass(frozen=True)
class BadSecretURI:
    raw: str
    error: str
    namespace: str

    def to_uri(self) -> str:
        return self.raw


def parse_secret_uri(uri: str) -> SecretRef:
    """
    Parse a secret reference URI (canonical forms only; no short forms; no fragments).

    Canonical (non-global):
        secret://<namespace>/<scope>/<target>/<name>
        secret://<namespace>/<scope>/<target>/<name>?key=<data_key>

    Canonical (global, no target):
        secret://<namespace>/global/<name>
        secret://<namespace>/global/<name>?key=<data_key>

    Rules:
      - Namespace is always the URI authority (netloc); never inferred from context.
      - Short forms ('secret://global/<name>' etc.) are rejected with a targeted error.
        Note: this disallows namespaces named exactly 'global', 'service', or 'host'.
      - Fragment form ('#<data_key>') is not supported; use '?key=' instead.
      - Exactly the right number of path segments is required; extras are rejected.
      - Empty segments (from '//' or trailing '/') are rejected.
      - Components must not contain '/' after percent-decoding; encoded slashes ('%2F')
        are therefore not valid in any component.
    """
    try:
        if not isinstance(uri, str):
            raise CephSecretException('secret uri must be a string')

        parsed = urlparse(uri)
        if parsed.scheme != SECRET_SCHEME:
            raise CephSecretException(f'Not a secret uri: {uri!r}')

        # Reject fragments explicitly.
        if parsed.fragment:
            raise CephSecretException(
                f"Invalid secret uri {uri!r}: fragment form ('#...') is not supported; "
                f"use '?key=<value>' instead."
            )

        # --- namespace (authority component) ---
        namespace = unquote(parsed.netloc or '').strip()
        if not namespace:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: missing namespace '
                f'(expected secret://<namespace>/...)'
            )

        # Detect old short-form attempts and give a targeted error.
        if namespace in _SCOPE_VALUES:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: short forms are not supported. '
                f'Expected secret://<namespace>/{namespace}/... instead.'
            )

        # --- path ---
        raw_path = parsed.path or ''
        if not raw_path.startswith('/'):
            raise CephSecretException(
                f"Invalid secret uri {uri!r}: malformed path (no leading '/')"
            )

        path_str = raw_path.lstrip('/')
        if not path_str:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: missing scope and name in path'
            )

        # Strict: reject empty segments caused by '//' or trailing '/'
        segs = path_str.split('/')
        if any(s == '' for s in segs):
            raise CephSecretException(
                f"Invalid secret uri {uri!r}: empty path segment (check for '//' or trailing '/')"
            )

        # Decode each segment exactly once (split before decoding so that a
        # percent-encoded slash '%2F' is never mistaken for a path separator).
        parts = [unquote(s) for s in segs]

        # --- key (optional, query string only) ---
        key = _extract_key_param(parsed)

        # --- scope: first path segment ---
        if len(parts) < 2:
            raise CephSecretException(
                f"Invalid secret uri {uri!r}. Expected "
                f"secret://<namespace>/<scope>/<target>/<name>[?key=...] "
                f"or secret://<namespace>/global/<name>[?key=...]"
            )

        scope = SecretScope.from_str(parts[0])

        # --- target + name: strict segment count per scope ---
        if scope == SecretScope.GLOBAL:
            if len(parts) != 2:
                raise CephSecretException(
                    f"Invalid secret uri {uri!r}: global scope requires exactly one name segment "
                    f"(got {len(parts) - 1}). "
                    f"Expected secret://<namespace>/global/<name>[?key=...]"
                )
            target, name = '', parts[1]
        else:
            if len(parts) != 3:
                raise CephSecretException(
                    f"Invalid secret uri {uri!r}: {scope.value!r} scope requires exactly "
                    f"<target>/<name> (got {len(parts) - 1} segment(s)). "
                    f"Expected secret://<namespace>/{scope.value}/<target>/<name>[?key=...]"
                )
            target, name = parts[1], parts[2]

        # Validate components with URI context so errors mention the URI,
        # not the '<SecretRef>' placeholder from __post_init__.
        # Also catches '/' in any decoded component (e.g. from %2F).
        _validate_components_no_slash(
            uri=uri,
            namespace=namespace,
            scope=scope,
            target=target,
            name=name,
            key=key,
        )

        return SecretRef(namespace=namespace, scope=scope, target=target, name=name, key=key)

    except CephSecretException:
        raise
    except Exception as e:
        # Normalize unexpected parsing errors (e.g. from urlparse internals)
        # into CephSecretException for consistent error handling by callers.
        raise CephSecretException(f'Invalid secret uri {uri!r}: {e}') from e


def _coerce_scope(s: str) -> 'SecretScope':
    # Accept both enum values ('global') and enum names ('GLOBAL')
    if not s.strip():
        raise CephSecretException('Scope must not be empty')

    s_norm = s.strip()
    try:
        return SecretScope(s_norm)
    except Exception:
        try:
            return SecretScope[s_norm.upper()]
        except Exception:
            allowed = ', '.join(x.value for x in SecretScope)
            raise CephSecretException(f'Unknown scope {s!r}. Expected one of: {allowed}')


def parse_secret_path(path: str) -> Tuple[str, 'SecretScope', str, str]:
    """
    Parse a secret locator path:
      <namespace>/<scope>/<target>/<name>

    Also supports global scope (no target):
      <namespace>/global/<name>

    Strict rules:
      - Input must be a string.
      - A single optional leading '/' is tolerated (e.g. from CLI tab-completion);
        multiple leading slashes are rejected to catch misformatted input.
      - No empty segments: '//' and trailing '/' are rejected.
      - Whitespace around segments is NOT normalised; padded segments are rejected.
      - Scope must be valid
      - In the 4-part form, 'global' scope is rejected (use 3-part form instead).
      - In the 3-part form, any non-global scope is rejected.
    """

    p = path.strip()
    if not p:
        raise CephSecretException('Invalid secret path: empty')

    # Tolerate exactly one leading slash (common from CLI), but reject '//' or more.
    if p.startswith('//'):
        raise CephSecretException(
            f"Invalid secret path {path!r}: multiple leading slashes are not allowed"
        )
    p = p.lstrip('/')

    # Reject empty segments from '//' in the middle or a trailing '/'.
    segs = p.split('/')
    if any(s == '' for s in segs):
        raise CephSecretException(
            f"Invalid secret path {path!r}: empty segment (check for '//' or trailing '/')"
        )

    # Reject whitespace-padded segments - do NOT silently strip them.
    if any(s != s.strip() for s in segs):
        raise CephSecretException(
            f"Invalid secret path {path!r}: segments must not contain leading/trailing whitespace"
        )

    parts = segs  # segments are already validated; no further transformation needed

    if len(parts) == 4:
        ns, scope_s, target, name = parts
        scope = _coerce_scope(scope_s)
        if scope == SecretScope.GLOBAL:
            raise CephSecretException(
                f"Invalid secret path {path!r}: global scope cannot have a target; "
                f"use the 3-part form '<namespace>/global/<name>' instead"
            )
        return ns, scope, target, name

    if len(parts) == 3:
        ns, scope_s, name = parts
        scope = _coerce_scope(scope_s)
        if scope != SecretScope.GLOBAL:
            raise CephSecretException(
                f"Invalid secret path {path!r}: 3-part form is only valid for global scope "
                f"(got {scope.value!r}). Use '<namespace>/{scope.value}/<target>/<name>' instead."
            )
        return ns, scope, '', name

    raise CephSecretException(
        f"Invalid secret path {path!r}. "
        f"Use '<namespace>/<scope>/<target>/<name>' "
        f"or '<namespace>/global/<name>'."
    )

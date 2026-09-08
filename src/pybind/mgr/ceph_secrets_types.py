# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Tuple
from urllib.parse import urlparse, quote


# Internal URI scheme for secret references.
# Canonical form has no authority: secret:/<namespace>/<scope>/...
SECRET_SCHEME = 'secret'


class CephSecretException(Exception):
    pass


class CephSecretDataError(CephSecretException):
    pass


class CephSecretNotFoundError(CephSecretException):
    pass


# ---------------------------------------------------------------------------
# Segment grammar
# ---------------------------------------------------------------------------
# Accepted characters: alphanumeric, dot, hyphen, underscore.
# Additional rule: a segment must not end with '.' (Vault API restriction).
# Applies to: namespace, global name, service/host target and name,
# and each individual segment of a custom path.

_SEGMENT_RE = re.compile(r'^[A-Za-z0-9._-]+$')


def _validate_segment(label: str, value: str) -> None:
    """Raise ValueError with a field-level message. No URI context — callers re-wrap."""
    if not isinstance(value, str):
        raise ValueError(f'{label} must be a string')
    if not value:
        raise ValueError(f'{label} must not be empty')
    if not _SEGMENT_RE.fullmatch(value):
        raise ValueError(f'{label} contains unsupported characters')
    if value.endswith('.'):
        raise ValueError(f"{label} must not end with '.'")


def validate_secret_namespace(namespace: str) -> None:
    """Validate a secret namespace segment. Raises ValueError."""
    _validate_segment('namespace', namespace)


def _validate_custom_path(value: str) -> None:
    """Validate a slash-delimited custom path. Raises ValueError."""
    if not isinstance(value, str):
        raise ValueError('custom path must be a string')
    if not value:
        raise ValueError('custom path must not be empty')
    parts = value.split('/')
    if any(p == '' for p in parts):
        raise ValueError('custom path must not contain empty segments')
    for part in parts:
        _validate_segment('custom path segment', part)


# ---------------------------------------------------------------------------
# URI serialisation helpers
# ---------------------------------------------------------------------------
# With strict segment validation, quoting is effectively a no-op. Kept as
# defensive correctness for round-trip safety.

def _quote_segment(v: str) -> str:
    """Percent-encode a single path segment (slashes not preserved)."""
    return quote(v, safe='')


def _quote_custom_path(v: str) -> str:
    """Percent-encode a custom path, preserving '/' as segment delimiters."""
    return quote(v, safe='/')


# ---------------------------------------------------------------------------
# Scope
# ---------------------------------------------------------------------------

class SecretScope(str, Enum):
    GLOBAL = 'global'
    SERVICE = 'service'
    HOST = 'host'
    CUSTOM = 'custom'

    @classmethod
    def from_str(cls, s: str) -> 'SecretScope':
        try:
            return SecretScope(s)
        except Exception as e:
            allowed = ', '.join(x.value for x in SecretScope)
            raise CephSecretException(
                f'Invalid secret scope {s!r}. Expected one of: {allowed}'
            ) from e

    def validate_fields(self, target: str, name: str) -> None:
        """Validate target/name for this scope. Raises ValueError."""
        if self == SecretScope.GLOBAL:
            if target:
                raise ValueError('target must be empty for global scope')
            _validate_segment('name', name)

        elif self == SecretScope.CUSTOM:
            if target:
                raise ValueError('target must be empty for custom scope')
            _validate_custom_path(name)

        elif self in (SecretScope.SERVICE, SecretScope.HOST):
            _validate_segment('target', target)
            _validate_segment('name', name)

        else:
            raise ValueError(f'unsupported scope {self!r}')


# ---------------------------------------------------------------------------
# SecretRef
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SecretRef:
    namespace: str
    scope: SecretScope
    target: str
    name: str

    def __post_init__(self) -> None:
        try:
            scope = (
                self.scope
                if isinstance(self.scope, SecretScope)
                else SecretScope.from_str(str(self.scope))
            )
        except CephSecretException as e:
            raise ValueError(str(e)) from e
        object.__setattr__(self, 'scope', scope)
        _validate_segment('namespace', self.namespace)
        scope.validate_fields(self.target, self.name)

    def ident(self) -> Tuple[str, str, str, str]:
        return (self.namespace, self.scope.value, self.target, self.name)

    def to_uri(self) -> str:
        ns = _quote_segment(self.namespace)
        scope = self.scope.value

        if self.scope == SecretScope.CUSTOM:
            return f'{SECRET_SCHEME}:/{ns}/{scope}/{_quote_custom_path(self.name)}'

        if self.scope == SecretScope.GLOBAL:
            return f'{SECRET_SCHEME}:/{ns}/{scope}/{_quote_segment(self.name)}'

        return (
            f'{SECRET_SCHEME}:/{ns}/{scope}/'
            f'{_quote_segment(self.target)}/{_quote_segment(self.name)}'
        )


@dataclass(frozen=True)
class BadSecretURI:
    raw: str
    error: str
    namespace: str

    def to_uri(self) -> str:
        return self.raw


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_secret_uri(uri: str) -> SecretRef:
    """
    Parse a secret reference URI.

    Canonical forms:
      secret:/<namespace>/global/<name>
      secret:/<namespace>/service/<target>/<name>
      secret:/<namespace>/host/<target>/<name>
      secret:/<namespace>/custom/<path>

    All segments must match [A-Za-z0-9._-]+ and must not end with '.'.
    Percent-encoding, query strings, fragments, and URI authority are not supported.
    """
    try:
        if not isinstance(uri, str):
            raise CephSecretException('secret uri must be a string')

        if uri != uri.strip():
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: leading/trailing whitespace is not allowed'
            )

        parsed = urlparse(uri)
        if parsed.scheme != SECRET_SCHEME:
            raise CephSecretException(f'Not a secret uri: {uri!r}')
        if parsed.query or parsed.fragment:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: query strings and fragments are not supported'
            )
        if uri.startswith(f'{SECRET_SCHEME}://') or parsed.netloc:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: authority is not supported; '
                f'use secret:/<namespace>/<scope>/<path>'
            )

        # Canonical form: secret:/<namespace>/<scope>/<path>.
        path = parsed.path or ''
        if not path.startswith('/'):
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: expected secret:/<namespace>/<scope>/<path>'
            )

        # Reject percent-encoding: the strict segment grammar has a single canonical
        # spelling for every valid identifier. Accepting encoded aliases (e.g.
        # db%2Dpassword → db-password, app%2Fdb → app/db) would silently create
        # multiple URIs that resolve to the same secret.
        if '%' in path:
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: percent-encoding is not supported'
            )

        # Split on raw '/' — no unquote() needed since percent-encoding is rejected.
        namespace_raw, sep, remainder = path.lstrip('/').partition('/')
        scope_raw, sep2, rest_raw = remainder.partition('/') if sep else ('', '', '')
        if not (sep and sep2):
            raise CephSecretException(
                f'Invalid secret uri {uri!r}: expected secret:/<namespace>/<scope>/<path>'
            )

        scope = SecretScope.from_str(scope_raw)

        if scope in (SecretScope.GLOBAL, SecretScope.CUSTOM):
            target = ''
            name = rest_raw
        else:
            target_raw, _, name_raw = rest_raw.partition('/')
            target = target_raw
            name = name_raw

        # Single construction point: SecretRef validates all fields.
        # ValueError is re-raised with the original URI for user-facing messages.
        try:
            return SecretRef(namespace=namespace_raw, scope=scope, target=target, name=name)
        except ValueError as e:
            raise CephSecretException(f'Invalid secret uri {uri!r}: {e}') from e

    except CephSecretException:
        raise
    except ValueError as e:
        raise CephSecretException(str(e)) from e
    except Exception as e:
        raise CephSecretException(f'Invalid secret uri {uri!r}: {e}') from e


def _coerce_scope(s: str) -> 'SecretScope':
    # Accept both enum values ('global') and enum names ('GLOBAL').
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
            raise CephSecretException(
                f'Unknown scope {s!r}. Expected one of: {allowed}'
            )


def parse_secret_path(path: str) -> SecretRef:
    """
    Parse a secret locator path (no URI scheme, no percent-encoding):
      <namespace>/global/<name>
      <namespace>/service/<target>/<name>
      <namespace>/host/<target>/<name>
      <namespace>/custom/<any-path>

    Returns a validated SecretRef. Raises CephSecretException on any
    structural or content error.
    """
    if not isinstance(path, str):
        raise CephSecretException('secret path must be a string')

    if path != path.strip():
        raise CephSecretException(
            f'Invalid secret path {path!r}: leading/trailing whitespace is not allowed'
        )

    p = path.strip()
    if not p:
        raise CephSecretException('Invalid secret path: empty')

    if p.startswith('//'):
        raise CephSecretException(
            f"Invalid secret path {path!r}: multiple leading slashes are not allowed"
        )
    p = p.lstrip('/')

    segs = p.split('/')
    if any(s == '' for s in segs):
        raise CephSecretException(
            f"Invalid secret path {path!r}: empty segment (check for '//' or trailing '/')"
        )
    if any(s != s.strip() for s in segs):
        raise CephSecretException(
            f"Invalid secret path {path!r}: segments must not contain leading/trailing whitespace"
        )
    if len(segs) < 3:
        raise CephSecretException(
            f"Invalid secret path {path!r}. Use '<namespace>/<scope>/<path>'."
        )

    ns, scope_s = segs[0], segs[1]
    scope = _coerce_scope(scope_s)
    rest = segs[2:]

    if scope == SecretScope.GLOBAL:
        if len(rest) != 1:
            raise CephSecretException(
                f"Invalid secret path {path!r}: global scope expects '<namespace>/global/<name>'"
            )
        target, name = '', rest[0]

    elif scope == SecretScope.CUSTOM:
        target, name = '', '/'.join(rest)

    elif len(rest) != 2:
        raise CephSecretException(
            f"Invalid secret path {path!r}: {scope.value!r} scope expects "
            f"'<namespace>/{scope.value}/<target>/<name>'"
        )

    else:
        target, name = rest[0], rest[1]

    try:
        return SecretRef(ns, scope, target, name)
    except ValueError as e:
        raise CephSecretException(f'Invalid secret path {path!r}: {e}') from e

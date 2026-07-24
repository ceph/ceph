from enum import Enum
from typing import Any, Dict, Protocol, Union, NamedTuple, Optional
from orchestrator import OrchestratorError
from ceph.utils import strtobool


def parse_bool(value: Any) -> bool:
    """
    Parses a value as a boolean. Accepts:
    - bool values (returns as-is)
    - strings like "true"/"false" (case-insensitive)
    - numeric values (0 -> False, non-zero -> True)
    Raises:
        ValueError if the value cannot be interpreted as a boolean.
    """
    try:
        return strtobool(value) if isinstance(value, str) else bool(value)
    except Exception:
        raise ValueError(f"Expected a boolean-compatible value but got: {type(value)}")


class TLSObjectException(OrchestratorError):
    pass


class TLSObjectScope(str, Enum):
    HOST = 'host'
    SERVICE = 'service'
    GLOBAL = 'global'
    UNKNOWN = 'unknown'

    def __str__(self) -> str:
        return self.value


class TLSObjectManager(str, Enum):
    USER = 'user'
    CEPHADM = 'cephadm'
    VAULT = 'vault'
    ACME = 'acme'

    def __str__(self) -> str:
        return self.value


def managed_by_from_user_made(user_made: bool) -> TLSObjectManager:
    return TLSObjectManager.USER if user_made else TLSObjectManager.CEPHADM


def user_made_from_managed_by(managed_by: TLSObjectManager) -> bool:
    return managed_by == TLSObjectManager.USER


def _normalize_managed_by(managed_by: Union[TLSObjectManager, str]) -> TLSObjectManager:
    if isinstance(managed_by, TLSObjectManager):
        return managed_by
    try:
        return TLSObjectManager(managed_by)
    except ValueError:
        raise TLSObjectException(f'Got invalid managed_by value for TLS object: {managed_by}')


def _resolve_managed_by(user_made: Optional[bool],
                        managed_by: Optional[Union[TLSObjectManager, str]]) -> TLSObjectManager:
    if managed_by is not None:
        return _normalize_managed_by(managed_by)
    if user_made is not None:
        return managed_by_from_user_made(user_made)
    return TLSObjectManager.CEPHADM


class TLSCredentials(NamedTuple):
    cert: str
    key: str
    ca_cert: Optional[str] = None

    def __bool__(self) -> bool:
        # Treat the pair as truthy only if both cert and key are non-empty
        return bool(self.cert and self.key)


class TLSObjectTarget(NamedTuple):
    service: Optional[str]
    host: Optional[str]


EMPTY_TLS_CREDENTIALS = TLSCredentials('', '', '')


class TLSObjectProtocol(Protocol):
    STORAGE_PREFIX: str

    def __init__(self,
                 cert: str = '',
                 user_made: Optional[bool] = None,
                 editable: bool = False,
                 managed_by: Optional[Union[TLSObjectManager, str]] = None) -> None:
        ...

    def __bool__(self) -> bool:
        ...

    def __eq__(self, other: Any) -> bool:
        ...

    @property
    def user_made(self) -> bool:
        ...

    def to_json(self) -> Dict[str, Union[str, bool]]:
        ...

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TLSObjectProtocol':
        ...


class Cert(TLSObjectProtocol):
    STORAGE_PREFIX = 'cert'

    def __init__(self,
                 cert: str = '',
                 user_made: Optional[bool] = None,
                 editable: bool = False,
                 managed_by: Optional[Union[TLSObjectManager, str]] = None) -> None:
        self.cert = cert
        self.managed_by = _resolve_managed_by(user_made, managed_by)
        self.editable = editable

    @property
    def user_made(self) -> bool:
        return user_made_from_managed_by(self.managed_by)

    def __bool__(self) -> bool:
        return bool(self.cert)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Cert):
            return self.cert == other.cert and self.managed_by == other.managed_by
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        if (self):
            return {
                'cert': self.cert,
                'managed_by': self.managed_by.value,
                'user_made': self.user_made,
                'editable': self.editable
            }
        else:
            return {}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'Cert':
        if 'cert' not in data:
            return cls()
        cert = data['cert']
        if not isinstance(cert, str):
            raise TLSObjectException('Tried to make Cert object with non-string cert')
        if any(k not in ['cert', 'managed_by', 'user_made', 'editable'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for Cert object. Fields: {data.keys()}')

        try:
            user_made = parse_bool(data['user_made']) if 'user_made' in data else None
            editable = parse_bool(data.get('editable', False))
        except ValueError as e:
            raise TLSObjectException(f'Expected field in Cert object to be bool but got another type: {e}')

        managed_by = data.get('managed_by')
        return cls(cert=cert, user_made=user_made, editable=editable, managed_by=managed_by)


class PrivKey(TLSObjectProtocol):
    STORAGE_PREFIX = 'key'

    def __init__(self,
                 key: str = '',
                 user_made: Optional[bool] = None,
                 editable: bool = False,
                 managed_by: Optional[Union[TLSObjectManager, str]] = None) -> None:
        self.key = key
        self.managed_by = _resolve_managed_by(user_made, managed_by)
        self.editable = editable

    @property
    def user_made(self) -> bool:
        return user_made_from_managed_by(self.managed_by)

    def __bool__(self) -> bool:
        return bool(self.key)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PrivKey):
            return self.key == other.key and self.managed_by == other.managed_by
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        if bool(self):
            return {
                'key': self.key,
                'managed_by': self.managed_by.value,
                'user_made': self.user_made,
                'editable': self.editable
            }
        else:
            return {}

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'PrivKey':
        if 'key' not in data:
            return cls()
        key = data['key']
        if not isinstance(key, str):
            raise TLSObjectException('Tried to make PrivKey object with non-string key')
        if any(k not in ['key', 'managed_by', 'user_made', 'editable'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for PrivKey object. Fields: {data.keys()}')

        try:
            user_made = parse_bool(data['user_made']) if 'user_made' in data else None
            editable = parse_bool(data.get('editable', False))
        except ValueError as e:
            raise TLSObjectException(f'Expected field in PrivKey object to be bool but got another type: {e}')

        managed_by = data.get('managed_by')
        return cls(key=key, user_made=user_made, editable=editable, managed_by=managed_by)

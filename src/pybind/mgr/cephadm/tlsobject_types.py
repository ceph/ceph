from typing import Any, Dict, Protocol, Union
from orchestrator import OrchestratorError


def parse_bool(value: Any) -> bool:
    """
    Parses a value as a boolean. Accepts:
    - bool values (returns as-is)
    - strings like "true"/"false" (case-insensitive)
    - numeric values (0 -> False, non-zero -> True)
    Raises:
        ValueError if the value cannot be interpreted as a boolean.
    """
    if isinstance(value, bool):
        return value

    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered == 'true':
            return True
        elif lowered == 'false':
            return False
        else:
            raise ValueError(f"Cannot parse boolean from string: {value}")

    # Accept ints or other types coercible to bool
    try:
        return bool(value)
    except Exception:
        raise ValueError(f"Expected a boolean-compatible value but got: {type(value)}")


class TLSObjectException(OrchestratorError):
    pass


class TLSObjectProtocol(Protocol):
    STORAGE_PREFIX: str

    def __init__(self, cert: str = '', user_made: bool = False, editable: bool = False) -> None:
        ...

    def __bool__(self) -> bool:
        ...

    def __eq__(self, other: Any) -> bool:
        ...

    def to_json(self) -> Dict[str, Union[str, bool]]:
        ...

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> 'TLSObjectProtocol':
        ...


class Cert(TLSObjectProtocol):
    STORAGE_PREFIX = 'cert'

    def __init__(self, cert: str = '', user_made: bool = False, editable: bool = False) -> None:
        self.cert = cert
        self.user_made = user_made
        self.editable = editable

    def __bool__(self) -> bool:
        return bool(self.cert)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Cert):
            return self.cert == other.cert and self.user_made == other.user_made
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        if (self):
            return {
                'cert': self.cert,
                'user_made': self.user_made,
                'editable': self.editable
            }
        else:
            return {}

    @classmethod
    def from_json(cls, data: Dict[str, Union[str, bool]]) -> 'Cert':
        if 'cert' not in data:
            return cls()
        cert = data['cert']
        if not isinstance(cert, str):
            raise TLSObjectException('Tried to make Cert object with non-string cert')
        if any(k not in ['cert', 'user_made', 'editable'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for Cert object. Fields: {data.keys()}')

        try:
            user_made: Union[str, bool] = data.get('user_made', False)
            user_made = parse_bool(user_made)
        except ValueError:
            raise TLSObjectException(f'Expected user_made field in Cert object to be bool but got {type(user_made)}')

        try:
            editable: Union[str, bool] = data.get('editable', False)
            editable = parse_bool(editable)
        except ValueError:
            raise TLSObjectException(f'Expected editable field in Cert object to be bool but got {type(editable)}')

        return cls(cert=cert, user_made=user_made, editable=editable)


class PrivKey(TLSObjectProtocol):
    STORAGE_PREFIX = 'key'

    def __init__(self, key: str = '', user_made: bool = False, editable: bool = False) -> None:
        self.key = key
        self.user_made = user_made
        self.editable = editable

    def __bool__(self) -> bool:
        return bool(self.key)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PrivKey):
            return self.key == other.key and self.user_made == other.user_made
        return NotImplemented

    def to_json(self) -> Dict[str, Union[str, bool]]:
        if bool(self):
            return {
                'key': self.key,
                'user_made': self.user_made,
                'editable': self.editable
            }
        else:
            return {}

    @classmethod
    def from_json(cls, data: Dict[str, str]) -> 'PrivKey':
        if 'key' not in data:
            return cls()
        key = data['key']
        if not isinstance(key, str):
            raise TLSObjectException('Tried to make PrivKey object with non-string key')
        if any(k not in ['key', 'user_made', 'editable'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for PrivKey object. Fields: {data.keys()}')

        try:
            user_made: Union[str, bool] = data.get('user_made', False)
            user_made = parse_bool(user_made)
        except ValueError:
            raise TLSObjectException(f'Expected user_made field in Cert object to be bool but got {type(user_made)}')

        try:
            editable: Union[str, bool] = data.get('editable', False)
            editable = parse_bool(editable)
        except ValueError:
            raise TLSObjectException(f'Expected editable field in Cert object to be bool but got {type(editable)}')

        return cls(key=key, user_made=user_made, editable=editable)

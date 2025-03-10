from typing import Any, Dict, Protocol, Union
from orchestrator import OrchestratorError


class TLSObjectException(OrchestratorError):
    pass


class TLSObjectProtocol(Protocol):
    STORAGE_PREFIX: str

    def __init__(self, cert: str = '', user_made: bool = False) -> None:
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

    def __init__(self, cert: str = '', user_made: bool = False) -> None:
        self.cert = cert
        self.user_made = user_made

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
                'user_made': self.user_made
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
        if any(k not in ['cert', 'user_made'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for Cert object. Fields: {data.keys()}')
        user_made: Union[str, bool] = data.get('user_made', False)
        if not isinstance(user_made, bool):
            if isinstance(user_made, str):
                if user_made.lower() == 'true':
                    user_made = True
                elif user_made.lower() == 'false':
                    user_made = False
            try:
                user_made = bool(user_made)
            except Exception:
                raise TLSObjectException(f'Expected user_made field in Cert object to be bool but got {type(user_made)}')
        return cls(cert=cert, user_made=user_made)


class PrivKey(TLSObjectProtocol):
    STORAGE_PREFIX = 'key'

    def __init__(self, key: str = '', user_made: bool = False) -> None:
        self.key = key
        self.user_made = user_made

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
                'user_made': self.user_made
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
        if any(k not in ['key', 'user_made'] for k in data.keys()):
            raise TLSObjectException(f'Got unknown field for PrivKey object. Fields: {data.keys()}')
        user_made: Union[str, bool] = data.get('user_made', False)
        if not isinstance(user_made, bool):
            if isinstance(user_made, str):
                if user_made.lower() == 'true':
                    user_made = True
                elif user_made.lower() == 'false':
                    user_made = False
            try:
                user_made = bool(user_made)
            except Exception:
                raise TLSObjectException(f'Expected user_made field in PrivKey object to be bool but got {type(user_made)}')
        return cls(key=key, user_made=user_made)

from typing import Dict, Any


class BaseClient:
    def __init__(self,
                 host: str,
                 username: str,
                 password: str) -> None:
        self.host = host
        self.username = username
        self.password = password

    def login(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def logout(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_path(self, path: str) -> Dict:
        raise NotImplementedError()

from typing import Dict


class BaseClient:
    def __init__(self,
                 host: str,
                 username: str,
                 password: str) -> None:
        self.host = host
        self.username = username
        self.password = password

    def login(self) -> None:
        raise NotImplementedError()

    def logout(self) -> None:
        raise NotImplementedError()

    def get_path(self, path: str) -> Dict:
        raise NotImplementedError()

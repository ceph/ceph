# context.py - cephadm application context support classes

import argparse
from typing import Any, Dict, List, Optional

from .constants import (
    CONTAINER_INIT,
    DATA_DIR,
    DEFAULT_RETRY,
    DEFAULT_TIMEOUT,
    LOGROTATE_DIR,
    LOG_DIR,
    SYSCTL_DIR,
    UNIT_DIR,
)


class BaseConfig:
    def __init__(self) -> None:
        self.image: str = ''
        self.docker: bool = False
        self.data_dir: str = DATA_DIR
        self.log_dir: str = LOG_DIR
        self.logrotate_dir: str = LOGROTATE_DIR
        self.sysctl_dir: str = SYSCTL_DIR
        self.unit_dir: str = UNIT_DIR
        self.verbose: bool = False
        self.timeout: Optional[int] = DEFAULT_TIMEOUT
        self.retry: int = DEFAULT_RETRY
        self.env: List[str] = []
        self.memory_request: Optional[int] = None
        self.memory_limit: Optional[int] = None
        self.log_to_journald: Optional[bool] = None

        self.container_init: bool = CONTAINER_INIT
        # FIXME(refactor) : should be Optional[ContainerEngine]
        self.container_engine: Any = None

    def set_from_args(self, args: argparse.Namespace) -> None:
        argdict: Dict[str, Any] = vars(args)
        for k, v in argdict.items():
            if hasattr(self, k):
                setattr(self, k, v)


class CephadmContext:
    def __init__(self) -> None:
        self.__dict__['_args'] = None
        self.__dict__['_conf'] = BaseConfig()

    def set_args(self, args: argparse.Namespace) -> None:
        self._conf.set_from_args(args)
        self._args = args

    def has_function(self) -> bool:
        return 'func' in self._args

    def __contains__(self, name: str) -> bool:
        return hasattr(self, name)

    def __getattr__(self, name: str) -> Any:
        if '_conf' in self.__dict__ and hasattr(self._conf, name):
            return getattr(self._conf, name)
        elif '_args' in self.__dict__ and hasattr(self._args, name):
            return getattr(self._args, name)
        else:
            return super().__getattribute__(name)

    def __setattr__(self, name: str, value: Any) -> None:
        if hasattr(self._conf, name):
            setattr(self._conf, name, value)
        elif hasattr(self._args, name):
            setattr(self._args, name, value)
        else:
            super().__setattr__(name, value)

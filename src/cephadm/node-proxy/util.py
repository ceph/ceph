import logging
import yaml
import os
import time
import re
from typing import Dict, List, Callable, Any


class Logger:
    _Logger: List['Logger'] = []

    def __init__(self, name: str, level: int = logging.INFO):
        self.name = name
        self.level = level

        Logger._Logger.append(self)
        self.logger = self.get_logger()

    def get_logger(self) -> logging.Logger:
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        handler = logging.StreamHandler()
        handler.setLevel(self.level)
        fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(fmt)
        logger.addHandler(handler)

        return logger


class Config:

    def __init__(self,
                 config_file: str = '/etc/ceph/node-proxy.yaml',
                 default_config: Dict[str, Any] = {}) -> None:
        self.config_file = config_file
        self.default_config = default_config

        self.load_config()

    def load_config(self) -> None:
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self.default_config

        for k, v in self.default_config.items():
            if k not in self.config.keys():
                self.config[k] = v

        for k, v in self.config.items():
            setattr(self, k, v)

        # TODO: need to be improved
        for _l in Logger._Logger:
            _l.logger.setLevel(self.logging['level'])  # type: ignore
            _l.logger.handlers[0].setLevel(self.logging['level'])  # type: ignore

    def reload(self, config_file: str = '') -> None:
        if config_file != '':
            self.config_file = config_file
        self.load_config()


log = Logger(__name__)


def to_snake_case(name: str) -> str:
    name = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', name).lower()


def normalize_dict(test_dict: Dict) -> Dict:
    res = dict()
    for key in test_dict.keys():
        if isinstance(test_dict[key], dict):
            res[key.lower()] = normalize_dict(test_dict[key])
        else:
            res[key.lower()] = test_dict[key]
    return res


def retry(exceptions: Any = Exception, retries: int = 20, delay: int = 1) -> Callable:
    def decorator(f: Callable) -> Callable:
        def _retry(*args: str, **kwargs: Any) -> Callable:
            _tries = retries
            while _tries > 1:
                try:
                    log.logger.debug("{} {} attempt(s) left.".format(f, _tries - 1))
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(delay)
                    _tries -= 1
            log.logger.warn("{} has failed after {} tries".format(f, retries))
            return f(*args, **kwargs)
        return _retry
    return decorator

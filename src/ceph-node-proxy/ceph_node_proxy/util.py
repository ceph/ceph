import logging
import yaml
import os
import time
import re
import ssl
import traceback
import threading
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from urllib.error import HTTPError, URLError
from urllib.request import urlopen, Request
from typing import Dict, Callable, Any, Optional, MutableMapping, Tuple, Union


CONFIG: Dict[str, Any] = {
    'reporter': {
        'check_interval': 5,
        'push_data_max_retries': 30,
        'endpoint': 'https://%(mgr_host):%(mgr_port)/node-proxy/data',
    },
    'system': {
        'refresh_interval': 5
    },
    'api': {
        'port': 9456,
    },
    'logging': {
        'level': logging.INFO,
    }
}


def get_logger(name: str, level: Union[int, str] = logging.NOTSET) -> logging.Logger:
    if level == logging.NOTSET:
        log_level = CONFIG['logging']['level']
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False

    return logger


logger = get_logger(__name__)


class Config:
    def __init__(self,
                 config_file: str = '/etc/ceph/node-proxy.yaml',
                 config: Dict[str, Any] = {}) -> None:
        self.config_file = config_file
        self.config = config

        self.load_config()

    def load_config(self) -> None:
        if os.path.exists(self.config_file):
            with open(self.config_file, 'r') as f:
                self.config = yaml.safe_load(f)
        else:
            self.config = self.config

        for k, v in self.config.items():
            if k not in self.config.keys():
                self.config[k] = v

        for k, v in self.config.items():
            setattr(self, k, v)

    def reload(self, config_file: str = '') -> None:
        if config_file != '':
            self.config_file = config_file
        self.load_config()


class BaseThread(threading.Thread):
    def __init__(self) -> None:
        super().__init__()
        self.exc: Optional[Exception] = None
        self.stop: bool = False
        self.daemon = True
        self.name = self.__class__.__name__
        self.log: logging.Logger = get_logger(__name__)
        self.pending_shutdown: bool = False

    def run(self) -> None:
        logger.info(f'Starting {self.name}')
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def shutdown(self) -> None:
        self.stop = True
        self.pending_shutdown = True

    def check_status(self) -> bool:
        logger.debug(f'Checking status of {self.name}')
        if self.exc:
            traceback.print_tb(self.exc.__traceback__)
            logger.error(f'Caught exception: {self.exc.__class__.__name__}')
            raise self.exc
        if not self.is_alive():
            logger.info(f'{self.name} not alive')
            self.start()
        return True

    def main(self) -> None:
        raise NotImplementedError()


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
                    logger.debug('{} {} attempt(s) left.'.format(f, _tries - 1))
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(delay)
                    _tries -= 1
            logger.warn('{} has failed after {} tries'.format(f, retries))
            return f(*args, **kwargs)
        return _retry
    return decorator


def http_req(hostname: str = '',
             port: str = '443',
             method: Optional[str] = None,
             headers: MutableMapping[str, str] = {},
             data: Optional[str] = None,
             endpoint: str = '/',
             scheme: str = 'https',
             ssl_verify: bool = False,
             timeout: Optional[int] = None,
             ssl_ctx: Optional[Any] = None) -> Tuple[Any, Any, Any]:

    if not ssl_ctx:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not ssl_verify:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        else:
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED

    url: str = f'{scheme}://{hostname}:{port}{endpoint}'
    _data = bytes(data, 'ascii') if data else None
    _headers = headers
    if data and not method:
        method = 'POST'
    if not _headers.get('Content-Type') and method in ['POST', 'PATCH']:
        _headers['Content-Type'] = 'application/json'
    try:
        req = Request(url, _data, _headers, method=method)
        with urlopen(req, context=ssl_ctx, timeout=timeout) as response:
            response_str = response.read()
            response_headers = response.headers
            response_code = response.code
        return response_headers, response_str.decode(), response_code
    except (HTTPError, URLError) as e:
        print(f'{e}')
        # handle error here if needed
        raise


def write_tmp_file(data: str, prefix_name: str = 'node-proxy-') -> _TemporaryFileWrapper:
    f = NamedTemporaryFile(prefix=prefix_name)
    os.fchmod(f.fileno(), 0o600)
    f.write(data.encode('utf-8'))
    f.flush()
    return f

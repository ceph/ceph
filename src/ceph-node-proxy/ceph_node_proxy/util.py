import logging
import os
import re
import ssl
import threading
import time
import traceback
from tempfile import NamedTemporaryFile, _TemporaryFileWrapper
from typing import Any, Callable, Dict, MutableMapping, Optional, Tuple, Union
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml

DEFAULTS: Dict[str, Any] = {
    "reporter": {
        "check_interval": 5,
        "push_data_max_retries": 30,
        "endpoint": "https://%(mgr_host):%(mgr_port)/node-proxy/data",
    },
    "system": {
        "refresh_interval": 20,
        "vendor": "generic",
    },
    "api": {
        "port": 9456,
    },
    "logging": {
        "level": logging.INFO,
    },
}


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config(
    path: str,
    defaults: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    defaults = defaults or {}
    if not os.path.exists(path):
        return _deep_merge({}, defaults)
    with open(path, "r") as f:
        loaded = yaml.safe_load(f) or {}
    return _deep_merge(defaults, loaded)


def get_logger(name: str, level: Union[int, str] = logging.NOTSET) -> logging.Logger:
    log_level: Union[int, str] = level
    if log_level == logging.NOTSET:
        log_level = DEFAULTS["logging"]["level"]
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    handler = logging.StreamHandler()
    handler.setLevel(log_level)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.propagate = False

    return logger


logger = get_logger(__name__)


class Config:
    def __init__(
        self,
        path: str,
        defaults: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.path = path
        self.defaults = defaults or {}
        self._data = load_config(self.path, self.defaults)

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def reload(self, path: Optional[str] = None) -> None:
        if path is not None:
            self.path = path
        self._data = load_config(self.path, self.defaults)


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
        logger.info(f"Starting {self.name}")
        try:
            self.main()
        except Exception as e:
            self.exc = e
            return

    def shutdown(self) -> None:
        self.stop = True
        self.pending_shutdown = True

    def check_status(self) -> bool:
        logger.debug(f"Checking status of {self.name}")
        if self.exc:
            traceback.print_tb(self.exc.__traceback__)
            logger.error(f"Caught exception: {self.exc.__class__.__name__}")
            raise self.exc
        if not self.is_alive():
            logger.warning(f"{self.name} not alive")
            self.start()
        return True

    def main(self) -> None:
        raise NotImplementedError()


def to_snake_case(name: str) -> str:
    name = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()


def normalize_dict(test_dict: Dict) -> Dict:
    res = dict()
    for key in test_dict.keys():
        if isinstance(test_dict[key], dict):
            res[key.lower()] = normalize_dict(test_dict[key])
        else:
            if test_dict[key] is None:
                test_dict[key] = "unknown"
            res[key.lower()] = test_dict[key]
    return res


def retry(
    exceptions: Any = Exception, retries: int = 20, delay: int = 1
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    def decorator(f: Callable[..., Any]) -> Callable[..., Any]:
        def _retry(*args: Any, **kwargs: Any) -> Any:
            _tries = retries
            while _tries > 1:
                try:
                    logger.debug("{} {} attempt(s) left.".format(f, _tries - 1))
                    return f(*args, **kwargs)
                except exceptions:
                    time.sleep(delay)
                    _tries -= 1
            logger.warning("{} has failed after {} tries".format(f, retries))
            return f(*args, **kwargs)

        return _retry

    return decorator


def http_req(
    hostname: str = "",
    port: str = "443",
    method: Optional[str] = None,
    headers: MutableMapping[str, str] = {},
    data: Optional[str] = None,
    endpoint: str = "/",
    scheme: str = "https",
    ssl_verify: bool = False,
    timeout: Optional[int] = None,
    ssl_ctx: Optional[Any] = None,
) -> Tuple[Any, Any, Any]:

    if not ssl_ctx:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not ssl_verify:
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        else:
            ssl_ctx.verify_mode = ssl.CERT_REQUIRED

    url: str = f"{scheme}://{hostname}:{port}{endpoint}"
    _data = bytes(data, "ascii") if data else None
    _headers = headers
    if data and not method:
        method = "POST"
    if not _headers.get("Content-Type") and method in ["POST", "PATCH"]:
        _headers["Content-Type"] = "application/json"
    try:
        req = Request(url, _data, _headers, method=method)
        with urlopen(req, context=ssl_ctx, timeout=timeout) as response:
            response_str = response.read()
            response_headers = response.headers
            response_code = response.code
        return response_headers, response_str.decode(), response_code
    except (HTTPError, URLError) as e:
        # Log level is debug only.
        # We let whatever calls `http_req()` catching and printing the error
        logger.debug(f"url={url} err={e}")
        # handle error here if needed
        raise


def write_tmp_file(
    data: str, prefix_name: str = "node-proxy-"
) -> _TemporaryFileWrapper:
    f = NamedTemporaryFile(prefix=prefix_name)
    os.fchmod(f.fileno(), 0o600)
    f.write(data.encode("utf-8"))
    f.flush()
    return f


def _dict_diff(old: Any, new: Any) -> Any:
    if old == new:
        return None
    if type(old) is not type(new) or not isinstance(new, dict):
        return new
    if not isinstance(old, dict):
        return new
    delta: Dict[str, Any] = {}
    for k in sorted(set(old) | set(new)):
        if k not in old:
            delta[k] = new[k]
        elif k not in new:
            delta[k] = None
        else:
            sub = _dict_diff(old[k], new[k])
            if sub is not None:
                delta[k] = sub
    return delta if delta else None

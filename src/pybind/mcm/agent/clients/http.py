import requests
from requests.exceptions import RequestException
from typing import Callable, Optional, Any
from ..auth.token_manager import with_token_auth
from ..models.config import RESTConfig
from ..models.http import HttpMethod
from ..utils.url import join_urls

class TokenHttpClient:
    # ToDo: Create a client pool
    def __init__(self, base_url: str, login_func: Callable[[], str], login_config: RESTConfig):
        self.base_url = base_url.rstrip("/")
        self.login_func = login_func
        self.login_config = login_config
        self._token: Optional[str] = None

    @with_token_auth(login_func=lambda self: self.login_func())
    def get(self, relative_url: str, **kwargs):
        url = join_urls(self.base_url, relative_url)
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if self._token is not None:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            response = requests.get(url, headers=headers, verify=False)
            #response.raise_for_status()
            return response
        except Exception as e:
            print(f"[ERROR] GET failed: {e}")
            return {}

    @with_token_auth(login_func=lambda self: self.login_func())
    def post(self, relative_url:str, data:dict, **kwargs):
        url = join_urls(self.base_url, relative_url)
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if self._token is not None:
            headers["Authorization"] = f"Bearer {self._token}"
        print("request url: ", url, "headers: ", headers, "data: ", data, "\n")
        try:
            response = requests.post(url, headers=headers, json=data, verify=False)
            #response.raise_for_status()
            return response
        except Exception as e:
            print(f"[ERROR] POST failed: {e}")
            return {}

class UnguardedHttpClient:
    def __init__(self, base_url: str, login_func: Callable[[], str], login_config: RESTConfig):
        self.base_url = base_url.rstrip("/")
        self.login_func = login_func
        self.login_config = login_config
        self._token: Optional[str] = None

    def get(self, relative_url: str, **kwargs):
        url = join_urls(self.base_url, relative_url)
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if self._token is not None:
            headers["Authorization"] = f"Bearer {self._token}"
        try:
            response = requests.get(url, headers=headers, verify=False)
            #response.raise_for_status()
            return response
        except Exception as e:
            print(f"[ERROR] GET failed: {e}")
            return {}

    def post(self, relative_url:str, data:dict, **kwargs):
        url = join_urls(self.base_url, relative_url)
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if self._token is not None:
            headers["Authorization"] = f"Bearer {self._token}"
        print("request url: ", url, "headers: ", headers, "data: ", data, "\n")
        try:
            response = requests.post(url, headers=headers, json=data, verify=False)
            #response.raise_for_status()
            return response
        except Exception as e:
            print(f"[ERROR] POST failed: {e}")
            return {}
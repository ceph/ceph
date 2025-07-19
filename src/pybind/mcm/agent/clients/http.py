import requests
from requests.exceptions import HTTPError, Timeout, ConnectionError
from typing import Callable, Optional
from ..auth.token_manager import with_token_auth
from ..models.config import RESTConfig
from ..models.http import HttpMethod
from ..utils.url import join_urls
import time

class TokenHttpClient:
    # ToDo: Create a client pool
    def __init__(self, base_url: str, login_func: Callable[[], str], login_config: RESTConfig):
        self.base_url = base_url.rstrip("/")
        self.login_func = login_func
        self.login_config = login_config
        self._token: Optional[str] = None

    @with_token_auth(login_func=lambda self: self.login_func())
    def get(self, relative_url: str, **kwargs):
        return self._request(HttpMethod.GET, relative_url, **kwargs)

    @with_token_auth(login_func=lambda self: self.login_func())
    def post(self, relative_url: str, data: dict, **kwargs):
        return self._request(HttpMethod.POST, relative_url, json=data, **kwargs)

    def _request(self, method: HttpMethod, relative_url: str, **kwargs):
        url = join_urls(self.base_url, relative_url)
        headers = {"Accept": "application/vnd.ceph.api.v1.0+json", "Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"

        retries = self.login_config.retries or 3
        backoff = 1

        for attempt in range(retries):
            try:
                if method == HttpMethod.GET:
                    response = requests.get(url, headers=headers, verify=False, timeout=10, **kwargs)
                elif method == HttpMethod.POST:
                    response = requests.post(url, headers=headers, verify=False, timeout=10, **kwargs)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response.raise_for_status()
                return response

            except (HTTPError, Timeout, ConnectionError) as e:
                print(f"[RETRY] Attempt {attempt+1}/{retries} failed: {e}")
                time.sleep(backoff)
                backoff *= 2

        raise Exception(f"All {retries} retries failed for {method} {url}")

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

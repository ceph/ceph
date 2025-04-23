from functools import wraps
from typing import Callable
from ..models.config import RESTConfig

""" class TokenManager:
    def __init__(self, login_func: Callable[[], str], config: RESTConfig):
        self.login_func = login_func
        self.config = config
        self.token = None

    def get_token(self):
        if not self.token:
            self.token = self.login_func()
        return self.token

    def refresh_token(self):
        self.token = self.login_func()
        return self.token

def with_token(token_manager):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(token_manager.config.retries):
                token = token_manager.get_token()
                headers = {}
                headers["Authorization"] = f"Bearer {token}"
                kwargs["headers"] = headers

                response = func(*args, **kwargs)

                if response.status_code == 401:
                    print("[!] Unauthorized — refreshing token")
                    token_manager.refresh_token()
                    continue

                return response
            raise Exception("[X] All retries failed due to token/auth issues.")
        return wrapper
    return decorator
 """
def with_token_auth(login_func):
    def decorator(func):
        def wrapper(self, *args, **kwargs):
            # Initial token fetch if not present
            if not getattr(self, "_token", None):
                try:
                    token = login_func(self)
                    if not token:
                        raise Exception("Login returned no token")
                    self._token = token
                except Exception as e:
                    print(f"[ERROR] Failed to get token: {e}")
                    return {}

            # Attempt the original request
            result = func(self, *args, **kwargs)

            # If token expired (401), retry once with refreshed token
            if hasattr(result, "status_code") and result.status_code == 401:
                print("[AUTH] Token expired. Re-authenticating...")
                try:
                    token = login_func(self)
                    if not token:
                        raise Exception("Login returned no token on retry")
                    self._token = token
                    result = func(self, *args, **kwargs)
                except Exception as e:
                    print(f"[ERROR] Retry after token refresh failed: {e}")
                    return {}

            return result
        return wrapper
    return decorator
import os
from .constants import DASHBOARD_USERNAME, DEFAULT_DASHBOARD_USERNAME, DASHBOARD_PASSWORD, DEFAULT_DASHBOARD_PASSWORD
from .base import MCMAgentBase

class AuthConfig(MCMAgentBase):
    def __init__(self, username: str, password: str):
        self.username = username
        self.password = password

class RESTConfig(MCMAgentBase):
    def __init__(self, api_base_path: str, retries: int, auth_cfg: AuthConfig = None):
        self.api_base_path = api_base_path
        self.retries = retries
        self.auth_config = auth_cfg

class MCMAgentConfig(MCMAgentBase):
    def __init__(self, prometheus_api_base_path: str, dashboard_api_base_path: str, http_retries: int, mcm_api_base_path: str):
        self.prometheus_config = RESTConfig(
            prometheus_api_base_path,
            http_retries,
        )
        self.dashboard_config = RESTConfig(
            dashboard_api_base_path,
            http_retries,
            AuthConfig(
                os.getenv(
                    DASHBOARD_USERNAME,
                    DEFAULT_DASHBOARD_USERNAME,
                ),
                os.getenv(
                    DASHBOARD_PASSWORD,
                    DEFAULT_DASHBOARD_PASSWORD,
                ),
            ),
        )
        self.mcm_api_config = RESTConfig(
            mcm_api_base_path,
            http_retries,
        )
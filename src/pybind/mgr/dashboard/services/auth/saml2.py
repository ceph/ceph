from typing import Any

from .auth import BaseAuth, SSOAuthMixin


class Saml2(BaseAuth, SSOAuthMixin):
    LOGIN_URL = 'auth/saml2/login'
    LOGOUT_URL = 'auth/saml2/slo'

    class Saml2Config(BaseAuth.Config):
        onelogin_settings: Any

    def __init__(self, onelogin_settings):
        self.onelogin_settings = onelogin_settings

    def get_username_attribute(self):
        return self.onelogin_settings['sp']['attributeConsumingService']['requestedAttributes'][0][
            'name']

    def to_dict(self) -> 'Saml2Config':
        return self.Saml2Config(onelogin_settings = self.onelogin_settings)

    @classmethod
    def from_dict(cls, s_dict: Saml2Config) -> 'Saml2':
        config = s_dict.get('onelogin_settings', {})
        return Saml2(config)


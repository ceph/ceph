from typing import Any

from .auth import BaseAuth, SSOAuth


class Saml2(SSOAuth):
    LOGIN_URL = 'auth/saml2/login'
    LOGOUT_URL = 'auth/saml2/slo'
    sso = True

    class Saml2Config(BaseAuth.Config):
        onelogin_settings: Any

    def __init__(self, onelogin_settings):
        self.onelogin_settings = onelogin_settings

    def get_username_attribute(self):
        return self.onelogin_settings['sp']['attributeConsumingService']['requestedAttributes'][0][
            'name']

    def to_dict(self) -> 'Saml2Config':
        return {
            'onelogin_settings': self.onelogin_settings
        }

    @classmethod
    def from_dict(cls, s_dict: Saml2Config) -> 'Saml2':
        try:
            return Saml2(s_dict['onelogin_settings'])
        except KeyError:
            return Saml2({})

    @classmethod
    def get_auth_name(cls):
        return cls.__name__.lower()

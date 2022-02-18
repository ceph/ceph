from ._router import Router


class UIRouter(Router):
    def __init__(self, path, security_scope=None, secure=True):
        super().__init__(path, base_url="/ui-api",
                         security_scope=security_scope,
                         secure=secure)

    def __call__(self, cls):
        cls = super().__call__(cls)
        cls._api_endpoint = False
        return cls

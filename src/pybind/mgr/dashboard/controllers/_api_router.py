from ._router import Router


class APIRouter(Router):
    def __init__(self, path, security_scope=None, secure=True):
        super().__init__(path, base_url="/api",
                         security_scope=security_scope,
                         secure=secure)

    def __call__(self, cls):
        cls = super().__call__(cls)
        cls._api_endpoint = True
        return cls

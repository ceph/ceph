from pecan import request, response
from base64 import b64decode
from functools import wraps

import traceback

import module


# Handle authorization
def auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not request.authorization:
            response.status = 401
            response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
            return {'message': 'auth: No HTTP username/password'}

        username, password = b64decode(request.authorization[1]).split(':')

        # Check that the username exists
        if username not in module.instance.keys:
            response.status = 401
            response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
            return {'message': 'auth: No such user'}

        # Check the password
        if module.instance.keys[username] != password:
            response.status = 401
            response.headers['WWW-Authenticate'] = 'Basic realm="Login Required"'
            return {'message': 'auth: Incorrect password'}

        return f(*args, **kwargs)
    return decorated


# Helper function to lock the function
def lock(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        with module.instance.requests_lock:
            return f(*args, **kwargs)
    return decorated

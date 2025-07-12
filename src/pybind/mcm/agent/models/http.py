from enum import Enum

class HttpMethod(Enum):
    GET     = "GET"
    POST    = "POST"
    PUT     = "PUT"
    DELETE  = "DELETE"
    PATCH   = "PATCH"
    OPTIONS = "OPTIONS"
    HEAD    = "HEAD"
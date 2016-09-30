
"""
Convenience classes for raising HTTP status codes as exceptions,
in addition to rest_framework's builtin exception classes
"""

from rest_framework import status
from rest_framework.exceptions import APIException


class ServiceUnavailable(APIException):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    default_detail = "Service unavailable"

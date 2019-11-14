import sys

PY3 = False

if sys.version_info >= (3, 0):
    PY3 = True

if PY3:
    from urllib.parse import parse_qs, urljoin, urlparse, urlencode # noqa: F401
    from urllib.request import urlopen, Request # noqa: F401
    from urllib.error import HTTPError # noqa: F401
else:
    from urlparse import parse_qs, urljoin, urlparse # noqa: F401
    from urllib import urlencode # noqa: F401
    from urllib2 import urlopen, Request, HTTPError # noqa: F401


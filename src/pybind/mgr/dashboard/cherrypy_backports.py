# -*- coding: utf-8 -*-
"""
Copyright © 2004-2019, CherryPy Team (team@cherrypy.org)

All rights reserved.

* * *

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of CherryPy nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

from distutils.version import StrictVersion

# The SSL code in CherryPy 3.5.0 is buggy.  It was fixed long ago,
# but 3.5.0 is still shipping in major linux distributions
# (Fedora 27, Ubuntu Xenial), so we must monkey patch it to get SSL working.


def patch_http_connection_init(v):
    # It was fixed in 3.7.0.  Exact lower bound version is probably earlier,
    # but 3.5.0 is what this monkey patch is tested on.
    if StrictVersion("3.5.0") <= v < StrictVersion("3.7.0"):
        from cherrypy.wsgiserver.wsgiserver2 import \
            HTTPConnection, CP_fileobject

        def fixed_init(hc_self, server, sock, makefile=CP_fileobject):
            hc_self.server = server
            hc_self.socket = sock
            hc_self.rfile = makefile(sock, "rb", hc_self.rbufsize)
            hc_self.wfile = makefile(sock, "wb", hc_self.wbufsize)
            hc_self.requests_seen = 0

        HTTPConnection.__init__ = fixed_init


# When the CherryPy server in 3.2.2 (and later) starts it attempts to verify
# that the ports its listening on are in fact bound. When using the any address
# "::" it tries both ipv4 and ipv6, and in some environments (e.g. kubernetes)
# ipv6 isn't yet configured / supported and CherryPy throws an uncaught
# exception.
def skip_wait_for_occupied_port(v):
    # the issue was fixed in 3.2.3. it's present in 3.2.2 (current version on
    # centos:7) and back to at least 3.0.0.
    if StrictVersion("3.1.2") <= v < StrictVersion("3.2.3"):
        # https://github.com/cherrypy/cherrypy/issues/1100
        from cherrypy.process import servers
        servers.wait_for_occupied_port = lambda host, port: None


# cherrypy.wsgiserver was extracted wsgiserver into cheroot in cherrypy v9.0.0
def patch_builtin_ssl_wrap(v, new_wrap):
    if v < StrictVersion("9.0.0"):
        from cherrypy.wsgiserver.ssl_builtin import BuiltinSSLAdapter as builtin_ssl
    else:
        from cheroot.ssl.builtin import BuiltinSSLAdapter as builtin_ssl
    builtin_ssl.wrap = new_wrap(builtin_ssl.wrap)


def accept_exceptions_from_builtin_ssl(v):
    # the fix was included by cheroot v5.2.0, which was included by cherrypy
    # 10.2.0.
    if v < StrictVersion("10.2.0"):
        # see https://github.com/cherrypy/cheroot/pull/4
        import ssl

        def accept_ssl_errors(func):
            def wrapper(self, sock):
                try:
                    return func(self, sock)
                except ssl.SSLError as e:
                    if e.errno == ssl.SSL_ERROR_SSL:
                        # Check if it's one of the known errors
                        # Errors that are caught by PyOpenSSL, but thrown by
                        # built-in ssl
                        _block_errors = ('unknown protocol', 'unknown ca',
                                         'unknown_ca', 'inappropriate fallback',
                                         'wrong version number',
                                         'no shared cipher', 'certificate unknown',
                                         'ccs received early')
                        for error_text in _block_errors:
                            if error_text in e.args[1].lower():
                                # Accepted error, let's pass
                                return None, {}
                        raise
            return wrapper
        patch_builtin_ssl_wrap(v, accept_ssl_errors)


def accept_socket_error_0(v):
    # see https://github.com/cherrypy/cherrypy/issues/1618
    try:
        import cheroot
        cheroot_version = cheroot.__version__
    except ImportError:
        pass

    if v < StrictVersion("9.0.0") or cheroot_version < StrictVersion("6.5.5"):
        generic_socket_error = OSError

        def accept_socket_error_0(func):
            def wrapper(self, sock):
                try:
                    return func(self, sock)
                except generic_socket_error as e:
                    """It is unclear why exactly this happens.

                    It's reproducible only with openssl>1.0 and stdlib ``ssl`` wrapper.
                    In CherryPy it's triggered by Checker plugin, which connects
                    to the app listening to the socket port in TLS mode via plain
                    HTTP during startup (from the same process).

                    Ref: https://github.com/cherrypy/cherrypy/issues/1618
                    """
                    import ssl
                    is_error0 = e.args == (0, 'Error')
                    IS_ABOVE_OPENSSL10 = ssl.OPENSSL_VERSION_INFO >= (1, 1)
                    del ssl
                    if is_error0 and IS_ABOVE_OPENSSL10:
                        return None, {}
                    raise
            return wrapper
        patch_builtin_ssl_wrap(v, accept_socket_error_0)


def patch_request_unique_id(v):
    """
    Older versions of cherrypy don't include request.unique_id field (a lazily
    calculated UUID4).

    Monkey-patching is preferred over alternatives as inheritance, as it'd break
    type checks (cherrypy/lib/cgtools.py: `isinstance(obj, _cprequest.Request)`)
    """
    if v < StrictVersion('11.1.0'):
        import uuid
        from functools import update_wrapper
        from cherrypy._cprequest import Request

        class LazyUUID4(object):
            def __str__(self):
                """Return UUID4 and keep it for future calls."""
                return str(self.uuid4)

            @property
            def uuid4(self):
                """Provide unique id on per-request basis using UUID4.
                It's evaluated lazily on render.
                """
                try:
                    self._uuid4  # type: ignore
                except AttributeError:
                    # evaluate on first access
                    self._uuid4 = uuid.uuid4()

                return self._uuid4

        old_init = Request.__init__

        def init_with_unique_id(self, *args, **kwargs):
            old_init(self, *args, **kwargs)
            self.unique_id = LazyUUID4()

        Request.__init__ = update_wrapper(init_with_unique_id, old_init)


def patch_cherrypy(v):
    patch_http_connection_init(v)
    skip_wait_for_occupied_port(v)
    accept_exceptions_from_builtin_ssl(v)
    accept_socket_error_0(v)
    patch_request_unique_id(v)

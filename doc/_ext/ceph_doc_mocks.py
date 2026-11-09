"""Mocks used while importing mgr modules for the documentation."""

import sys
from unittest import mock


def install_cherrypy_mock():
    """Make the CherryPy mock available to full and incremental builds."""
    cherrypy = mock.Mock(__version__='3.2.3')
    cherrypy.expose = expose
    for name in ('allow', 'json_in', 'json_out'):
        setattr(cherrypy.tools, name, identity_tool)
    sys.modules['cherrypy'] = cherrypy
    for name in ('cherrypy.process',
                 'cherrypy.process.servers',
                 'cherrypy._cptree',
                 'cheroot',
                 'cheroot.wsgi',
                 'cheroot.ssl',
                 'cheroot.ssl.builtin'):
        sys.modules.setdefault(name, mock.Mock())


def expose(func=None, **_kwargs):
    def mark_exposed(func):
        func.exposed = True
        return func

    if callable(func):
        return mark_exposed(func)
    return mark_exposed


def identity_tool(*_args, **_kwargs):
    return lambda func: func


def setup(app):
    if 'cherrypy' not in sys.modules:
        install_cherrypy_mock()
    return {'parallel_read_safe': True, 'parallel_write_safe': True}

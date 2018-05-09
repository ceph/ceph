# -*- coding: utf-8 -*-
from __future__ import absolute_import


class ViewCacheNoDataException(Exception):
    def __init__(self):
        self.status = 200
        super(ViewCacheNoDataException, self).__init__('ViewCache: unable to retrieve data')


class DashboardException(Exception):
    """
    Used for exceptions that are already handled and should end up as a user error.
    Or, as a replacement for cherrypy.HTTPError(...)

    Typically, you don't inherent from DashboardException
    """

    # pylint: disable=too-many-arguments
    def __init__(self, e=None, code=None, component=None, http_status_code=None, msg=None):
        super(DashboardException, self).__init__(msg)
        self._code = code
        self.component = component
        if e:
            self.e = e
        if http_status_code:
            self.status = http_status_code
        else:
            self.status = 400

    def __str__(self):
        try:
            return str(self.e)
        except AttributeError:
            return super(DashboardException, self).__str__()

    @property
    def errno(self):
        return self.e.errno

    @property
    def code(self):
        if self._code:
            return str(self._code)
        return str(abs(self.errno))

# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..tools import ApiController, AuthRequired, RESTController


@ApiController('host')
@AuthRequired()
class Host(RESTController):
    def list(self):
        return self.mgr.list_servers()

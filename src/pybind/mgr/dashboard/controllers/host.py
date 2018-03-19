# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .. import mgr
from ..tools import ApiController, AuthRequired, RESTController


@ApiController('host')
@AuthRequired()
class Host(RESTController):
    def list(self):
        return mgr.list_servers()

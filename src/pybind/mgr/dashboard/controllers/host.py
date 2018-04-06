# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, AuthRequired, RESTController
from .. import mgr


@ApiController('host')
@AuthRequired()
class Host(RESTController):
    def list(self):
        return mgr.list_servers()

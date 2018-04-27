# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from .. import mgr
from ..security import Scope


@ApiController('/host', Scope.HOSTS)
class Host(RESTController):
    def list(self):
        return mgr.list_servers()

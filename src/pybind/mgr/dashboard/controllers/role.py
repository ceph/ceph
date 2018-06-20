# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from ..security import Scope
from ..services.access_control import ACCESS_CTRL_DB, SYSTEM_ROLES


@ApiController('/role', Scope.USER)
class Role(RESTController):
    def list(self):
        all_roles = dict(ACCESS_CTRL_DB.roles)
        all_roles.update(SYSTEM_ROLES)
        items = sorted(all_roles.items(), key=lambda role: role[1].name)
        return [r.to_dict() for _, r in items]

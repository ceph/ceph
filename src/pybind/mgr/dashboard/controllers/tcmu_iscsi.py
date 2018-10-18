# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from ..security import Scope
from ..services.tcmu_service import TcmuService


@ApiController('/tcmuiscsi', Scope.ISCSI)
class TcmuIscsi(RESTController):

    def list(self):
        return TcmuService.get_iscsi_info()

# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController
from ..security import Scope

@ApiController('/crush_ruleset', Scope.OSD)
class CrushRuleset(RESTController):
    # TODO
    pass

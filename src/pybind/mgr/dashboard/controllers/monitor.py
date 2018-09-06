# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from . import ApiController, Endpoint, BaseController, ReadPermission
from .. import mgr
from ..security import Scope


@ApiController('/monitor', Scope.MONITOR)
class Monitor(BaseController):
    @Endpoint()
    @ReadPermission
    def __call__(self):
        in_quorum, out_quorum = [], []

        counters = ['mon.num_sessions']

        mon_status_json = mgr.get("mon_status")
        mon_status = json.loads(mon_status_json['json'])

        for mon in mon_status["monmap"]["mons"]:
            mon["stats"] = {}
            for counter in counters:
                data = mgr.get_counter("mon", mon["name"], counter)
                if data is not None:
                    mon["stats"][counter.split(".")[1]] = data[counter]
                else:
                    mon["stats"][counter.split(".")[1]] = []
            if mon["rank"] in mon_status["quorum"]:
                in_quorum.append(mon)
            else:
                out_quorum.append(mon)

        return {
            'mon_status': mon_status,
            'in_quorum': in_quorum,
            'out_quorum': out_quorum
        }

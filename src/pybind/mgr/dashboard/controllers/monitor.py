# -*- coding: utf-8 -*-

import json

from .. import mgr
from ..security import Scope
from . import APIDoc, APIRouter, BaseController, Endpoint, EndpointDoc, ReadPermission

MONITOR_SCHEMA = {
    "mon_status": ({
        "name": (str, ""),
        "rank": (int, ""),
        "state": (str, ""),
        "election_epoch": (int, ""),
        "quorum": ([int], ""),
        "quorum_age": (int, ""),
        "features": ({
            "required_con": (str, ""),
            "required_mon": ([int], ""),
            "quorum_con": (str, ""),
            "quorum_mon": ([str], "")
        }, ""),
        "outside_quorum": ([str], ""),
        "extra_probe_peers": ([str], ""),
        "sync_provider": ([str], ""),
        "monmap": ({
            "epoch": (int, ""),
            "fsid": (str, ""),
            "modified": (str, ""),
            "created": (str, ""),
            "min_mon_release": (int, ""),
            "min_mon_release_name": (str, ""),
            "features": ({
                "persistent": ([str], ""),
                "optional": ([str], "")
            }, ""),
            "mons": ([{
                "rank": (int, ""),
                "name": (str, ""),
                "public_addrs": ({
                    "addrvec": ([{
                        "type": (str, ""),
                        "addr": (str, ""),
                        "nonce": (int, "")
                    }], "")
                }, ""),
                "addr": (str, ""),
                "public_addr": (str, ""),
                "priority": (int, ""),
                "weight": (int, ""),
                "stats": ({
                    "num_sessions": ([int], ""),
                }, "")
            }], "")
        }, ""),
        "feature_map": ({
            "mon": ([{
                "features": (str, ""),
                "release": (str, ""),
                "num": (int, "")
            }], ""),
            "mds": ([{
                "features": (str, ""),
                "release": (str, ""),
                "num": (int, "")
            }], ""),
            "client": ([{
                "features": (str, ""),
                "release": (str, ""),
                "num": (int, "")
            }], ""),
            "mgr": ([{
                "features": (str, ""),
                "release": (str, ""),
                "num": (int, "")
            }], ""),
        }, "")
    }, ""),
    "in_quorum": ([{
        "rank": (int, ""),
        "name": (str, ""),
        "public_addrs": ({
            "addrvec": ([{
                "type": (str, ""),
                "addr": (str, ""),
                "nonce": (int, "")
            }], "")
        }, ""),
        "addr": (str, ""),
        "public_addr": (str, ""),
        "priority": (int, ""),
        "weight": (int, ""),
        "stats": ({
            "num_sessions": ([int], "")
        }, "")
    }], ""),
    "out_quorum": ([int], "")
}


@APIRouter('/monitor', Scope.MONITOR)
@APIDoc("Get Monitor Details", "Monitor")
class Monitor(BaseController):
    @Endpoint()
    @ReadPermission
    @EndpointDoc("Get Monitor Details",
                 responses={200: MONITOR_SCHEMA})
    def __call__(self):
        in_quorum, out_quorum = [], []

        counters = ['mon.num_sessions']

        mon_status_json = mgr.get("mon_status")
        mon_status = json.loads(mon_status_json['json'])

        for mon in mon_status["monmap"]["mons"]:
            mon["stats"] = {}
            for counter in counters:
                data = mgr.get_unlabeled_counter("mon", mon["name"], counter)
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

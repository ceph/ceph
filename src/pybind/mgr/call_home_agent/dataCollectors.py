from typing import List, Any, Tuple, Dict, Optional

sample_inventory = {
    "_source": {
        "id": "c549a36a-b377-11ed-b55e-525400bfa136",
        "daemons": [
            {
                "mon": 3
            },
            {
                "mgr": 2
            },
            {
                "osd": 12,
                "up": 12,
                "in": 12
            },
            {
                "rgw": 3
            },
            {
                "alertmanager": 1
            },
            {
                "prometheus": 1
            },
            {
                "ceph-exporter": 3
            },
            {
                "node-      },exporter": 3
            },
            {
                "grafana": 1
            }
        ],
        "health": "HEALTH_WARN",
        "pools": 12,
        "pgs": 3,
        "objects": 200,
        "capacity_used_Gib": 0.7,
        "capacity_available_Gib": 15,
        "alerts": [
            {
                "name": "cephadmdaemonfailed",
                "summary": "A ceph daemon manged by cephadm is down",
                "description": "A daemon managed by cephadm is no longer active. Determine, which daemon is down with 'ceph health detail'. you may start daemons with the 'ceph orch daemon start <daemon_id>'",
                "severity": "critical",
                "state": "active",
                "started": "7 hours ago"
            },
            {
                "name": "CephHealthWarning",
                "summary": "Ceph is in the WARNING state",
                "description": "The cluster state has been HEALTH_WARN for more than 15 minutes. Please check 'ceph health detail' for more information.",
                "severity": "warning",
                "state": "active",
                "started": "7 hours ago"
            }
        ]
    }
}


def inventory() -> Dict[str, Dict[str, Any]]:
    return sample_inventory

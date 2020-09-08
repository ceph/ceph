# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import ApiController, RESTController, ControllerDoc, EndpointDoc
from .. import mgr
from ..exceptions import DashboardException
from ..security import Scope

REPORT_SCHEMA = {
    "report": ({
        "leaderboard": (bool, ""),
        "report_version": (int, ""),
        "report_timestamp": (str, ""),
        "report_id": (str, ""),
        "channels": ([str], ""),
        "channels_available": ([str], ""),
        "license": (str, ""),
        "created": (str, ""),
        "mon": ({
            "count": (int, ""),
            "features": ({
                "persistent": ([str], ""),
                "optional": ([int], "")
            }, ""),
            "min_mon_release": (int, ""),
            "v1_addr_mons": (int, ""),
            "v2_addr_mons": (int, ""),
            "ipv4_addr_mons": (int, ""),
            "ipv6_addr_mons": (int, ""),
        }, ""),
        "config": ({
            "cluster_changed": ([str], ""),
            "active_changed": ([str], "")
        }, ""),
        "rbd": ({
            "num_pools": (int, ""),
            "num_images_by_pool": ([int], ""),
            "mirroring_by_pool": ([bool], ""),
        }, ""),
        "pools": ([{
            "pool": (int, ""),
            "type": (str, ""),
            "pg_num": (int, ""),
            "pgp_num": (int, ""),
            "size": (int, ""),
            "min_size": (int, ""),
            "pg_autoscale_mode": (str, ""),
            "target_max_bytes": (int, ""),
            "target_max_objects": (int, ""),
            "erasure_code_profile": (str, ""),
            "cache_mode": (str, ""),
        }], ""),
        "osd": ({
            "count": (int, ""),
            "require_osd_release": (str, ""),
            "require_min_compat_client": (str, ""),
            "cluster_network": (bool, ""),
        }, ""),
        "crush": ({
            "num_devices": (int, ""),
            "num_types": (int, ""),
            "num_buckets": (int, ""),
            "num_rules": (int, ""),
            "device_classes": ([int], ""),
            "tunables": ({
                "choose_local_tries": (int, ""),
                "choose_local_fallback_tries": (int, ""),
                "choose_total_tries": (int, ""),
                "chooseleaf_descend_once": (int, ""),
                "chooseleaf_vary_r": (int, ""),
                "chooseleaf_stable": (int, ""),
                "straw_calc_version": (int, ""),
                "allowed_bucket_algs": (int, ""),
                "profile": (str, ""),
                "optimal_tunables": (int, ""),
                "legacy_tunables": (int, ""),
                "minimum_required_version": (str, ""),
                "require_feature_tunables": (int, ""),
                "require_feature_tunables2": (int, ""),
                "has_v2_rules": (int, ""),
                "require_feature_tunables3": (int, ""),
                "has_v3_rules": (int, ""),
                "has_v4_buckets": (int, ""),
                "require_feature_tunables5": (int, ""),
                "has_v5_rules": (int, ""),
            }, ""),
            "compat_weight_set": (bool, ""),
            "num_weight_sets": (int, ""),
            "bucket_algs": ({
                "straw2": (int, ""),
            }, ""),
            "bucket_sizes": ({
                "1": (int, ""),
                "3": (int, ""),
            }, ""),
            "bucket_types": ({
                "1": (int, ""),
                "11": (int, ""),
            }, ""),
        }, ""),
        "fs": ({
            "count": (int, ""),
            "feature_flags": ({
                "enable_multiple": (bool, ""),
                "ever_enabled_multiple": (bool, ""),
            }, ""),
            "num_standby_mds": (int, ""),
            "filesystems": ([int], ""),
            "total_num_mds": (int, ""),
        }, ""),
        "metadata": ({
            "osd": ({
                "osd_objectstore": ({
                    "bluestore": (int, ""),
                }, ""),
                "rotational": ({
                    "1": (int, ""),
                }, ""),
                "arch": ({
                    "x86_64": (int, ""),
                }, ""),
                "ceph_version": ({
                    "ceph version 16.0.0-3151-gf202994fcf": (int, ""),
                }, ""),
                "os": ({
                    "Linux": (int, ""),
                }, ""),
                "cpu": ({
                    "Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz": (int, ""),
                }, ""),
                "kernel_description": ({
                    "#1 SMP Wed Jul 1 19:53:01 UTC 2020": (int, ""),
                }, ""),
                "kernel_version": ({
                    "5.7.7-200.fc32.x86_64": (int, ""),
                }, ""),
                "distro_description": ({
                    "CentOS Linux 8 (Core)": (int, ""),
                }, ""),
                "distro": ({
                    "centos": (int, ""),
                }, ""),
            }, ""),
            "mon": ({
                "arch": ({
                    "x86_64": (int, ""),
                }, ""),
                "ceph_version": ({
                    "ceph version 16.0.0-3151-gf202994fcf": (int, ""),
                }, ""),
                "os": ({
                    "Linux": (int, ""),
                }, ""),
                "cpu": ({
                    "Intel(R) Core(TM) i7-8665U CPU @ 1.90GHz": (int, ""),
                }, ""),
                "kernel_description": ({
                    "#1 SMP Wed Jul 1 19:53:01 UTC 2020": (int, ""),
                }, ""),
                "kernel_version": ({
                    "5.7.7-200.fc32.x86_64": (int, ""),
                }, ""),
                "distro_description": ({
                    "CentOS Linux 8 (Core)": (int, ""),
                }, ""),
                "distro": ({
                    "centos": (int, ""),
                }, ""),
            }, ""),
        }, ""),
        "hosts": ({
            "num": (int, ""),
            "num_with_mon": (int, ""),
            "num_with_mds": (int, ""),
            "num_with_osd": (int, ""),
            "num_with_mgr": (int, ""),
        }, ""),
        "usage": ({
            "pools": (int, ""),
            "pg_num": (int, ""),
            "total_used_bytes": (int, ""),
            "total_bytes": (int, ""),
            "total_avail_bytes": (int, ""),
        }, ""),
        "services": ({
            "rgw": (int, ""),
        }, ""),
        "rgw": ({
            "count": (int, ""),
            "zones": (int, ""),
            "zonegroups": (int, ""),
            "frontends": ([str], "")
        }, ""),
        "balancer": ({
            "active": (bool, ""),
            "mode": (str, ""),
        }, ""),
        "crashes": ([int], "")
    }, ""),
    "device_report": (str, "")
}


@ApiController('/telemetry', Scope.CONFIG_OPT)
@ControllerDoc("Display Telemetry Report", "Telemetry")
class Telemetry(RESTController):

    @RESTController.Collection('GET')
    @EndpointDoc("Get Detailed Telemetry report",
                 responses={200: REPORT_SCHEMA})
    def report(self):
        """
        Get Ceph and device report data
        :return: Ceph and device report data
        :rtype: dict
        """
        return mgr.remote('telemetry', 'get_report', 'all')

    def singleton_set(self, enable=True, license_name=None):
        """
        Enables or disables sending data collected by the Telemetry
        module.
        :param enable: Enable or disable sending data
        :type enable: bool
        :param license_name: License string e.g. 'sharing-1-0' to
        make sure the user is aware of and accepts the license
        for sharing Telemetry data.
        :type license_name: string
        """
        if enable:
            if not license_name or (license_name != 'sharing-1-0'):
                raise DashboardException(
                    code='telemetry_enable_license_missing',
                    msg='Telemetry data is licensed under the Community Data License Agreement - '
                        'Sharing - Version 1.0 (https://cdla.io/sharing-1-0/). To enable, add '
                        '{"license": "sharing-1-0"} to the request payload.'
                )
            mgr.remote('telemetry', 'on')
        else:
            mgr.remote('telemetry', 'off')

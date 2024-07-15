import os
from depoly_tools import exec_cmd
from depoly_tools import rpm_top_dir
from depoly_tools import rpm_version

target=exec_cmd("hostname")
exec_cmd("sleep 1")
nice="min"
if (nice == "min"):
    exec_cmd("ceph tell osd.* injectargs '--osd_recovery_max_single_start 1 --osd_recovery_sleep_hdd 0.1 --osd_recovery_max_active 1 --osd_max_backfills 1'")
else:
    exec_cmd("ceph tell osd.* injectargs '--osd_recovery_max_single_start 64 --osd_recovery_sleep_hdd 0 --osd_recovery_max_active 64 --osd_max_backfills 64'"

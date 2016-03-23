#!/usr/bin/env python

import json
import re
import subprocess

disks = json.loads(subprocess.check_output("ceph-disk list --format json", shell=True))
used = 0

for disk in disks:
    for partition in disk.get('partition', []):
        if partition.get('type') == 'data':
            df = subprocess.check_output("df --output=used " + partition['path'], shell=True)
            used += int(re.findall('\d+', df)[0])

open("/etc/rhsm/facts/ceph_usage.facts", 'w').write("""
{
"band.storage.usage": {used}
}
""".format(used=used/(1024*1024*1024)))

#!/usr/bin/env bash

set -ex

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 8 ]; then
    echo "test requires at least 8 OSDs up and running"
    exit 1
fi

ceph mon set election_strategy connectivity
ceph mon add disallowed_leader c

for zone in iris pze
    do
      ceph osd crush add-bucket $zone zone
      ceph osd crush move $zone root=default
    done

ceph osd crush add-bucket node-2 host
ceph osd crush add-bucket node-3 host

ceph osd crush move node-2 zone=iris
ceph osd crush move node-3 zone=pze

ceph osd crush move osd.0 host=node-2
ceph osd crush move osd.1 host=node-2
ceph osd crush move osd.2 host=node-2
ceph osd crush move osd.3 host=node-2
ceph osd crush move osd.4 host=node-3
ceph osd crush move osd.5 host=node-3
ceph osd crush move osd.6 host=node-3
ceph osd crush move osd.7 host=node-3


ceph mon set_location a zone=iris host=node-2
ceph mon set_location b zone=pze host=node-3

hostname=$(hostname -s)
ceph osd crush remove $hostname ||  { echo 'command failed' ; exit 1; }
ceph osd getcrushmap > crushmap ||  { echo 'command failed' ; exit 1; }
crushtool --decompile crushmap > crushmap.txt ||  { echo 'command failed' ; exit 1; }
sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt || { echo 'command failed' ; exit 1; }
cat >> crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take iris
        step chooseleaf firstn 2 type host
        step emit
        step take pze
        step chooseleaf firstn 2 type host
        step emit
}
# end crush map
EOF

crushtool --compile crushmap_modified.txt -o crushmap.bin || { echo 'command failed' ; exit 1; }
ceph osd setcrushmap -i crushmap.bin  || { echo 'command failed' ; exit 1; }
stretched_poolname=stretch_pool
ceph osd pool create $stretched_poolname 32 32 stretch_rule || { echo 'command failed' ; exit 1; }
ceph osd pool set $stretched_poolname size 4 || { echo 'command failed' ; exit 1; }
ceph osd pool application enable $stretched_poolname rados || { echo 'command failed' ; exit 1; }
ceph mon set_location c zone=arbiter host=node-1 || { echo 'command failed' ; exit 1; }
ceph mon enable_stretch_mode c stretch_rule zone || { echo 'command failed' ; exit 1; } # Enter strech mode

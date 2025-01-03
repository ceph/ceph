#!/usr/bin/env bash

set -ex

NUM_OSDS_UP=$(ceph osd df | grep "up" | wc -l)

if [ $NUM_OSDS_UP -lt 8 ]; then
    echo "test requires at least 8 OSDs up and running"
    exit 1
fi

ceph mon set election_strategy connectivity
ceph mon add disallowed_leader e

for dc in dc1 dc2
    do
      ceph osd crush add-bucket $dc datacenter
      ceph osd crush move $dc root=default
    done

ceph osd crush add-bucket node-2 host
ceph osd crush add-bucket node-3 host
ceph osd crush add-bucket node-4 host
ceph osd crush add-bucket node-5 host
ceph osd crush add-bucket node-6 host
ceph osd crush add-bucket node-7 host
ceph osd crush add-bucket node-8 host
ceph osd crush add-bucket node-9 host

ceph osd crush move node-2 datacenter=dc1
ceph osd crush move node-3 datacenter=dc1
ceph osd crush move node-4 datacenter=dc1
ceph osd crush move node-5 datacenter=dc1

ceph osd crush move node-6 datacenter=dc2
ceph osd crush move node-7 datacenter=dc2
ceph osd crush move node-8 datacenter=dc2
ceph osd crush move node-9 datacenter=dc2

ceph osd crush move osd.0 host=node-2
ceph osd crush move osd.1 host=node-3
ceph osd crush move osd.2 host=node-4
ceph osd crush move osd.3 host=node-5

ceph osd crush move osd.4 host=node-6
ceph osd crush move osd.5 host=node-7
ceph osd crush move osd.6 host=node-8
ceph osd crush move osd.7 host=node-9


ceph mon set_location a datacenter=dc1 host=node-2
ceph mon set_location b datacenter=dc1 host=node-3
ceph mon set_location c datacenter=dc2 host=node-6
ceph mon set_location d datacenter=dc2 host=node-7

hostname=$(hostname -s)
ceph osd crush remove $hostname ||  { echo 'command failed' ; exit 1; }
ceph osd getcrushmap > crushmap ||  { echo 'command failed' ; exit 1; }
crushtool --decompile crushmap > crushmap.txt ||  { echo 'command failed' ; exit 1; }
sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt || { echo 'command failed' ; exit 1; }
cat >> crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        step take dc1
        step chooseleaf firstn 2 type host
        step emit
        step take dc2
        step chooseleaf firstn 2 type host
        step emit
}
# rule stretch_rule {
#         id 1
#         type replicated
#         step take default
#         step chooseleaf firstn 2 type datacenter
#         step chooseleaf firstn 2 type host
#         step emit
# }
# end crush map
EOF

crushtool --compile crushmap_modified.txt -o crushmap.bin || { echo 'command failed' ; exit 1; }
ceph osd setcrushmap -i crushmap.bin  || { echo 'command failed' ; exit 1; }
stretched_poolname=stretch_pool
ceph osd pool create $stretched_poolname 32 32 stretch_rule || { echo 'command failed' ; exit 1; }
ceph osd pool set $stretched_poolname size 4 || { echo 'command failed' ; exit 1; }
ceph osd pool application enable $stretched_poolname rados || { echo 'command failed' ; exit 1; }
ceph mon set_location e datacenter=arbiter host=node-1 || { echo 'command failed' ; exit 1; }
ceph mon enable_stretch_mode e stretch_rule datacenter || { echo 'command failed' ; exit 1; } # Enter strech mode

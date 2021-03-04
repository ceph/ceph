#!/usr/bin/env bash

set -x

./bin/ceph config set osd osd_crush_update_on_start false

./bin/ceph osd crush move osd.0 host=host1-1 datacenter=site1 root=default
./bin/ceph osd crush move osd.1 host=host1-2 datacenter=site1 root=default
./bin/ceph osd crush move osd.2 host=host2-1 datacenter=site2 root=default
./bin/ceph osd crush move osd.3 host=host2-2 datacenter=site2 root=default

./bin/ceph osd getcrushmap > crush.map.bin
./bin/crushtool -d crush.map.bin -o crush.map.txt
cat <<EOF >> crush.map.txt
rule stretch_rule {
        id 1
        type replicated
        min_size 1
        max_size 10
        step take site1
        step chooseleaf firstn 2 type host
        step emit
        step take site2
        step chooseleaf firstn 2 type host
        step emit
}
EOF
./bin/crushtool -c crush.map.txt -o crush2.map.bin
./bin/ceph osd setcrushmap -i crush2.map.bin
./bin/ceph mon set election_strategy connectivity

./bin/ceph mon set_location a datacenter=site1
./bin/ceph mon set_location b datacenter=site2
./bin/ceph mon set_location c datacenter=site3
./bin/ceph osd pool create test_stretch1 300 300 replicated
./bin/ceph mon enable_stretch_mode c stretch_rule datacenter

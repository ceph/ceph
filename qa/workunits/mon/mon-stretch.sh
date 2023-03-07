# Before Stretch

ceph orch device ls
ceph osd tree
ceph mon dump

ceph mon set election_strategy connectivity

ceph mon add disallowed_leader e

# Entering Stretch
for datacenter in DC1 DC2
do
  ceph osd crush add-bucket $datacenter datacenter
  ceph osd crush move $datacenter root=default
done

# ceph osd crush add-bucket node-1 host
# ceph osd crush add-bucket node-2 host
# ceph osd crush add-bucket node-3 host
# ceph osd crush add-bucket node-4 host


ceph osd crush move host.1 datacenter=DC1
ceph osd crush move host.2 datacenter=DC1
ceph osd crush move host.3 datacenter=DC2
ceph osd crush move host.4 datacenter=DC2

ceph osd crush move osd.0 host=host.1
ceph osd crush move osd.1 host=host.1
ceph osd crush move osd.2 host=host.2
ceph osd crush move osd.3 host=host.2
ceph osd crush move osd.4 host=host.3
ceph osd crush move osd.5 host=host.3
ceph osd crush move osd.6 host=host.4
ceph osd crush move osd.7 host=host.4

ceph mon set_location a datacenter=DC1 host=host.1
ceph mon set_location b datacenter=DC1 host=host.2
ceph mon set_location c datacenter=DC2 host=host.3
ceph mon set_location d datacenter=DC2 host=host.4

hostname=$(hostname -s)
ceph osd crush remove $hostname || echo failed
ceph osd getcrushmap > crushmap || echo failed
crushtool --decompile crushmap > crushmap.txt || echo failed
sed 's/^# end crush map$//' crushmap.txt > crushmap_modified.txt || echo failed
cat >> crushmap_modified.txt << EOF
rule stretch_rule {
        id 1
        type replicated
        min_size 1
        max_size 10
        step take DC1
        step chooseleaf firstn 2 type host
        step emit
        step take DC2
        step chooseleaf firstn 2 type host
        step emit
}

# end crush map
EOF

crushtool --compile crushmap_modified.txt -o crushmap.bin || echo failed
ceph osd setcrushmap -i crushmap.bin  || echo failed
stretched_poolname=stretched_rbdpool
ceph osd pool create $stretched_poolname 32 32 stretch_rule || echo failed
ceph osd pool set $stretched_poolname size 4 || echo failed

sleep 3

ceph mon set_location e datacenter=DC3 host=host.arbiter
ceph mon enable_stretch_mode e stretch_rule datacenter

# After stretchmode

ceph orch device ls
ceph osd tree
ceph mon dump
pool=$1
ceph osd set-require-min-compat-client luminous
ceph osd getmap >./osdmap.bin
osdmaptool --upmap-pool ${pool} ./osdmap.bin --upmap ./upmap.bin --upmap-deviation 2
source ./upmap.bin 
ceph osd df


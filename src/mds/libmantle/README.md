#Mantle - a framework for injectable metadata balancers

##Build Prerequisites
Mantle depends on Lua:

`sudo apt-get install lua liblua5.2-dev`

Enable Mantle at build `./configure --with-mantle` and set the `LUA_LIBS` environment variable.

##Quickstart

On terminal A, start Ceph and configure MDS: 
`cd src; sudo OSD=3 MON=1 MDS=2 ./vstart.sh -n -l; mds/libmantle/configmds.sh`

On terminal B, mount CephFS: 
`cd src; mkdir /mnt/cephfs; (sudo ./ceph-fuse -c ceph.conf /mnt/cephfs -d) > out/log 2>&1 &`

On terminal B, start a create benchmark (create (-C) 100000 (-n) files (-F) at /mnt/cephfs (-d)):
`/path/to/mdtest/mdtest -F -C -n 100000 -d /mnt/cephfs/dir0`

##Operation
Point Mantle at the library directory:

`ceph --admin-daemon </path/to/daemon/socket> config set mds_bal_dir </path/to/this/library/>`

Set values for all policies:

`ceph --admin-daemon </path/to/daemon/socket> config set mds_bal_metaload    "IWR"`

`ceph --admin-daemon </path/to/daemon/socket> config set mds_bal_mdsload     "MDSs[i]["cpu"]"`

`ceph --admin-daemon </path/to/daemon/socket> config set mds_bal_when        "if MDSs[whoami]["cpu"] > 0.8 then"`

`ceph --admin-daemon </path/to/daemon/socket> config set mds_bal_where       "for i=1,#targets do targets[i] = 0 end``"`

##Files
- libmantle: global functions and variables for setting when/where policies

- libbinpacker: strategies for determining how much load to send

- configmds.sh: script that injects policies for the Greedy balancer

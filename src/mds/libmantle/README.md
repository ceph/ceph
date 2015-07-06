#Mantle - a framework for injectable metadata balancers

##Build Prerequisites
Mantle depends in Lua:

`sudo apt-get install lua liblua5.2-dev`

Enable Mantle in when using ./configure and set the `LUA_LIBS` environment variable.

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

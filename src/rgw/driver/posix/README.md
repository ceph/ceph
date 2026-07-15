# POSIX Driver
Standalone Rados Gateway (RGW) on a local POSIX filesystem (Experimental)


## CMake Option
Add below cmake option (enabled by default)

    -DWITH_RADOSGW_POSIX=ON 


## Build

    cd build
    ninja [vstart]


## Running RGW Standalone

### Running by hand

To run RGW by hand, you need to create a config file, and some directories.

The base config for a POSIXDriver run is as follows:

    [client]
        rgw backend store = posix
        rgw config store = dbstore

This will use default locations for things in the filesystem (mostly in /var/lib/ceph), which may not be optimal.  A suggested test config for an unprivileged user would be something like this:

    [global]
        run dir = /BASE/out
        crash dir = /BASE/out
    [client]
        rgw backend store = posix
        rgw config store = dbstore
        rgw posix base path = /BASE/root
        rgw posix userdb path = /BASE/db
        rgw posix database root = /BASE/db
        dbstore config uri = file:///BASE/db/config.db

replacing BASE with whatever path you want, and obviously add logging if you want that.  RGW needs it's directories created, so

    mkdir -p /BASE/out /BASE/root /BASE/db

Then, you can start RGW with the following command:

    /path/to/radosgw -d -c /BASE/ceph.conf -n client.rgw.8000 '--rgw_frontends=beast port=8000'

That's it.  RGW is listening, and you can use s3cmd or aws-cli, or whatever to access it.  The data is stored in /BASE/root if you want to look at it.  The user is the default `vstart` user `testid`

### Running via vstart.sh

To start the `vstart` cluster, run the following cmd:

    MON=0 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -n -d --rgw_store posix

The above vstart command brings up RGW server on POSIXDriver. It creates default zonegroup, zone and few default users (e.g., testid) to be used for s3 operations.

### Admin

`radosgw-admin` command can be used to create and remove other users, zonegroups and zones.


# POSIX Driver
Standalone Rados Gateway (RGW) on a local POSIX filesystem (Experimental)


## CMake Option
Add below cmake option (enabled by default)

    -DWITH_RADOSGW_POSIX=ON 


## Build

    cd build
    ninja [vstart]


## Running Test cluster
Currently, POSIXDriver depends on DBStore for user storage.  This will change, eventually, but for now, it's run as a filter on top of DBStore.  Not that only users are stored in DBStore, the rest is in the POSIX filesystem.
Edit ceph.conf to add below option

    [client]
        rgw backend store = dbstore
        rgw config store = dbstore
        rgw filter = posix

Start vstart cluster

    MON=0 OSD=0 MDS=0 MGR=0 RGW=1 ../src/vstart.sh -o rgw_backend_store=dbstore -o rgw_config_store=dbstore -o rgw_filter=posix -n -d

The above vstart command brings up RGW server on POSIXDriver. It creates default zonegroup, zone and few default users (eg., testid) to be used for s3 operations.

`radosgw-admin` can be used to create and remove other users, zonegroups and zones.

By default, the directory exported is *'/tmp/rgw_posix_driver'*.   This can be changed with the `rgw_posix_base_path` option, either in ceph.conf or on the vstart command line above.

The POSIXDriver keeps a LMDB based cache of directories, so that it can provide ordered listings.  This directory lives in `rgw_posix_database_root`, which by default is in *'/var/lib/ceph/radosgw'*


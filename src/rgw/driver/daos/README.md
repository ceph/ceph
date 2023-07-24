# DAOS

Standalone RADOS Gateway (RGW) on [DAOS](http://daos.io/) (Experimental)

## CMake Option

Add below cmake option

```bash
    -DWITH_RADOSGW_DAOS=ON
```

## Build

```bash
    cd build
    ninja [vstart]
```

## Running Test cluster

Edit ceph.conf to add below option

```conf
    [client]
        rgw backend store = daos
```

Restart vstart cluster or just RGW server

```bash
    [..] RGW=1 ../src/vstart.sh -d
```

The above configuration brings up an RGW server on DAOS.

## Creating a test user

 To create a `testid` user to be used for s3 operations, use the following command:

 ```bash
local akey='0555b35654ad1656d804'
local skey='h7GhxuBLTrlhVUyxSPUKUV8r/2EI4ngqJxD7iBdBYLhwluN30JaT3Q=='
    radosgw-admin user create --uid testid \
        --access-key $akey --secret $skey \
        --display-name 'M. Tester' --email tester@ceph.com --no-mon-config
 ```

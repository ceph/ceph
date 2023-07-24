# Introduction

This directory contains an example `storage_class.lua` on how to
use [Lua Scripting](https://docs.ceph.com/en/latest/radosgw/lua-scripting/)
to read and write the Storage Class field of a put request.

## Usage - following examples based on vstart environment built in ceph/build and commands invoked from ceph/build

* Create Zonegroup placement info for a Storage Class (QLC_CLASS in this example) and point class to a data pool (qlc_pool in this example)
NOTE: RGW will need restarted due to the Zonegroup placement info change.
See: https://docs.ceph.com/en/latest/radosgw/placement/#zonegroup-zone-configuration for more information.

```bash
# Create Storage Class
./bin/radosgw-admin zonegroup placement add --rgw-zonegroup default --placement-id default-placement --storage-class QLC_CLASS
# Steer objects in QLC_CLASS to the qlc_pool data pool
./bin/radosgw-admin zone placement add --rgw-zone default --placement-id default-placement --storage-class QLC_CLASS --data-pool qlc_pool
```
* Restart radosgw for Zone/ZoneGroup placement changes to take effect.

* Upload the script:

```bash
./bin/radosgw-admin script put --infile=storage_class.lua --context=preRequest
```

* Create a bucket and put and object with a Storage Class header (no modification will occur):
```bash
aws --profile=ceph --endpoint=http://localhost:8000 s3api create-bucket --bucket test-bucket
aws --profile=ceph --endpoint=http://localhost:8000 s3api put-object --bucket test-bucket --key truv-0 --body ./64KiB_object.bin --storage-class STANDARD
```

* Send a request without a Storage Class header (Storage Class will be changed to QLC_CLASS by Lua script):
```bash
aws --profile=ceph --endpoint=http://localhost:8000 s3api put-object --bucket test-bucket --key truv-0 --body ./64KiB_object.bin
```
NOTE: If you use s3cmd instead of aws command-line, s3cmd adds "STANDARD" StorageClass to any put request so the example Lua script will not modify it.

* Verify S3 object had its StorageClass header added
```bash
grep Lua ceph/build/out/radosgw.8000.log

2021-11-01T17:10:14.048-0400 7f9c7f697640 20 Lua INFO: Put_Obj with StorageClass:
2021-11-01T17:10:14.048-0400 7f9c7f697640 20 Lua INFO: No StorageClass for Object and size >= threshold: truv-0 adding QLC StorageClass
```

## Requirements
* Lua 5.3


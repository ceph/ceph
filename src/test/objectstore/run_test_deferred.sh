#!/bin/bash


if [[ ! (-x ./bin/test_corrupt_deferred) || ! (-x ./bin/ceph-kvstore-tool) || ! (-x ./bin/ceph-bluestore-tool)]]
then
    echo Test must be run from ceph build directory
    echo with test_corrupt_deferred, ceph-kvstore-tool and ceph-bluestore-tool compiled
    exit 1
fi

# Create BlueStore, only main block device, 4K AU, forced deferred 4K, 64K AU for BlueFS

# Create file zapchajdziura, that is 0xe000 in size.
# This adds to 0x0000 - 0x1000 of BlueStore superblock and 0x1000 - 0x2000 of BlueFS superblock,
# making 0x00000 - 0x10000 filled, nicely aligning for 64K BlueFS requirements

# Prefill 10 objects Object-0 .. Object-9, each 64K. Sync to disk.
# Do transactions like:
# - fill Object-x+1 16 times at offsets 0x0000, 0x1000, ... 0xf000 with 8bytes, trigerring deferred writes
# - fill Object-x with 64K data
# Repeat for Object-0 to Object-8.

# Right after getting notification on_complete for all 9 transactions, immediately exit(1).
./bin/test_corrupt_deferred --log-to-stderr=false

# Now we should have a considerable amount of pending deferred writes.
# They do refer disk regions that do not belong to any object.

# Perform compaction on RocksDB
# This initializes BlueFS, but does not replay deferred writes.
# It jiggles RocksDB files around. CURRENT and MANIFEST are recreated, with some .sst files too.
# The hope here is that newly created RocksDB files will occupy space that is free,
# but targetted by pending deferred writes.
./bin/ceph-kvstore-tool bluestore-kv bluestore.test_temp_dir/ compact --log-to-stderr=false

# It this step we (hopefully) get RocksDB files overwritten
# We initialize BlueFS and RocksDB, there should be no problem here.
# Then we apply deferred writes. Now some of RocksDB files might get corrupted.
# It is very likely that this will not cause any problems, since CURRENT and MANIFEST are only read at bootup.
./bin/ceph-bluestore-tool --path bluestore.test_temp_dir/ --command fsck --deep 1 --debug-bluestore=30/30 --debug-bdev=30/30 --log-file=log-bs-corrupts.txt --log-to-file --log-to-stderr=false

# If we were lucky, this command now fails
./bin/ceph-bluestore-tool --path bluestore.test_temp_dir/ --command fsck --deep 1 --debug-bluestore=30/30 --debug-bdev=30/30 --log-file=log-bs-crash.txt --log-to-file --log-to-stderr=false
if [[ $? != 0 ]]
then
    echo "Deferred writes corruption successfully created !"
else
    echo "No deferred write problems detected."
fi

#cleanup
rm -rf bluestore.test_temp_dir/

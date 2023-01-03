#!/bin/bash -ex

NUM_OSDS=$(ceph osd dump -f json | jq '.osds | length')

# Check bluestore settings
for (( i=0; i<$NUM_OSDS; i++ ))
do
        SETTING=$(ceph config get osd.$i bluestore_zero_block_detection)
        if [ $SETTING != "true" ]; then
		ceph config set osd.$i bluestore_zero_block_detection true
        fi

	ceph config set osd.$i debug_bluestore 20
done

# Create a test pool "test_pool" (bluestore_zero_block_detection=false)
ceph osd pool create test_pool

# Check utilization after creating "test_pool" (bluestore_zero_block_detection=false)
sleep 15
SIZE_0=$(rados df -f json | jq '.pools[] | select(.name=="test_pool").size_kb')
if [ $SIZE_0 -ne 0 ]; then
	echo "Pool should be empty since it was just created."
	exit 1
fi

# Make a directory for test files
mkdir ~/bluestore_zero_block_detection_test_files

# Write a non-zero object to pool "test_pool" (bluestore_zero_block_detection=false)
head -c 10 /dev/random > ~/bluestore_zero_block_detection_test_files/random_data
rados -p test_pool put random_data ~/bluestore_zero_block_detection_test_files/random_data

# Check utilization after writing a random object (bluestore_zero_block_detection=false)
sleep 15
SIZE_1=$(rados df -f json | jq '.pools[] | select(.name=="test_pool").size_kb')
if [ $SIZE_1 -le $SIZE_0 ]; then
        echo "Pool should be filled since we wrote a non-zero object."
        exit 1
fi

# Write a zero object to pool "test_pool" (bluestore_zero_block_detection=false)
head -c 10 /dev/zero > ~/bluestore_zero_block_detection_test_files/zero_data
rados -p test_pool put zero_data ~/bluestore_zero_block_detection_test_files/zero_data

# Check utilization after writing a zero object (bluestore_zero_block_detection=false)
sleep 15
SIZE_2=$(rados df -f json | jq '.pools[] | select(.name=="test_pool").size_kb')
if [ $SIZE_2 -ne $SIZE_1 ]; then
        echo "Pool should not have changed since BZBD is enabled."
        exit 1
fi

# Test on a larger zeroed object (bluestore_zero_block_detection=true)
head -c 10000 /dev/zero > ~/bluestore_zero_block_detection_test_files/zero_data_big
rados -p test_pool put zero_data_2 ~/bluestore_zero_block_detection_test_files/zero_data_big

# Check utilization after writing a large zero object (bluestore_zero_block_detection=true)
sleep 15
SIZE_3=$(rados df -f json | jq '.pools[] | select(.name=="test_pool").size_kb')
if [ $SIZE_3 -ne $SIZE_2 ]; then
        echo "Pool should not have changed sinze BZBD is enabled."
        exit 1
fi

# Listing the objects from pool "test_pool", we can see a total of 3 objects that we created,
# two of which were skipped due to bluestore_zero_block_detection.
NUM_OBJECTS=$( rados -p test_pool ls -f json | jq '. | length')
if [ $NUM_OBJECTS -ne 3 ]; then
	echo "Incorrect amount of objects."
	exit 1
fi

# Remove test file directory
rm -rf ~/bluestore_zero_block_detection_test_files

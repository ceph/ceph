#!/bin/bash

OSD_MEMORY=${OSD_MEMORY:-4G}

FS=0 MON=3 MDS=0 MGR=1 OSD=3 ../src/vstart.sh -l -n -b --without-dashboard \
	-o rbd_cache=false \
	-o debug_bluestore=0/0 \
	-o debug_bluefs=0/0 \
    -o debug_rocksdb=4/4 \
	-o debug_osd=0/0 \
	-o debug_ms=0/0 \
	-o debug_mon=0 \
	--nolockdep	\
    -o 'bluestore block path = /ceph-devices/block.$id' \
    -o 'bluestore block db path = /ceph-devices/block.db.$id' \
    -o 'bluestore block db create = false' \
    -o 'bluestore block wal path = ' \
    -o 'bluestore block wal create = false' \
	-o osd_memory_target=${OSD_MEMORY} \
	-o bluestore_max_blob_size=${BLOB_SIZE} \
        ${EXTRA_DEPLOY_OPTIONS}
#!/bin/bash
set -e

ceph_test_seastore --memory 256M --smp 1
ceph_test_transaction_manager --memory 256M --smp 1
ceph_test_btree_lba_manager --memory 256M --smp 1
ceph_test_object_data_handler --memory 256M --smp 1

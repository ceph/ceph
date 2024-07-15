id=$1
ceph deamon osd${id} config set --bluestore_debug_omit_block_device_write true
ceph deamon osd${id} config set --bluestore_debug_omit_kv_commit true


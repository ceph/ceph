set -ex

ceph osd tier add cold-storage hot-storage
ceph osd tier cache-mode hot-storage writeback
ceph osd tier set-overlay cold-storage hot-storage

ceph osd pool set hot-storage hit_set_type bloom

ceph osd tier cache-mode hot-storage forward || true
ceph osd tier cache-mode hot-storage forward --yes-i-really-mean-it

ceph health | grep "HEALTH_OK"

# Removing tiering ###"
ceph osd tier cache-mode hot-storage none
ceph osd tier remove-overlay cold-storage
ceph osd tier remove cold-storage hot-storage

# Removing pools ###"
ceph osd pool rm hot-storage hot-storage --yes-i-really-really-mean-it
ceph osd pool rm cold-storage cold-storage --yes-i-really-really-mean-it

ceph health | grep "HEALTH_OK"


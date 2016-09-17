#!/bin/bash
if [ -f $(dirname $0)/../ceph-helpers-root.sh ]; then
    source $(dirname $0)/../ceph-helpers-root.sh
else
    echo "$(dirname $0)/../ceph-helpers-root.sh does not exist."
    exit 1
fi

install python-pytest || true
install pytest || true

# complete the cluster setup done by the teuthology ceph task
sudo chown $(id -u) /etc/ceph/ceph.conf
if ! test -f /etc/ceph/ceph.client.admin.keyring ; then
    sudo cp /etc/ceph/ceph.keyring /etc/ceph/ceph.client.admin.keyring
fi
if ! sudo test -f /var/lib/ceph/bootstrap-osd/ceph.keyring ; then
    sudo ceph-create-keys --id a
fi
sudo ceph osd crush rm osd.0 || true
sudo ceph osd crush rm osd.1 || true

sudo cp $(dirname $0)/60-ceph-by-partuuid.rules /lib/udev/rules.d
sudo udevadm control --reload

perl -pi -e 's|pid file.*|pid file = /var/run/ceph/\$cluster-\$name.pid|' /etc/ceph/ceph.conf

PATH=$(dirname $0):$(dirname $0)/..:$PATH

if ! which py.test > /dev/null; then
    echo "py.test not installed"
    exit 1
fi

sudo env PATH=$(dirname $0):$(dirname $0)/..:$PATH py.test -s -v $(dirname $0)/ceph-disk-test.py
result=$?

sudo rm -f /lib/udev/rules.d/60-ceph-by-partuuid.rules
# own whatever was created as a side effect of the py.test run
# so that it can successfully be removed later on by a non privileged 
# process
sudo chown -R $(id -u) $(dirname $0)
exit $result

source $(dirname $0)/../ceph-helpers-root.sh true

install python-pytest
install pytest
sudo env PATH=$(dirname $0)/..:$PATH py.test -v $(dirname $0)/ceph-disk-test.py

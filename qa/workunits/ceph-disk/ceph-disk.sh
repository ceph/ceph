source $(dirname $0)/../ceph-helpers-root.sh true

install python-pytest
install pytest
sudo env PATH=$(dirname $0)/..:$PATH py.test -v $(dirname $0)/ceph-disk-test.py
# own whatever was created as a side effect of the py.test run
# so that it can successfully be removed later on by a non privileged 
# process
sudo chown -R $(id -u) $(dirname $0)


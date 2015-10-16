#!/bin/bash
if [ -f $(dirname $0)/../ceph-helpers-root.sh ]; then
    source $(dirname $0)/../ceph-helpers-root.sh
else
    echo "$(dirname $0)/../ceph-helpers-root.sh does not exist."
    exit 1
fi

install python-pytest || true
install pytest || true

PATH=$(dirname $0)/..:$PATH

if ! which py.test > /dev/null; then
    echo "py.test not installed"
    exit 1
fi

sudo env PATH=$(dirname $0)/..:$PATH py.test -v $(dirname $0)/ceph-disk-test.py
result=$?

# own whatever was created as a side effect of the py.test run
# so that it can successfully be removed later on by a non privileged 
# process
sudo chown -R $(id -u) $(dirname $0)
exit $result

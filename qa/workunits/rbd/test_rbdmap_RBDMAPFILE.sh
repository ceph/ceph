#!/bin/sh
#
# Regression test for http://tracker.ceph.com/issues/14984
#
# When the bug is present, starting the rbdmap service causes
# a bogus log message to be emitted to the log because the RBDMAPFILE
# environment variable is not set.
#
# When the bug is not present, starting the rbdmap service will emit 
# no log messages, because /etc/ceph/rbdmap does not contain any lines 
# that require processing.
#
set -ex

which ceph-detect-init >/dev/null || exit 1
[ "$(ceph-detect-init)" = "systemd" ] || exit 0

echo "TEST: save timestamp for use later with journalctl --since"
TIMESTAMP=$(date +%Y-%m-%d\ %H:%M:%S)

echo "TEST: assert that rbdmap has not logged anything since boot"
journalctl -b 0 -t rbdmap | grep 'rbdmap\[[[:digit:]]' && exit 1
journalctl -b 0 -t init-rbdmap | grep 'rbdmap\[[[:digit:]]' && exit 1

echo "TEST: restart the rbdmap.service"
sudo systemctl restart rbdmap.service

echo "TEST: ensure that /usr/bin/rbdmap runs to completion"
until sudo systemctl status rbdmap.service | grep 'active (exited)' ; do 
    sleep 0.5
done

echo "TEST: assert that rbdmap has not logged anything since TIMESTAMP"
journalctl --since "$TIMESTAMP" -t rbdmap  | grep 'rbdmap\[[[:digit:]]' && exit 1
journalctl --since "$TIMESTAMP" -t init-rbdmap | grep 'rbdmap\[[[:digit:]]' && exit 1

exit 0

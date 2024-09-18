from unittest import mock

import pytest

from tests.fixtures import import_cephadm, cephadm_fs

from cephadmlib import logging


_cephadm = import_cephadm()

def test_cluster_logrotate_config(cephadm_fs):
    ctx = _cephadm.CephadmContext()
    ctx.logrotate_dir = '/my/log/dir'
    fsid = '5dcc9af0-7cd3-11ee-9e84-525400babd0a'

    cephadm_fs.create_dir(ctx.logrotate_dir)

    expected_cluster_logrotate_file = """# created by cephadm
/var/log/ceph/5dcc9af0-7cd3-11ee-9e84-525400babd0a/*.log {
    rotate 7
    daily
    compress
    sharedscripts
    postrotate
        killall -q -1 ceph-mon ceph-mgr ceph-mds ceph-osd ceph-fuse radosgw rbd-mirror cephfs-mirror tcmu-runner || pkill -1 -x 'ceph-mon|ceph-mgr|ceph-mds|ceph-osd|ceph-fuse|radosgw|rbd-mirror|cephfs-mirror|tcmu-runner' || true
    endscript
    missingok
    notifempty
    su root root
}"""

    logging.write_cluster_logrotate_config(ctx, fsid)

    with open(ctx.logrotate_dir + f'/ceph-{fsid}', 'r') as f:
        assert f.read() == expected_cluster_logrotate_file

def test_cephadm_logrotate_config(cephadm_fs):
    ctx = _cephadm.CephadmContext()
    ctx.logrotate_dir = '/my/log/dir'

    cephadm_fs.create_dir(ctx.logrotate_dir)

    expected_cephadm_logrotate_file = """# created by cephadm
/var/log/ceph/cephadm.log {
    rotate 7
    daily
    compress
    missingok
    notifempty
    su root root
}"""

    logging.write_cephadm_logrotate_config(ctx)

    with open(ctx.logrotate_dir + f'/cephadm', 'r') as f:
        assert f.read() == expected_cephadm_logrotate_file

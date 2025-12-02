import shutil

import pytest

from tests.fixtures import import_cephadm, cephadm_fs, mock_podman

from cephadmlib import logging


_cephadm = import_cephadm()

def test_cluster_logrotate_config(cephadm_fs):
    ctx = _cephadm.CephadmContext()
    ctx.logrotate_dir = '/my/log/dir'
    ctx.data_dir = '/my/data/dir'
    ctx.container_engine = mock_podman()
    fsid = '5dcc9af0-7cd3-11ee-9e84-525400babd0a'
    helper_script = f'{ctx.data_dir}/{fsid}/logrotate-reopen-logs.sh'
    systemd_run_path = shutil.which('systemd-run') or '/usr/bin/systemd-run'

    cephadm_fs.create_dir(ctx.logrotate_dir)
    cephadm_fs.create_dir(ctx.data_dir)

    expected_cluster_logrotate_file = f"""# created by cephadm
/var/log/ceph/{fsid}/*.log {{
    rotate 7
    daily
    compress
    sharedscripts
    postrotate
        {systemd_run_path} --wait --collect --quiet {helper_script} || true
        killall -q -1 ceph-mon ceph-mgr ceph-mds ceph-osd ceph-fuse radosgw rbd-mirror cephfs-mirror tcmu-runner || pkill -1 -x 'ceph-mon|ceph-mgr|ceph-mds|ceph-osd|ceph-fuse|radosgw|rbd-mirror|cephfs-mirror|tcmu-runner' || true
    endscript
    missingok
    notifempty
    su root root
}}"""

    expected_helper_script = f"""#!/bin/bash
# created by cephadm
set -euo pipefail

mapfile -t containers < <(/usr/bin/podman ps --filter "label=ceph=True" --format "{{{{.Names}}}}" | grep -E "^ceph-{fsid}" || true)

if [ "${{containers[*]:-}}" = "" ]; then
    exit 0
fi

/usr/bin/podman kill --signal SIGHUP "${{containers[@]}}" || true
exit 0
"""

    logging.write_cluster_logrotate_config(ctx, fsid)

    with open(ctx.logrotate_dir + f'/ceph-{fsid}', 'r') as f:
        assert f.read() == expected_cluster_logrotate_file
    with open(helper_script, 'r') as helper_fd:
        assert helper_fd.read() == expected_helper_script


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

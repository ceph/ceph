import re
import time

import pytest

import cephutil
import smbutil


def _cluster_id(smb_cfg):
    return smbutil.get_shares(smb_cfg)[0]['cluster_id']


def _ctdb_cmd(smb_cfg, cluster_id, args):
    result = cephutil.cephadm_enter_cmd(
        smb_cfg,
        cluster_id,
        list(args),
        capture_output=True,
        check=True,
    )
    return result.stdout.decode()


def _wait_for_ctdb_status(
    smb_cfg,
    cluster_id,
    matchers,
    *,
    attempts=18,
    sleep_s=10,
):
    """Poll ctdb status until all matchers succeed or attempts are exhausted."""
    last_status = ''
    last_nodes = ''
    for i in range(1, attempts + 1):
        last_status = _ctdb_cmd(smb_cfg, cluster_id, ['ctdb', 'status'])
        last_nodes = _ctdb_cmd(smb_cfg, cluster_id, ['ctdb', 'listnodes'])
        if all(m.search(last_status) for m in matchers):
            return last_status
        if i < attempts:
            time.sleep(sleep_s)
    raise AssertionError(
        'CTDB status did not converge in time.\n'
        f'Last status:\n{last_status}\n'
        f'Last listnodes:\n{last_nodes}'
    )


@pytest.mark.ctdb_healthy
def test_ctdb_cluster_healthy(smb_cfg):
    """After deploy with placement count:3, CTDB reports three OK nodes."""
    cluster_id = _cluster_id(smb_cfg)
    _wait_for_ctdb_status(
        smb_cfg,
        cluster_id,
        [
            re.compile(r'pnn:0 .*OK'),
            re.compile(r'pnn:1 .*OK'),
            re.compile(r'pnn:2 .*OK'),
            re.compile(r'Number of nodes:3'),
        ],
        # allow CTDB a bit of time after orch reports the service running
        attempts=12,
        sleep_s=10,
    )


@pytest.mark.ctdb_node_gone
def test_ctdb_deleted_node_after_shrink(smb_cfg):
    """After shrinking placement 3→2, CTDB reports one deleted node.

    orch wait_for_service only waits until running==size; CTDB can lag and
    briefly omit the deleted-node line, so retry for up to ~3 minutes.
    """
    cluster_id = _cluster_id(smb_cfg)
    _wait_for_ctdb_status(
        smb_cfg,
        cluster_id,
        [
            re.compile(r'pnn:0 .*OK'),
            re.compile(r'pnn:1 .*OK'),
            re.compile(r'Number of nodes:3 \(including 1 deleted nodes\)'),
        ],
        attempts=18,
        sleep_s=10,
    )

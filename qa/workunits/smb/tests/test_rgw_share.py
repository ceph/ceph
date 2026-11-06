import contextlib as _contextlib
import hashlib
import pathlib as _pathlib
import time

import pytest
import smbclient as _smbclient
import smbprotocol

import cephutil
import smbutil

NtStatus = smbprotocol.header.NtStatus

SEED_NAME = 'seed.txt'
SEED_TEXT = 'hello from s3\n'

# Marker used on every test in this module
pytestmark = pytest.mark.rgw_share


def _rgw_params(smb_cfg):
    params = smb_cfg.params.get('rgw_share') or {}
    if not params:
        pytest.skip('no rgw_share test params')
    return params


def _shares(smb_cfg, readonly):
    """Return a list of (share_id, share_params) tuples for shares matching
    the readonly setting.
    """
    params = _rgw_params(smb_cfg)
    return [
        (sid, sp)
        for sid, sp in params['shares'].items()
        if bool(sp.get('readonly', False)) == readonly
    ]


def _bucket_arg(share_params):
    """Return the bucket name in the form radosgw-admin expects, including the
    tenant if there is one.
    """
    bucket = share_params['bucket']
    tenant = share_params.get('tenant')
    return f'{tenant}/{bucket}' if tenant else bucket


def _bucket_entries(smb_cfg, share_params):
    """Return the radosgw-admin bucket list entries for a share's bucket."""
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        [
            'radosgw-admin',
            'bucket',
            'list',
            '--bucket',
            _bucket_arg(share_params),
        ],
        load_json=True,
    )
    return jres.obj


def _bucket_objects(smb_cfg, share_params):
    """Return the names of the objects in the rgw bucket backing a share."""
    return [o['name'] for o in _bucket_entries(smb_cfg, share_params)]


def _wait_for_object(smb_cfg, share_params, name, present, timeout=60):
    end = time.monotonic() + timeout
    while True:
        found = name in _bucket_objects(smb_cfg, share_params)
        if found == present:
            return
        if time.monotonic() > end:
            state = 'appear in' if present else 'be removed from'
            raise AssertionError(
                f'timed out waiting for {name} to {state} bucket'
            )
        time.sleep(2)


def _wait_for_object_content(smb_cfg, share_params, name, data, timeout=60):
    """Wait until the object in the rgw bucket has the given content.

    The content is compared using the size and etag recorded by radosgw for
    the object. The etag of a single-part object is the md5 of its content,
    which holds for the small objects written by these tests.
    """
    size = len(data)
    # md5 is only used to compute the expected etag, not for security
    etag = hashlib.md5(data, usedforsecurity=False).hexdigest()
    end = time.monotonic() + timeout
    while True:
        meta = {}
        for entry in _bucket_entries(smb_cfg, share_params):
            if entry['name'] == name:
                meta = entry.get('meta', {})
                break
        if meta.get('size') == size and meta.get('etag') == etag:
            return
        if time.monotonic() > end:
            raise AssertionError(
                f'timed out waiting for {name} in bucket to have size '
                f'{size} and etag {etag}; last seen size {meta.get("size")} '
                f'etag {meta.get("etag")}'
            )
        time.sleep(2)


def test_rgw_share_config(smb_cfg):
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    for sid, sp in params['shares'].items():
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        assert share['rgw']['bucket'] == sp['bucket']
        assert 'cephfs' not in share
        # a credential is automatically created and linked to the share
        assert share['rgw'].get('credential_ref')
        assert bool(share.get('readonly', False)) == bool(
            sp.get('readonly', False)
        )


def test_rgw_share_lists_bucket_objects(smb_cfg):
    for sid in _rgw_params(smb_cfg)['shares']:
        with smbutil.connection(smb_cfg, sid) as sharep:
            assert SEED_NAME in sharep.listdir()
            assert (sharep / SEED_NAME).read_text() == SEED_TEXT


def test_rgw_share_write_visible_in_bucket(smb_cfg):
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    for sid, sp in shares:
        fname = f'smb_written_{sid}.dat'
        with smbutil.connection(smb_cfg, sid) as sharep:
            fpath = sharep / fname
            fpath.write_text('HELLO RGW\n')
            try:
                assert fname in sharep.listdir()
                assert fpath.read_text() == 'HELLO RGW\n'
                # the object must have been written to the bucket itself
                _wait_for_object_content(
                    smb_cfg, sp, fname, 'HELLO RGW\n'.encode()
                )
            finally:
                fpath.unlink()
        _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_readonly_share_rejects_write(smb_cfg):
    shares = _shares(smb_cfg, readonly=True)
    assert shares
    for sid, sp in shares:
        fname = f'smb_denied_{sid}.dat'
        with smbutil.connection(smb_cfg, sid) as sharep:
            with smbutil.raises_nt_error(NtStatus.STATUS_ACCESS_DENIED):
                (sharep / fname).write_text('should not be written\n')
            # reads still work
            assert SEED_NAME in sharep.listdir()
        assert fname not in _bucket_objects(smb_cfg, sp)


def test_rgw_share_missing_bucket_rejected(smb_cfg):
    params = _rgw_params(smb_cfg)
    # any configured user is fine, the bucket is what must not be found
    user_id = next(iter(params['shares'].values()))['user_id']
    share = {
        'resource_type': 'ceph.smb.share',
        'cluster_id': params['cluster_id'],
        'share_id': 's3missing',
        'rgw': {'bucket': 'bucket-that-does-not-exist', 'user_id': user_id},
    }
    cmd_res = smbutil.apply_resources_unchecked(
        smb_cfg, [share], immediate=True
    )
    assert cmd_res.returncode != 0
    jres = cmd_res.obj
    assert not jres['success']
    assert len(jres['results']) == 1
    assert 'does not exist' in jres['results'][0]['msg']


def _get_rgw_user_info(smb_cfg, user_id):
    """Fetch radosgw-admin user info for the given user_id.

    Handles tenant-aware user IDs in 'tenant$user' format by splitting the
    tenant and passing it as a separate --tenant argument.
    """
    if '$' in user_id:
        tenant, uid = user_id.split('$', 1)
        cmd = [
            'radosgw-admin',
            'user',
            'info',
            '--uid',
            uid,
            '--tenant',
            tenant,
        ]
    else:
        cmd = ['radosgw-admin', 'user', 'info', '--uid', user_id]
    jres = cephutil.cephadm_shell_cmd(smb_cfg, cmd, load_json=True)
    return jres.obj


def _bucket_owner(smb_cfg, share_params):
    """Return the owner of the rgw bucket backing a share, in the
    'user' or 'tenant$user' form used by radosgw.
    """
    jres = cephutil.cephadm_shell_cmd(
        smb_cfg,
        [
            'radosgw-admin',
            'bucket',
            'stats',
            '--bucket',
            _bucket_arg(share_params),
        ],
        load_json=True,
    )
    return jres.obj['owner']


def test_rgw_bucket_exists(smb_cfg):
    """Validate that the bucket backing each share exists in radosgw and is
    owned by the user given for the share in the test params.
    """
    for sid, sp in _rgw_params(smb_cfg)['shares'].items():
        owner = _bucket_owner(smb_cfg, sp)
        assert owner == sp['user_id'], (
            f'share {sid}: bucket {_bucket_arg(sp)!r} owner {owner!r} '
            f'!= expected {sp["user_id"]!r}'
        )


def test_rgw_credential_valid(smb_cfg):
    """Validate the RGW credential linked to each share against radosgw.

    For every share the credential resource ('ceph smb show
    ceph.smb.rgw.credential.<id>') must:
      - belong to the user configured for the share,
      - have an access key and secret key set, and
      - match a key of that user in radosgw, which must be active, and the
        user must not be suspended.
    Any missing or mismatched field fails the test immediately. Key values are
    never included in assertion messages.
    """
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    for sid, sp in params['shares'].items():
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        cred_id = share['rgw'].get('credential_ref')
        assert cred_id, f'share {sid} has no credential_ref'
        cred = smbutil.get_rgw_credential(smb_cfg, cred_id)
        assert cred.get('user_id') == sp['user_id'], (
            f'share {sid}: credential {cred_id} user_id '
            f'{cred.get("user_id")!r} != expected {sp["user_id"]!r}'
        )
        access_key = cred.get('access_key_id')
        secret_key = cred.get('secret_access_key')
        assert access_key, f'credential {cred_id} has no access_key_id'
        assert secret_key, f'credential {cred_id} has no secret_access_key'
        user_info = _get_rgw_user_info(smb_cfg, sp['user_id'])
        assert user_info.get('suspended', 0) == 0, (
            f'radosgw user {sp["user_id"]!r} is suspended'
        )
        matching = [
            k
            for k in user_info.get('keys', [])
            if k.get('access_key') == access_key
            and k.get('secret_key') == secret_key
        ]
        assert matching, (
            f'credential {cred_id} does not match any key of radosgw user '
            f'{sp["user_id"]!r}'
        )
        assert matching[0].get('active', True), (
            f'credential {cred_id} key of radosgw user {sp["user_id"]!r} '
            'is not active'
        )


def test_rgw_tenant_share_config(smb_cfg):
    """Validate that a tenant-aware share is configured with the tenant
    qualified user ('tenant$user') and a linked credential. Reading the
    share contents is covered by test_rgw_share_lists_bucket_objects.
    """
    params = _rgw_params(smb_cfg)
    cluster_id = params['cluster_id']
    tenant_shares = [
        (sid, sp)
        for sid, sp in params['shares'].items()
        if sp.get('tenant')
    ]
    if not tenant_shares:
        pytest.skip('no tenant shares configured')
    for sid, sp in tenant_shares:
        share = smbutil.get_share_by_id(smb_cfg, cluster_id, sid)
        assert share['rgw']['bucket'] == sp['bucket']
        # applying the share replaces rgw.user_id with a credential_ref, so
        # the tenant qualified user is verified on the linked credential
        cred_id = share['rgw'].get('credential_ref')
        assert cred_id, f'tenant share {sid} has no credential_ref'
        cred = smbutil.get_rgw_credential(smb_cfg, cred_id)
        assert cred.get('user_id') == sp['user_id'], (
            f'tenant share {sid}: credential {cred_id} user_id '
            f'{cred.get("user_id")!r} != expected {sp["user_id"]!r}'
        )


def test_rgw_share_delete_removes_bucket_object(smb_cfg):
    """Verify that deleting a file over SMB removes the corresponding
    object from the RGW bucket.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_delete_test_{sid}.dat'
    try:
        with smbutil.connection(smb_cfg, sid) as sharep:
            fpath = sharep / fname
            fpath.write_text('to be deleted\n')
            _wait_for_object(smb_cfg, sp, fname, present=True)
            fpath.unlink()
        _wait_for_object(smb_cfg, sp, fname, present=False)
    finally:
        # make sure the file is gone even if a step above failed; it is
        # already gone (and the error ignored) when the test passed
        with smbutil.connection(smb_cfg, sid) as sharep:
            with _contextlib.suppress(OSError):
                (sharep / fname).unlink()


def test_rgw_share_overwrite_updates_bucket_object(smb_cfg):
    """Verify that overwriting an existing file via SMB replaces the object
    in the RGW bucket, and that the new contents read back via SMB.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_overwrite_{sid}.dat'
    first_content = 'first version\n'
    second_content = 'second version\n'
    with smbutil.connection(smb_cfg, sid) as sharep:
        fpath = sharep / fname
        fpath.write_text(first_content)
        try:
            _wait_for_object_content(
                smb_cfg, sp, fname, first_content.encode()
            )
            fpath.write_text(second_content)
            assert fpath.read_text() == second_content
            # the bucket object itself must now hold the new contents
            _wait_for_object_content(
                smb_cfg, sp, fname, second_content.encode()
            )
        finally:
            fpath.unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_large_file_roundtrip(smb_cfg):
    """Write a file larger than a typical SMB read-size, then read it back,
    to exercise multi-chunk I/O through the RGW VFS layer.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, sp = shares[0]
    fname = f'smb_large_{sid}.dat'
    # 4 MiB of repeated pattern
    data = b'A' * (4 * 1024 * 1024)
    with smbutil.connection(smb_cfg, sid) as sharep:
        fpath = sharep / fname
        fpath.write_bytes(data)
        try:
            _wait_for_object(smb_cfg, sp, fname, present=True)
            assert fpath.stat().st_size == len(data)
            with fpath.open(mode='rb') as fh:
                assert fh.read() == data
        finally:
            fpath.unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_subdir_create_and_list(smb_cfg):
    """Verify that creating a sub-directory and writing a file inside it
    works correctly (RGW VFS represents directories as key prefixes).
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares
    sid, _ = shares[0]
    dname = f'smb_subdir_{sid}'
    fname = 'inner.txt'
    with smbutil.connection(smb_cfg, sid) as sharep:
        subdir = sharep / dname
        subdir.mkdir(exist_ok=True)
        try:
            assert dname in sharep.listdir()
            inner = subdir / fname
            inner.write_text('inside subdir\n')
            try:
                assert fname in subdir.listdir()
                assert inner.read_text() == 'inside subdir\n'
            finally:
                inner.unlink()
        finally:
            subdir.rmdir()


@_contextlib.contextmanager
def _connection_to(server_ip, share, conf, username=None, password=None):
    """Like smbutil.connection() but targets an explicit server IP so
    cross-node tests can connect to any CTDB node, not just nodes[0].
    """
    port = 445
    username = conf.username if username is None else username
    password = conf.password if password is None else password
    _smbclient.register_session(
        server=server_ip,
        port=port,
        username=username,
        password=password,
    )
    try:
        spath = _pathlib.PureWindowsPath(f'//{server_ip}/{share}')
        yield smbutil.PathWrapper(spath)
    finally:
        _smbclient.delete_session(server_ip, port)


def test_rgw_share_credential_on_all_nodes(smb_cfg):
    """Verify that every CTDB node can serve the RGW share: each node must
    be able to reach the bucket (using the share's credential) and list the
    seed object.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, _ = shares[0]

    for server in smb_cfg.servers:
        with _connection_to(server.ip_address, sid, smb_cfg) as sharep:
            assert SEED_NAME in sharep.listdir(), (
                f'seed object missing on node {server.ip_address}'
            )


def test_rgw_share_cross_node_read_after_write(smb_cfg):
    """Write a file via the first CTDB node, then read it back via a
    different node.  Both nodes must resolve to the same RGW bucket object,
    confirming that the credential propagated and the S3 namespace is shared.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, sp = shares[0]

    servers = smb_cfg.servers
    writer_ip = servers[0].ip_address
    reader_ip = servers[-1].ip_address  # last node — different from writer

    fname = f'smb_xnode_{sid}.dat'
    content = 'cross-node RGW write\n'

    try:
        # write on writer node
        with _connection_to(writer_ip, sid, smb_cfg) as sharep:
            (sharep / fname).write_text(content)

        # object must appear in the bucket
        _wait_for_object(smb_cfg, sp, fname, present=True)

        # read back on reader node
        with _connection_to(reader_ip, sid, smb_cfg) as sharep:
            assert fname in sharep.listdir(), (
                f'{fname} not visible on reader node {reader_ip}'
            )
            assert (sharep / fname).read_text() == content, (
                f'content mismatch on reader node {reader_ip}'
            )
    finally:
        # clean up via writer node, even if a step above failed
        with _connection_to(writer_ip, sid, smb_cfg) as sharep:
            with _contextlib.suppress(OSError):
                (sharep / fname).unlink()
    _wait_for_object(smb_cfg, sp, fname, present=False)


def test_rgw_share_cross_node_delete(smb_cfg):
    """Write a file via one CTDB node and delete it via another.
    The bucket object must disappear after the cross-node delete.
    """
    shares = _shares(smb_cfg, readonly=False)
    assert shares, 'no writable share in params'
    sid, sp = shares[0]

    servers = smb_cfg.servers
    writer_ip = servers[0].ip_address
    deleter_ip = servers[-1].ip_address

    fname = f'smb_xnode_del_{sid}.dat'

    try:
        with _connection_to(writer_ip, sid, smb_cfg) as sharep:
            (sharep / fname).write_text('to be cross-deleted\n')

        _wait_for_object(smb_cfg, sp, fname, present=True)

        with _connection_to(deleter_ip, sid, smb_cfg) as sharep:
            (sharep / fname).unlink()

        _wait_for_object(smb_cfg, sp, fname, present=False)
    finally:
        # make sure the file is gone even if a step above failed; it is
        # already gone (and the error ignored) when the test passed
        with _connection_to(writer_ip, sid, smb_cfg) as sharep:
            with _contextlib.suppress(OSError):
                (sharep / fname).unlink()


def test_rgw_readonly_share_on_all_nodes(smb_cfg):
    """Every CTDB node must enforce read-only on the read-only share:
    writes must be denied and reads must succeed on every node.
    """
    shares = _shares(smb_cfg, readonly=True)
    if not shares:
        pytest.skip('no read-only RGW share configured')
    sid, sp = shares[0]

    for server in smb_cfg.servers:
        fname = f'smb_ro_node_{server.ip_address}_{sid}.dat'
        with _connection_to(server.ip_address, sid, smb_cfg) as sharep:
            with smbutil.raises_nt_error(NtStatus.STATUS_ACCESS_DENIED):
                (sharep / fname).write_text('should not land\n')
            # reads must still work on this node
            assert SEED_NAME in sharep.listdir(), (
                f'seed object not readable on node {server.ip_address}'
            )
        assert fname not in _bucket_objects(smb_cfg, sp), (
            f'file unexpectedly written to bucket via node {server.ip_address}'
        )

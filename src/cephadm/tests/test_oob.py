from unittest import mock

import pytest

from tests.fixtures import with_cephadm_ctx, import_cephadm

from cephadmlib import oob

_cephadm = import_cephadm()

FSID = '9b9d7609-f4d5-4aba-94c8-effa764d96c9'


@pytest.fixture
def host(tmp_path):
    """A fake host with two RDMA devices, mlx5_0 and mlx5_1."""
    sysfs = tmp_path / 'sys'
    devroot = tmp_path / 'dev'
    for n, name in enumerate(['mlx5_0', 'mlx5_1']):
        d = sysfs / 'class' / 'infiniband_verbs' / f'uverbs{n}'
        d.mkdir(parents=True)
        (d / 'ibdev').write_text(f'{name}\n')
    # the ABI version file is not a device
    (sysfs / 'class' / 'infiniband_verbs' / 'abi_version').write_text('6\n')
    (devroot / 'infiniband').mkdir(parents=True)
    (devroot / 'infiniband' / 'rdma_cm').write_text('')
    with mock.patch.object(oob, 'SYSFS', str(sysfs)), \
            mock.patch.object(oob, 'DEVROOT', str(devroot)):
        yield tmp_path


def test_rdma_uverbs_devices(host):
    assert oob.rdma_uverbs_devices() == {
        'mlx5_0': '/dev/infiniband/uverbs0',
        'mlx5_1': '/dev/infiniband/uverbs1',
    }


def test_rdma_uverbs_devices_none(tmp_path):
    assert oob.rdma_uverbs_devices(str(tmp_path)) == {}


@pytest.mark.parametrize(
    'config,privileged,args',
    [
        (
            {'rdma_devices': ['mlx5_1'], 'memlock': 3 << 30},
            False,
            [
                '--ulimit=memlock=3221225472:3221225472',
                '--device=/dev/infiniband/uverbs1',
                '--device=/dev/infiniband/rdma_cm',
            ],
        ),
        (
            {'rdma_devices': 'all', 'memlock': -1, 'raw_sockets': True},
            False,
            [
                '--ulimit=memlock=-1:-1',
                '--device=/dev/infiniband/uverbs0',
                '--device=/dev/infiniband/uverbs1',
                '--device=/dev/infiniband/rdma_cm',
                '--cap-add=NET_RAW',
            ],
        ),
        (
            {'memlock': 1 << 30, 'raw_sockets': True},
            False,
            ['--ulimit=memlock=1073741824:1073741824', '--cap-add=NET_RAW'],
        ),
        # a privileged container has every device and capability already
        (
            {'rdma_devices': 'all', 'memlock': 3 << 30, 'raw_sockets': True},
            True,
            ['--ulimit=memlock=3221225472:3221225472'],
        ),
    ],
)
def test_oob_container_args(host, config, privileged, args):
    assert oob.oob_container_args(config, privileged) == args


def test_oob_container_args_missing_device(host):
    with pytest.raises(_cephadm.Error, match='mlx5_7 not found'):
        oob.oob_container_args({'rdma_devices': ['mlx5_0', 'mlx5_7']}, False)


def test_oob_container_args_no_devices(tmp_path):
    with pytest.raises(_cephadm.Error, match='no RDMA devices'):
        oob.oob_container_args(
            {'rdma_devices': 'all'}, False, sysfs=str(tmp_path)
        )


def test_oob_container_args_no_rdma_cm(host):
    (host / 'dev' / 'infiniband' / 'rdma_cm').unlink()
    with pytest.raises(_cephadm.Error, match='rdma_ucm'):
        oob.oob_container_args({'rdma_devices': ['mlx5_0']}, False)


def test_oob_process_args():
    assert oob.oob_process_args({'raw_sockets': True}, 'osd') == [
        '--oob-keep-net-raw=true'
    ]
    # a gateway switches users after it reads the monitor configuration
    assert oob.oob_process_args({'raw_sockets': True}, 'rgw') == []
    assert oob.oob_process_args({'rdma_devices': 'all'}, 'osd') == []
    assert oob.oob_process_args({}, 'osd') == []


def _container(ctx, root, daemon_type, daemon_id):
    ctx.fsid = FSID
    ctx.log_dir = str(root / 'log')
    ctx.data_dir = str(root / 'data')
    (root / 'log').mkdir(exist_ok=True)
    return _cephadm.get_container(
        ctx, _cephadm.DaemonIdentity(FSID, daemon_type, daemon_id)
    )


@mock.patch('cephadmlib.daemons.ceph.make_run_dir')
@mock.patch(
    'cephadmlib.daemons.ceph.extract_uid_gid', return_value=(167, 167)
)
def test_rgw_container(_uid_gid, _make_run_dir, host):
    with with_cephadm_ctx(['--image=ceph/ceph'], list_networks={}) as ctx:
        ctx.config_blobs = {
            'oob': {
                'rdma_devices': ['mlx5_0'],
                'memlock': 3 << 30,
                'raw_sockets': True,
            },
        }
        c = _container(ctx, host, 'rgw', 'foo.test.abcdef')
        assert not c.privileged
        oob_args = [
            '--ulimit=memlock=3221225472:3221225472',
            '--device=/dev/infiniband/uverbs0',
            '--device=/dev/infiniband/rdma_cm',
            '--cap-add=NET_RAW',
        ]
        i = c.container_args.index(oob_args[0])
        assert c.container_args[i:i + len(oob_args)] == oob_args
        assert '--oob-keep-net-raw=true' not in c.args


@mock.patch('cephadmlib.daemons.ceph.make_run_dir')
@mock.patch(
    'cephadmlib.daemons.ceph.extract_uid_gid', return_value=(167, 167)
)
def test_osd_container(_uid_gid, _make_run_dir, host):
    with with_cephadm_ctx(['--image=ceph/ceph'], list_networks={}) as ctx:
        ctx.config_blobs = {
            'oob': {
                'rdma_devices': 'all',
                'memlock': 3 << 30,
                'raw_sockets': True,
            },
        }
        c = _container(ctx, host, 'osd', '3')
        assert c.privileged
        assert '--ulimit=memlock=3221225472:3221225472' in c.container_args
        assert not [a for a in c.container_args if a.startswith('--device=')]
        assert '--cap-add=NET_RAW' not in c.container_args
        assert c.args[-1] == '--oob-keep-net-raw=true'


@mock.patch('cephadmlib.daemons.ceph.make_run_dir')
@mock.patch(
    'cephadmlib.daemons.ceph.extract_uid_gid', return_value=(167, 167)
)
def test_container_without_oob(_uid_gid, _make_run_dir, host):
    with with_cephadm_ctx(['--image=ceph/ceph'], list_networks={}) as ctx:
        ctx.config_blobs = {}
        for daemon_type, daemon_id in [('rgw', 'foo.test.abcdef'), ('osd', '3')]:
            c = _container(ctx, host, daemon_type, daemon_id)
            assert not [a for a in c.container_args if 'memlock=' in a]
            assert '--oob-keep-net-raw=true' not in c.args

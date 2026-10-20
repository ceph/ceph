import pytest
import yaml

from ceph.cephadm.oob_types import OOBSpec, OOBError
from ceph.deployment.drive_group import DriveGroupSpec
from ceph.deployment.hostspec import SpecValidationError
from ceph.deployment.service_spec import ServiceSpec


@pytest.mark.parametrize(
    "obj,config",
    [
        # memlock follows oob_memory (2G here) plus 1G
        ({'rdma_devices': ['mlx5_0', 'mlx5_1']},
         {'rdma_devices': ['mlx5_0', 'mlx5_1'], 'memlock': 3 << 30,
          'raw_sockets': False}),
        ({'rdma_devices': 'all', 'memlock': '8G'},
         {'rdma_devices': 'all', 'memlock': 8 << 30, 'raw_sockets': False}),
        ({'raw_sockets': True, 'memlock': 1048576},
         {'memlock': 1048576, 'raw_sockets': True}),
        ({'rdma_devices': ['irdma0'], 'memlock': 'unlimited'},
         {'rdma_devices': ['irdma0'], 'memlock': -1, 'raw_sockets': False}),
    ])
def test_oob_spec(obj, config):
    assert OOBSpec.from_json(obj).container_config(2 << 30) == config


def test_oob_spec_memlock_follows_oob_memory():
    spec = OOBSpec.from_json({'rdma_devices': ['mlx5_0']})
    assert spec.memlock_bytes(None) == 3 << 30
    assert spec.memlock_bytes(512 << 20) == (512 << 20) + (1 << 30)


@pytest.mark.parametrize(
    "obj,match",
    [
        ('mlx5_0', 'must be a dict'),
        ({'devices': ['mlx5_0']}, 'Unknown oob field'),
        ({'rdma_devices': []}, 'non-empty list'),
        ({'rdma_devices': 'mlx5_0'}, 'non-empty list'),
        ({'rdma_devices': ['/dev/infiniband/uverbs0']}, 'Invalid RDMA device'),
        ({'rdma_devices': ['../x']}, 'Invalid RDMA device'),
        ({'rdma_devices': [7]}, 'Invalid RDMA device'),
        ({'memlock': 'lots'}, 'oob.memlock'),
        ({'memlock': 0}, 'more than zero'),
        ({'memlock': True}, 'oob.memlock'),
        ({'raw_sockets': 'yes'}, 'true or false'),
    ])
def test_oob_spec_invalid(obj, match):
    with pytest.raises(OOBError, match=match):
        OOBSpec.from_json(obj)


def test_rgw_spec_oob():
    spec = ServiceSpec.from_json(yaml.safe_load("""
service_type: rgw
service_id: s3
spec:
  oob:
    rdma_devices:
    - mlx5_0
    memlock: 4G
"""))
    spec.validate()
    assert spec.oob == {'rdma_devices': ['mlx5_0'], 'memlock': '4G'}
    assert spec.to_json()['spec']['oob'] == spec.oob

    spec.oob = {'rdma_devices': ['mlx5 0']}
    with pytest.raises(SpecValidationError, match='Invalid RDMA device'):
        spec.validate()


def test_osd_spec_oob():
    spec = DriveGroupSpec.from_json(yaml.safe_load("""
service_type: osd
service_id: rdma
placement:
  host_pattern: '*'
spec:
  data_devices:
    all: true
  oob:
    rdma_devices: all
    raw_sockets: true
"""))
    spec.validate()
    assert spec.oob == {'rdma_devices': 'all', 'raw_sockets': True}
    assert spec.to_json()['spec']['oob'] == spec.oob

    spec.oob = {'raw_sockets': 1}
    with pytest.raises(SpecValidationError, match='true or false'):
        spec.validate()

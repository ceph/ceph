import orchestrator
from .fixtures import wait
import pytest
from unittest.mock import patch, PropertyMock

from rook.module import RookOrchestrator
from rook.rook_cluster import RookCluster


# we use this intermediate class as .rook_cluster property
# is read only in the paretn class RookCluster
class FakeRookCluster(RookCluster):
    def __init__(self):
        pass


class TestRook(object):

    @pytest.mark.parametrize("pods, expected_daemon_types", [
        (
            [
                {
                    'name': 'ceph-rook-exporter',
                    'hostname': 'host1',
                    "labels": {'app': 'rook-ceph-exporter',
                               'ceph_daemon_id': 'exporter'},
                    'phase': 'Pending',
                    'container_image_name': 'quay.io/ceph/ceph:v18',
                    'container_image_id': 'docker-pullable://quay.io/ceph/ceph@sha256:f239715e1c7756e32a202a572e2763a4ce15248e09fc6e8990985f8a09ffa784',
                    'refreshed': 'pod1_ts',
                    'started': 'pod1_ts',
                    'created': 'pod1_1ts',
                },
                {
                    'name': 'rook-ceph-mgr-a-68c7b9b6d8-vjjhl',
                    'hostname': 'host1',
                    "labels": {'app': 'rook-ceph-mgr',
                               'ceph_daemon_type': 'mgr',
                               'ceph_daemon_id': 'a'},
                    'phase': 'Failed',
                    'container_image_name': 'quay.io/ceph/ceph:v18',
                    'container_image_id': '',
                    'refreshed': 'pod2_ts',
                    'started': 'pod2_ts',
                    'created': 'pod2_1ts',
                },
                {
                    'name': 'rook-ceph-mon-a-65fb8694b4-mmtl5',
                    'hostname': 'host1',
                    "labels": {'app': 'rook-ceph-mon',
                               'ceph_daemon_type': 'mon',
                               'ceph_daemon_id': 'b'},
                    'phase': 'Running',
                    'container_image_name': 'quay.io/ceph/ceph:v18',
                    'container_image_id': '',
                    'refreshed': 'pod3_ts',
                    'started': 'pod3_ts',
                    'created': 'pod3_1ts',
                },
                {
                    'name': 'rook-ceph-osd-0-58cbd7b65c-6cjnr',
                    'hostname': 'host1',
                    "labels": {'app': 'rook-ceph-osd',
                               'ceph-osd-id': '0',
                               'ceph_daemon_type': 'osd',
                               'ceph_daemon_id': '0'},
                    'phase': 'Succeeded',
                    'container_image_name': 'quay.io/ceph/ceph:v18',
                    'container_image_id': '',
                    'refreshed': 'pod4_ts',
                    'started': 'pod4_ts',
                    'created': 'pod4_1ts',
                },
                # unknown pod: has no labels are provided, it shouldn't
                #  be part of the output
                {
                    'name': 'unknown-pod',
                    'hostname': '',
                    "labels": {'app': 'unkwon'},
                    'phase': 'Pending',
                    'container_image_name': 'quay.io/ceph/ceph:v18',
                    'container_image_id': '',
                    'refreshed': '',
                    'started': '',
                    'created': '',
                }
            ],
            ['ceph-exporter', 'mgr', 'mon', 'osd']
        )
    ])
    def test_list_daemons(self, pods, expected_daemon_types):

        status = {
            'Pending': orchestrator.DaemonDescriptionStatus.starting,
            'Running': orchestrator.DaemonDescriptionStatus.running,
            'Succeeded': orchestrator.DaemonDescriptionStatus.stopped,
            'Failed': orchestrator.DaemonDescriptionStatus.error,
            'Unknown': orchestrator.DaemonDescriptionStatus.unknown,
        }

        fake_rook_cluster = FakeRookCluster()
        ro = RookOrchestrator('rook', None, self)
        with patch('rook.RookOrchestrator.rook_cluster',
                   new_callable=PropertyMock,
                   return_value=fake_rook_cluster):
            with patch.object(fake_rook_cluster, 'describe_pods') as mock_describe_pods:
                mock_describe_pods.return_value = pods
                dds = wait(ro, ro.list_daemons())
                assert len(dds) == len(expected_daemon_types)
                for i in range(0, len(dds)):
                    assert dds[i].daemon_type == expected_daemon_types[i]
                    assert dds[i].hostname == pods[i]['hostname']
                    assert dds[i].status == status[pods[i]['phase']]
                    assert dds[i].container_image_name == pods[i]['container_image_name']
                    assert dds[i].container_image_id == pods[i]['container_image_id']
                    assert dds[i].created == pods[i]['created']
                    assert dds[i].last_configured == pods[i]['created']
                    assert dds[i].last_deployed == pods[i]['created']
                    assert dds[i].started == pods[i]['started']
                    assert dds[i].last_refresh == pods[i]['refreshed']

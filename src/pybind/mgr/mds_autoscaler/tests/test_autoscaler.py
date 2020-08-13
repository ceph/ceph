import pytest
from unittest import mock

from ceph.deployment.service_spec import ServiceSpec, PlacementSpec
from orchestrator import DaemonDescription, Completion, ServiceDescription

try:
    from typing import Any, List
except ImportError:
    pass

from mds_autoscaler.module import MDSAutoscaler



@pytest.yield_fixture()
def mds_autoscaler_module():

    with mock.patch("mds_autoscaler.module.MDSAutoscaler._orchestrator_wait"):
        m = MDSAutoscaler('cephadm', 0, 0)

        yield m


class TestCephadm(object):

    @mock.patch("mds_autoscaler.module.MDSAutoscaler.get")
    @mock.patch("mds_autoscaler.module.MDSAutoscaler.list_daemons")
    @mock.patch("mds_autoscaler.module.MDSAutoscaler.describe_service")
    @mock.patch("mds_autoscaler.module.MDSAutoscaler.apply_mds")
    def test_scale_up(self, _apply_mds, _describe_service, _list_daemons, _get, mds_autoscaler_module: MDSAutoscaler):
        daemons = Completion(value=[
            DaemonDescription(
                hostname='myhost',
                daemon_type='mds',
                daemon_id='fs_name.myhost.a'
            ),
            DaemonDescription(
                hostname='myhost',
                daemon_type='mds',
                daemon_id='fs_name.myhost.b'
            ),
        ])
        daemons.finalize()
        _list_daemons.return_value = daemons

        services = Completion(value=[
            ServiceDescription(
                spec=ServiceSpec(
                    service_type='mds',
                    service_id='fs_name',
                    placement=PlacementSpec(
                        count=2
                    )
                )
            )
        ])
        services.finalize()
        _describe_service.return_value = services

        apply = Completion(value='')
        apply.finalize()
        _apply_mds.return_value = apply


        _get.return_value = {
            'filesystems': [
                {
                    'mdsmap': {
                        'fs_name': 'fs_name',
                        'in': [
                            {
                                'name': 'mds.fs_name.myhost.a',
                            }
                        ],
                        'standby_count_wanted': 2,
                    }
                }
            ],
            'standbys': [
                {
                    'name': 'mds.fs_name.myhost.b',
                }
            ],

        }
        mds_autoscaler_module.notify('fs_map', None)

        _apply_mds.assert_called_with(ServiceSpec(
            service_type='mds',
            service_id='fs_name',
            placement=PlacementSpec(
                count=3
            )
        ))

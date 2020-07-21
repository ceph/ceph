import json
from unittest.mock import Mock, call, ANY

from ceph.deployment.drive_group import DriveGroupSpec, DeviceSelection
from ceph.deployment.hostspec import HostSpec
from rook.rook_client.ceph.cephcluster import CephCluster, Spec
from rook.rook_cluster import RookCluster


def test_rook_add_dg():
    cl = CephCluster(
        apiVersion='',
        spec=Spec(),
        metadata={},
    ).to_json()
    core = Mock()
    core.api_client.call_api.return_value = cl
    rc = RookCluster(core, Mock(), Mock())
    s = rc.add_osds(
        DriveGroupSpec(
            service_type='osd',
            service_id='all-osds',
            data_devices=DeviceSelection(all=True)
        ),
        [HostSpec(hostname='host')]
    )
    assert s == 'Success'
    print(repr(core.api_client.call_api.mock_calls[1]))
    core.api_client.call_api.assert_called_with(ANY, 'PATCH', _preload_content=True, _return_http_data_only=True, auth_settings=['BearerToken'], body=[{'op': 'add', 'path': '/spec/driveGroups', 'value': [{'name': 'all-osds', 'spec': {'data_devices': {'all': True}, 'filter_logic': 'AND', 'objectstore': 'bluestore'}, 'placement': {'node_affinity': None, 'pod_affinity': None, 'pod_anti_affinity': None}}]}], header_params={'Content-Type': 'application/json-patch+json'}, response_type='object')

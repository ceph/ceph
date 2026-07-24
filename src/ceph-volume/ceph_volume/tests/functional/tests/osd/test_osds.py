import pytest
import testinfra
from typing import Any, Dict, List


class TestOSDs:
    def test_osd_services_are_running(self, node: Dict[str, Any], host: testinfra.host) -> None:
        cluster_fsid: str = node['cluster_fsid']
        for osd_id in node["osd_ids"]:
            assert host.service(f'ceph-{cluster_fsid}@osd.{osd_id}.service').is_running

    def test_osds_listen_on_public_network(self, node: Dict[str, Any], host: testinfra.host) -> None:
        nb_port: int = (node['num_osds'] * node['num_osd_ports'])
        assert host.check_output(
            f'netstat -lntp | grep ceph-osd | grep {node["address"]} | wc -l') == str(nb_port)  # noqa: E501

    def test_osds_listen_on_cluster_network(self, node: Dict[str, Any], host: testinfra.host) -> None:
        nb_port: int = (node['num_osds'] * node['num_osd_ports'])
        assert host.check_output(
            f'netstat -lntp | grep ceph-osd | grep {node["cluster_address"]} | wc -l') == str(nb_port)  # noqa: E501

    @pytest.mark.unencrypted
    def test_osds_are_unencrypted(self, node: Dict[str, Any], host: testinfra.host) -> None:
        devices: List[str] = node['all_devices']
        for device in devices:
            # if `blkid -s TYPE <device> -o value` returns "ceph_bluestore" it means
            # no dmcrypt mapper was created. Therefore, no encryption.
            assert host.check_output(f'blkid -s TYPE {device} -o value') == 'ceph_bluestore'

    @pytest.mark.dmcrypt
    def test_osds_are_encrypted(self, node: Dict[str, Any], host: testinfra.host) -> None:
        devices: List[str] = node['all_devices']
        for device in devices:
            assert host.check_output(f'blkid -s TYPE {device} -o value') in ['crypto_LUKS', 'LVM2_member']
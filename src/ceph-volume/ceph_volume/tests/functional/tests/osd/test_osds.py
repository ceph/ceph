import json


class TestOSDs(object):

    def test_ceph_osd_package_is_installed(self, node, host):
        assert host.package("ceph-osd").is_installed

    def test_osds_listen_on_public_network(self, node, host):
        # TODO: figure out way to paramaterize this test
        nb_port = (node["num_osds"] * node["num_osd_ports"])
        assert host.check_output(
            "netstat -lntp | grep ceph-osd | grep %s | wc -l" % (node["address"])) == str(nb_port)  # noqa E501

    def test_osds_listen_on_cluster_network(self, node, host):
        # TODO: figure out way to paramaterize this test
        nb_port = (node["num_osds"] * node["num_osd_ports"])
        assert host.check_output("netstat -lntp | grep ceph-osd | grep %s | wc -l" %  # noqa E501
                                 (node["cluster_address"])) == str(nb_port)

    def test_osd_services_are_running(self, node, host):
        # TODO: figure out way to paramaterize node['osds'] for this test
        for osd in node["osds"]:
            assert host.service("ceph-osd@%s" % osd).is_running

    def test_osd_are_mounted(self, node, host):
        # TODO: figure out way to paramaterize node['osd_ids'] for this test
        for osd_id in node["osd_ids"]:
            osd_path = "/var/lib/ceph/osd/{cluster}-{osd_id}".format(
                cluster=node["cluster_name"],
                osd_id=osd_id,
            )
            assert host.mount_point(osd_path).exists

    def test_ceph_volume_is_installed(self, node, host):
        host.exists('ceph-volume')

    def test_ceph_volume_systemd_is_installed(self, node, host):
        host.exists('ceph-volume-systemd')

    def _get_osd_id_from_host(self, node, osd_tree):
        children = []
        for n in osd_tree['nodes']:
            if n['name'] == node['vars']['inventory_hostname'] and n['type'] == 'host':  # noqa E501
                children = n['children']
        return children

    def _get_nb_up_osds_from_ids(self, node, osd_tree):
        nb_up = 0
        ids = self._get_osd_id_from_host(node, osd_tree)
        for n in osd_tree['nodes']:
            if n['id'] in ids and n['status'] == 'up':
                nb_up += 1
        return nb_up

    def test_all_osds_are_up_and_in(self, node, host):
        cmd = "sudo ceph --cluster={cluster} --connect-timeout 5 --keyring /var/lib/ceph/bootstrap-osd/{cluster}.keyring -n client.bootstrap-osd osd tree -f json".format(  # noqa E501
            cluster=node["cluster_name"])
        output = json.loads(host.check_output(cmd))
        assert node["num_osds"] == self._get_nb_up_osds_from_ids(node, output)

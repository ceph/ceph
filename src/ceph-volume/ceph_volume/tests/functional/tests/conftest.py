import pytest
import os


@pytest.fixture()
def node(host, request):
    """ This fixture represents a single node in the ceph cluster. Using the
    host.ansible fixture provided by testinfra it can access all the ansible
    variables provided to it by the specific test scenario being ran.

    You must include this fixture on any tests that operate on specific type
    of node because it contains the logic to manage which tests a node
    should run.
    """
    ansible_vars = host.ansible.get_variables()
    # tox/jenkins/user will pass in this environment variable. we need to do it this way
    # because testinfra does not collect and provide ansible config passed in
    # from using --extra-vars
    ceph_dev_branch = os.environ.get("CEPH_DEV_BRANCH", "master")
    group_names = ansible_vars["group_names"]
    num_osd_ports = 4
    if ceph_dev_branch in ['luminous', 'mimic']:
        num_osd_ports = 2

    # capture the initial/default state
    test_is_applicable = False
    for marker in request.node.iter_markers():
        if marker.name in group_names or marker.name == 'all':
            test_is_applicable = True
            break
    # Check if any markers on the test method exist in the nodes group_names.
    # If they do not, this test is not valid for the node being tested.
    if not test_is_applicable:
        reason = "%s: Not a valid test for node type: %s" % (
            request.function, group_names)
        pytest.skip(reason)

    osd_ids = []
    osds = []
    cluster_address = ""
    # I can assume eth1 because I know all the vagrant
    # boxes we test with use that interface
    address = host.interface("eth1").addresses[0]
    subnet = ".".join(ansible_vars["public_network"].split(".")[0:-1])
    num_mons = len(ansible_vars["groups"]["mons"])
    num_osds = len(ansible_vars.get("devices", []))
    if not num_osds:
        num_osds = len(ansible_vars.get("lvm_volumes", []))
    osds_per_device = ansible_vars.get("osds_per_device", 1)
    num_osds = num_osds * osds_per_device

    # If number of devices doesn't map to number of OSDs, allow tests to define
    # that custom number, defaulting it to ``num_devices``
    num_osds = ansible_vars.get('num_osds', num_osds)
    cluster_name = ansible_vars.get("cluster", "ceph")
    conf_path = "/etc/ceph/{}.conf".format(cluster_name)
    if "osds" in group_names:
        # I can assume eth2 because I know all the vagrant
        # boxes we test with use that interface. OSDs are the only
        # nodes that have this interface.
        cluster_address = host.interface("eth2").addresses[0]
        cmd = host.run('sudo ls /var/lib/ceph/osd/ | sed "s/.*-//"')
        if cmd.rc == 0:
            osd_ids = cmd.stdout.rstrip("\n").split("\n")
            osds = osd_ids

    data = dict(
        address=address,
        subnet=subnet,
        vars=ansible_vars,
        osd_ids=osd_ids,
        num_mons=num_mons,
        num_osds=num_osds,
        num_osd_ports=num_osd_ports,
        cluster_name=cluster_name,
        conf_path=conf_path,
        cluster_address=cluster_address,
        osds=osds,
    )
    return data


def pytest_collection_modifyitems(session, config, items):
    for item in items:
        test_path = item.location[0]
        if "mon" in test_path:
            item.add_marker(pytest.mark.mons)
        elif "osd" in test_path:
            item.add_marker(pytest.mark.osds)
        elif "mds" in test_path:
            item.add_marker(pytest.mark.mdss)
        elif "mgr" in test_path:
            item.add_marker(pytest.mark.mgrs)
        elif "rbd-mirror" in test_path:
            item.add_marker(pytest.mark.rbdmirrors)
        elif "rgw" in test_path:
            item.add_marker(pytest.mark.rgws)
        elif "nfs" in test_path:
            item.add_marker(pytest.mark.nfss)
        elif "iscsi" in test_path:
            item.add_marker(pytest.mark.iscsigws)
        else:
            item.add_marker(pytest.mark.all)

# flake8: noqa
import json
import yaml

import pytest

from ceph.deployment.service_spec import HostPlacementSpec, PlacementSpec, RGWSpec, NFSServiceSpec, \
    servicespec_validate_add, ServiceSpec, ServiceSpecValidationError
from ceph.deployment.drive_group import DriveGroupSpec


@pytest.mark.parametrize("test_input,expected, require_network",
                         [("myhost", ('myhost', '', ''), False),
                          ("myhost=sname", ('myhost', '', 'sname'), False),
                          ("myhost:10.1.1.10", ('myhost', '10.1.1.10', ''), True),
                          ("myhost:10.1.1.10=sname", ('myhost', '10.1.1.10', 'sname'), True),
                          ("myhost:10.1.1.0/32", ('myhost', '10.1.1.0/32', ''), True),
                          ("myhost:10.1.1.0/32=sname", ('myhost', '10.1.1.0/32', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789]", ('myhost', '[v1:10.1.1.10:6789]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789]=sname", ('myhost', '[v1:10.1.1.10:6789]', 'sname'), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', ''), True),
                          ("myhost:[v1:10.1.1.10:6789,v2:10.1.1.11:3000]=sname", ('myhost', '[v1:10.1.1.10:6789,v2:10.1.1.11:3000]', 'sname'), True),
                          ])
def test_parse_host_placement_specs(test_input, expected, require_network):
    ret = HostPlacementSpec.parse(test_input, require_network=require_network)
    assert ret == expected
    assert str(ret) == test_input


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ('', "PlacementSpec()"),
        ("count:2", "PlacementSpec(count=2)"),
        ("3", "PlacementSpec(count=3)"),
        ("host1 host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1;host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1,host2", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ("host1 host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1=a host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name='a'), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1:1.2.3.4=a host2:1.2.3.5=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='1.2.3.4', name='a'), HostPlacementSpec(hostname='host2', network='1.2.3.5', name='b')])"),
        ("myhost:[v1:10.1.1.10:6789]", "PlacementSpec(hosts=[HostPlacementSpec(hostname='myhost', network='[v1:10.1.1.10:6789]', name='')])"),
        ('2 host1 host2', "PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ('label:foo', "PlacementSpec(label='foo')"),
        ('3 label:foo', "PlacementSpec(count=3, label='foo')"),
        ('*', "PlacementSpec(host_pattern='*')"),
        ('3 data[1-3]', "PlacementSpec(count=3, host_pattern='data[1-3]')"),
        ('3 data?', "PlacementSpec(count=3, host_pattern='data?')"),
        ('3 data*', "PlacementSpec(count=3, host_pattern='data*')"),
    ])
def test_parse_placement_specs(test_input, expected):
    ret = PlacementSpec.from_string(test_input)
    assert str(ret) == expected

@pytest.mark.parametrize(
    "test_input",
    [
        ("host=a host*"),
        ("host=a label:wrong"),
        ("host? host*"),
    ]
)
def test_parse_placement_specs_raises(test_input):
    with pytest.raises(ServiceSpecValidationError):
        PlacementSpec.from_string(test_input)

@pytest.mark.parametrize("test_input",
                         # wrong subnet
                         [("myhost:1.1.1.1/24"),
                          # wrong ip format
                          ("myhost:1"),
                          ])
def test_parse_host_placement_specs_raises_wrong_format(test_input):
    with pytest.raises(ValueError):
        HostPlacementSpec.parse(test_input)


@pytest.mark.parametrize(
    "s_type,o_spec,s_id",
    [
        ("mgr", ServiceSpec, 'test'),
        ("mon", ServiceSpec, 'test'),
        ("mds", ServiceSpec, 'test'),
        ("rgw", RGWSpec, 'realm.zone'),
        ("nfs", NFSServiceSpec, 'test'),
        ("osd", DriveGroupSpec, 'test'),
    ])
def test_servicespec_map_test(s_type, o_spec, s_id):
    dict_spec = {
        "service_id": s_id,
        "service_type": s_type,
        "placement":
            dict(hosts=["host1:1.1.1.1"])
    }
    if s_type == 'nfs':
        dict_spec['pool'] = 'pool'
    spec = ServiceSpec.from_json(dict_spec)
    assert isinstance(spec, o_spec)
    assert isinstance(spec.placement, PlacementSpec)
    assert isinstance(spec.placement.hosts[0], HostPlacementSpec)
    assert spec.placement.hosts[0].hostname == 'host1'
    assert spec.placement.hosts[0].network == '1.1.1.1'
    assert spec.placement.hosts[0].name == ''
    assert servicespec_validate_add(spec) is None
    ServiceSpec.from_json(spec.to_json())


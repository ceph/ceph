# flake8: noqa
import json

import pytest

from ceph.deployment.service_spec import HostPlacementSpec, PlacementSpec, RGWSpec, \
    servicespec_validate_add


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
        ("host1=a host2=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='', name='a'), HostPlacementSpec(hostname='host2', network='', name='b')])"),
        ("host1:1.2.3.4=a host2:1.2.3.5=b", "PlacementSpec(hosts=[HostPlacementSpec(hostname='host1', network='1.2.3.4', name='a'), HostPlacementSpec(hostname='host2', network='1.2.3.5', name='b')])"),
        ('2 host1 host2', "PlacementSpec(count=2, hosts=[HostPlacementSpec(hostname='host1', network='', name=''), HostPlacementSpec(hostname='host2', network='', name='')])"),
        ('label:foo', "PlacementSpec(label='foo')"),
        ('3 label:foo', "PlacementSpec(count=3, label='foo')"),
        ('*', 'PlacementSpec(all_hosts=True)'),
    ])
def test_parse_placement_specs(test_input, expected):
    ret = PlacementSpec.from_string(test_input)
    assert str(ret) == expected

@pytest.mark.parametrize("test_input",
                         # wrong subnet
                         [("myhost:1.1.1.1/24"),
                          # wrong ip format
                          ("myhost:1"),
                          # empty string
                          ("myhost=1"),
                          ])
def test_parse_host_placement_specs_raises(test_input):
    with pytest.raises(ValueError):
        ret = HostPlacementSpec.parse(test_input)


def test_rgwspec():
    """
    {
        "rgw_zone": "zonename",
        "service_type": "rgw",
        "rgw_frontend_port": 8080,
        "rgw_realm": "realm"
    }
    """
    example = json.loads(test_rgwspec.__doc__.strip())
    spec = RGWSpec.from_json(example)
    assert servicespec_validate_add(spec) is None
# flake8: noqa
import json
import yaml

import pytest

from ceph.deployment.hostspec import HostSpec, SpecValidationError


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ({"hostname": "foo"}, HostSpec('foo')),
        ({"hostname": "foo", "labels": "l1"}, HostSpec('foo', labels=['l1'])),
        ({"hostname": "foo", "labels": ["l1", "l2"]}, HostSpec('foo', labels=['l1', 'l2'])),
        ({"hostname": "foo", "location": {"rack": "foo"}}, HostSpec('foo', location={'rack': 'foo'})),
    ]
)
def test_parse_host_specs(test_input, expected):
    hs = HostSpec.from_json(test_input)
    assert hs == expected


@pytest.mark.parametrize(
    "bad_input",
    [
        ({"hostname": "foo", "labels": 124}),
        ({"hostname": "foo", "labels": {"a", "b"}}),
        ({"hostname": "foo", "labels": {"a", "b"}}),
        ({"hostname": "foo", "labels": ["a", 2]}),
        ({"hostname": "foo", "location": "rack=bar"}),
        ({"hostname": "foo", "location": ["a"]}),
        ({"hostname": "foo", "location": {"rack", 1}}),
        ({"hostname": "foo", "location": {1: "rack"}}),
    ]
)
def test_parse_host_specs(bad_input):
    with pytest.raises(SpecValidationError):
        hs = HostSpec.from_json(bad_input)
    

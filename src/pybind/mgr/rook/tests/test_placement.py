# flake8: noqa

from rook.rook_cluster import placement_spec_to_node_selector, node_selector_to_placement_spec
from rook.rook_client.ceph.cephcluster import MatchExpressionsItem, MatchExpressionsList, NodeSelectorTermsItem
import pytest
from orchestrator import HostSpec
from ceph.deployment.service_spec import PlacementSpec

@pytest.mark.parametrize("hosts",
    [   # noqa: E128
        [
            HostSpec(
                hostname="node1",
                labels=["label1"]
            ),
            HostSpec(
                hostname="node2",
                labels=[]
            ),
            HostSpec(
                hostname="node3",
                labels=["label1"]
            )
        ]
    ])
@pytest.mark.parametrize("expected_placement_spec, expected_node_selector",
    [   # noqa: E128
        (
            PlacementSpec(
                label="label1"
            ), 
            NodeSelectorTermsItem(
                matchExpressions=MatchExpressionsList(
                    [
                        MatchExpressionsItem(
                            key="ceph-label/label1",
                            operator="Exists"
                        )
                    ]
                )
            )
        ),
        (
            PlacementSpec(
                label="label1",
                host_pattern="*"
            ), 
            NodeSelectorTermsItem(
                matchExpressions=MatchExpressionsList(
                    [
                        MatchExpressionsItem(
                            key="ceph-label/label1",
                            operator="Exists"
                        ),
                        MatchExpressionsItem(
                            key="kubernetes.io/hostname",
                            operator="Exists",
                        )
                    ]
                )
            )
        ),
        (
            PlacementSpec(
                host_pattern="*"
            ), 
            NodeSelectorTermsItem(
                matchExpressions=MatchExpressionsList(
                    [
                        MatchExpressionsItem(
                            key="kubernetes.io/hostname",
                            operator="Exists",
                        )
                    ]
                )
            )
        ),
        (
            PlacementSpec(
                hosts=["node1", "node2", "node3"]
            ), 
            NodeSelectorTermsItem(
                matchExpressions=MatchExpressionsList(
                    [
                        MatchExpressionsItem(
                            key="kubernetes.io/hostname",
                            operator="In",
                            values=["node1", "node2", "node3"]
                        )
                    ]
                )
            )
        ),
    ])
def test_placement_spec_translate(hosts, expected_placement_spec, expected_node_selector):
    node_selector = placement_spec_to_node_selector(expected_placement_spec, hosts)
    assert [(getattr(expression, 'key', None), getattr(expression, 'operator', None), getattr(expression, 'values', None)) for expression in node_selector.matchExpressions] == [(getattr(expression, 'key', None), getattr(expression, 'operator', None), getattr(expression, 'values', None)) for expression in expected_node_selector.matchExpressions]
    placement_spec = node_selector_to_placement_spec(expected_node_selector)
    assert placement_spec == expected_placement_spec
    assert (getattr(placement_spec, 'label', None), getattr(placement_spec, 'hosts', None), getattr(placement_spec, 'host_pattern', None)) == (getattr(expected_placement_spec, 'label', None), getattr(expected_placement_spec, 'hosts', None), getattr(expected_placement_spec, 'host_pattern', None))

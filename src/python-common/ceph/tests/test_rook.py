import yaml

from ceph.deployment.rook import placement_spec_to_k8s
from ceph.deployment.service_spec import PlacementSpec


def test_placement_spec_to_k8s():
    spec = PlacementSpec(label='app:ceph-rook-rgw')
    count, aff = placement_spec_to_k8s([], spec)
    yml = yaml.dump(aff.to_dict())
    assert yml == """node_affinity:
  preferred_during_scheduling_ignored_during_execution: null
  required_during_scheduling_ignored_during_execution:
    node_selector_terms:
    - match_expressions:
      - key: app
        operator: In
        values:
        - ceph-rook-rgw
      match_fields: null
pod_affinity: null
pod_anti_affinity: null
"""

from ceph.deployment.rook import k8s_node_to_hostspec
from ceph.deployment.service_spec import PlacementSpec


def test_placement_match():
    """
    NOTE

    Used by rook. Be careful when changing the API
    """
    hostspec = k8s_node_to_hostspec({
      "kind": "Node",
      "apiVersion": "v1",
      "metadata": {
        "name": "name",
        "labels": {
          "name": "my-first-k8s-node"
        }
      }
    })

    assert PlacementSpec(label='name:my-first-k8s-node').filter_matching_hostspecs([hostspec]) == [
        'name']

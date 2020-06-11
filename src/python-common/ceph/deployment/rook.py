from typing import Tuple, List, Optional

from kubernetes.client import V1Affinity, V1NodeAffinity, V1NodeSelector, \
    V1NodeSelectorTerm, V1NodeSelectorRequirement

from ceph.deployment.hostspec import HostSpec
from ceph.deployment.service_spec import PlacementSpec, ServiceSpecValidationError


def placement_spec_to_k8s(
        all_hosts: List[HostSpec],
        spec: PlacementSpec
) -> Tuple[Optional[int], V1Affinity]:
    """
    - node list
    - labels
    - count (?)
    - all nodes?
    - host glob?

    Q: podAntiAffinity
    A: might required in the future for scheduling pods.

    Q: topologySpreadConstraints
    A: might required in the future for scheduling pods.

    Q: pod(Anti)Affinity
    A: not yet supported in PlacementSpec

    :returns: <replica count>, V1Affinity
    """

    if spec.label:
        try:
            key, value = spec.label.split(':', 1)
        except ValueError:
            msg = f'label "{spec.label}" needs to be in the form of `key:value`'
            raise ServiceSpecValidationError(msg)

        return spec.count, V1Affinity(
            node_affinity=V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=V1NodeSelector(
                    node_selector_terms=[
                        V1NodeSelectorTerm(
                            match_expressions=[
                                V1NodeSelectorRequirement(
                                    key=key,
                                    operator='In',
                                    values=[
                                        value
                                    ]
                                )
                            ]
                        )
                    ]
                )
            )
        )
    hostnames = spec.filter_matching_hostspecs(all_hosts)
    if not hostnames or len(hostnames) == len(all_hosts):
        return spec.count, V1Affinity()
    return spec.count, V1Affinity(
            node_affinity=V1NodeAffinity(
                required_during_scheduling_ignored_during_execution=V1NodeSelector(
                    node_selector_terms=[
                        V1NodeSelectorTerm(
                            match_expressions=[
                                V1NodeSelectorRequirement(
                                    key='kubernetes.io/hostname',
                                    operator='In',
                                    values=[
                                        h
                                    ]
                                )
                            ]
                        )
                        for h in hostnames
                    ]
                )
            )
        )

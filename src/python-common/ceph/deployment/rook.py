
try:
    from typing import Dict, Any
    from .hostspec import HostSpec
except ImportError:
    pass  # just for type checking


def k8s_node_to_hostspec(node: Dict[str, Any]) -> HostSpec:
    """
    >>> k8s_node_to_hostspec({
    ...   "kind": "Node",
    ...   "apiVersion": "v1",
    ...   "metadata": {
    ...     "name": "10.240.79.157",
    ...     "labels": {
    ...       "name": "my-first-k8s-node"
    ...     }
    ...   }
    ... })
    HostSpec('10.240.79.157', '10.240.79.157', ['name:my-first-k8s-node'])

    """
    metadata = node.get('metadata', {})
    return HostSpec(
        hostname=metadata['name'],
        labels=[f'{k}:{v}' for k, v in metadata.get('labels', {}).items()]
    )

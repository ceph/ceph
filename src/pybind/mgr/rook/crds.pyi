# Warning.
# This file is generated. Do not touch.

from typing import List, Dict, Any


class CRD(object):
    def _to_dict(self) -> Dict:
        ...


class CephVersion(object):
    allowUnsupported: bool
    image: str
    name: str


class Dashboard(object):
    enabled: bool
    urlPrefix: str
    port: int


class Mon(object):
    allowMultiplePerNode: bool
    count: int
    preferredCount: int


class Network(object):
    hostNetwork: bool


class Storage(object):
    nodes: List
    useAllDevices: Any
    useAllNodes: bool


class Spec(object):
    cephVersion: CephVersion
    dashboard: Dashboard
    dataDirHostPath: str
    mon: Mon
    network: Network
    storage: Storage


class CephCluster(CRD):
    apiVersion: str
    kind: str
    metadata: Any
    spec: Spec

from ceph_node_proxy.baseredfishsystem import BaseRedfishSystem
from ceph_node_proxy.util import get_logger
from typing import Any


class AtollonSystem(BaseRedfishSystem):
    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.log = get_logger(__name__)

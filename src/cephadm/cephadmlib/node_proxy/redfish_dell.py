from .redfishdellchassis import RedfishDellChassis
from .redfishdellsystem import RedfishDellSystem
from .util import Logger
from typing import Any


class RedfishDell(RedfishDellSystem, RedfishDellChassis):
    def __init__(self, **kw: Any) -> None:
        if kw.get('system_endpoint') is None:
            kw['system_endpoint'] = '/Systems/System.Embedded.1'
        if kw.get('chassis_endpoint') is None:
            kw['chassis_endpoint'] = '/Chassis/System.Embedded.1'
        super().__init__(**kw)
        self.log = Logger(__name__)

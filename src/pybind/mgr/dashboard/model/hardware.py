"""
Model definitions for the hardware API.

node-proxy daemons poll the BMC via Redfish and POST the assembled report to
cephadm every 5 minutes. cephadm keeps one report per host in
NodeProxyCache.data (in-memory + KV store).
"""
from collections import Counter
from dataclasses import asdict, dataclass, fields
from enum import Enum
from typing import Any, Dict, List, Literal, Mapping, Optional, Sequence, \
    Tuple, Type, TypedDict, Union, cast

from ceph.deployment.hostspec import normalize_hostname

# pylint: disable=inherit-non-class

Hostname = str
ChassisId = str
ComponentId = str


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class Health(str, Enum):
    """
    API health strings. Members compare equal to (and serialize as) their
    string values. Lookup is case-insensitive, matching Redfish status.health
    values ("OK", "Warning", "Critical").

        >>> Health('critical')
        <Health.CRITICAL: 'Critical'>
        >>> Health.parse('something-else')
        <Health.UNKNOWN: 'Unknown'>
    """
    OK = 'OK'
    WARNING = 'Warning'
    CRITICAL = 'Critical'
    UNKNOWN = 'Unknown'

    @classmethod
    def _missing_(cls, value: object) -> Optional['Health']:
        if isinstance(value, str):
            for member in cls:
                if member.value.lower() == value.lower():
                    return member
        return None

    @classmethod
    def parse(cls, value: object) -> 'Health':
        """Like Health(value) but returns UNKNOWN for unrecognised values."""
        try:
            return cls(value)
        except ValueError:
            return cls.UNKNOWN

    @property
    def rank(self) -> int:
        """Severity ordering used to pick the worst health: higher is worse."""
        return _HEALTH_RANK[self]

    @staticmethod
    def worst(healths: Sequence['Health']) -> 'Health':
        """The most severe of healths; UNKNOWN if there are none."""
        return max(healths, key=lambda health: health.rank, default=Health.UNKNOWN)


_HEALTH_RANK = {
    Health.CRITICAL: 3,
    Health.WARNING: 2,
    Health.UNKNOWN: 1,
    Health.OK: 0,
}


# ---------------------------------------------------------------------------
# NodeProxyRawData: the report node-proxy sends, as cephadm stores it.
# node-proxy turns missing values into the string 'unknown'.
# ---------------------------------------------------------------------------

Unknown = Literal['unknown']
IntOrUnknown = Union[int, Unknown]
FloatOrUnknown = Union[float, Unknown]

NodeProxyRawData = Dict[Hostname, 'RawHostReport']


class RawHostReport(TypedDict, total=False):
    host: Hostname
    sn: str
    status: 'RawHardwareStatus'
    firmware: Dict[str, 'RawFirmware']
    firmwares: Dict[str, 'RawFirmware']  # sent by older node-proxy versions


class RawHardwareStatus(TypedDict, total=False):
    storage: Dict[ChassisId, Dict[ComponentId, 'RawStorage']]
    processors: Dict[ChassisId, Dict[ComponentId, 'RawProcessor']]
    memory: Dict[ChassisId, Dict[ComponentId, 'RawMemory']]
    network: Dict[ChassisId, Dict[ComponentId, 'RawNetwork']]
    power: Dict[ChassisId, Dict[ComponentId, 'RawPower']]
    fans: Dict[ChassisId, Dict[ComponentId, 'RawSensor']]
    temperatures: Dict[ChassisId, Dict[ComponentId, 'RawSensor']]
    fcm: Dict[ChassisId, Dict[ComponentId, 'RawFcm']]


class RawComponent(TypedDict, total=False):
    id: str
    status: Union['RawStatus', Unknown]


RawCategory = Mapping[ChassisId, Mapping[ComponentId, RawComponent]]


class RawStorage(RawComponent, total=False):
    description: str
    entity: str
    model: str
    protocol: str
    serial_number: str
    capacity_bytes: IntOrUnknown
    firmware_version: str
    slot: str
    speed_gbps: float
    location_indicator_active: Union[bool, Unknown]
    redfish_endpoint: str
    physical_location: Union['RawPhysicalLocation', Unknown]


class RawProcessor(RawComponent, total=False):
    description: str
    model: str
    manufacturer: str
    total_cores: IntOrUnknown
    total_threads: IntOrUnknown
    processor_type: str


class RawMemory(RawComponent, total=False):
    description: str
    memory_device_type: str
    capacity_mi_b: IntOrUnknown


class RawNetwork(RawComponent, total=False):
    name: str
    description: str
    speed_mbps: IntOrUnknown


class RawPower(RawComponent, total=False):
    name: str
    model: str
    manufacturer: str


class RawSensor(RawComponent, total=False):
    name: str
    physical_context: str
    reading: FloatOrUnknown
    reading_units: str


class RawFcm(TypedDict, total=False):
    device: str
    model: str
    serial_number: str
    valid: bool
    phy_size_bytes: int
    phy_util_bytes: int
    log_size_bytes: int
    log_util_bytes: int
    phy_util_percent: float
    log_util_percent: float
    compression_ratio: float
    compression_ratio_str: str
    savings_bytes: int
    compression_ratio_display: str
    savings_display: str
    phy_usage_display: str
    log_usage_display: str
    status: 'RawStatus'


class RawStatus(TypedDict, total=False):
    health: str
    healthrollup: str
    state: str


class RawPhysicalLocation(TypedDict, total=False):
    partlocation: 'RawPartLocation'


class RawPartLocation(TypedDict, total=False):
    locationordinalvalue: Union[int, str]
    locationtype: str


class RawFirmware(TypedDict, total=False):
    id: str
    name: str
    description: str
    version: str
    release_date: str
    updateable: Union[bool, Unknown]
    status: Union[RawStatus, Unknown]


# ---------------------------------------------------------------------------
# Responses: GET /api/hardware/summary and /api/hardware/compression
# ---------------------------------------------------------------------------

class HealthCounts(TypedDict):
    total: int
    ok: int
    warn: int
    critical: int


class SummaryTotals(TypedDict):
    category: Dict[str, HealthCounts]
    total: HealthCounts


class HostFlawed(TypedDict):
    flawed: bool


class HardwareSummary(TypedDict):
    total: SummaryTotals
    # hostname -> HostFlawed, plus 'flawed' -> number of flawed hosts
    host: Dict[str, Union[HostFlawed, int]]


class CompressionStats(TypedDict):
    fcm_drive_count: int
    phy_util_percent: Optional[float]
    log_util_percent: Optional[float]
    savings_bytes: Optional[int]
    compression_ratio: Optional[float]
    efficiency_percent: Optional[float]

# pylint: enable=inherit-non-class


# ---------------------------------------------------------------------------
# Responses: GET /api/hardware/hosts and /api/hardware/hosts/{hostname}
# ---------------------------------------------------------------------------

@dataclass
class HardwareHostResponse:
    hostname: Hostname
    sn: str
    health: Health
    status: 'HardwareStatus'
    firmware: Dict[str, str]

    @classmethod
    def from_raw(cls, raw: RawHostReport) -> 'HardwareHostResponse':
        status = HardwareStatus.from_raw(raw['status'])
        firmware = raw.get('firmware', raw.get('firmwares', {}))
        return cls(normalize_hostname(raw['host']), raw['sn'], status.health, status,
                   _firmware_versions(firmware))

    def to_dict(self) -> Dict[str, object]:
        return asdict(self, dict_factory=_without_absent)


@dataclass
class HardwareStatus:
    storage: 'Category'
    processors: 'Category'
    memory: 'Category'
    network: 'Category'
    power: 'Category'
    fans: 'Category'
    temperatures: 'Category'

    @classmethod
    def from_raw(cls, raw: RawHardwareStatus) -> 'HardwareStatus':
        return cls(
            storage=Category.from_raw(StorageComponent, raw.get('storage', {})),
            processors=Category.from_raw(ProcessorComponent, raw.get('processors', {})),
            memory=Category.from_raw(MemoryComponent, raw.get('memory', {})),
            network=Category.from_raw(NetworkComponent, raw.get('network', {})),
            power=Category.from_raw(PowerComponent, raw.get('power', {})),
            fans=Category.from_raw(SensorComponent, raw.get('fans', {})),
            temperatures=Category.from_raw(SensorComponent, raw.get('temperatures', {})),
        )

    @property
    def health(self) -> Health:
        """Worst health of the categories that have components."""
        categories = [getattr(self, f.name) for f in fields(self)]
        return Health.worst([category.health for category in categories if category.total])


@dataclass
class Category:
    health: Health
    total: int
    ok: int
    warning: int
    critical: int
    unknown: int
    components: List['Component']

    @classmethod
    def from_raw(cls, kind: Type['Component'], raw: RawCategory) -> 'Category':
        components = [kind.from_raw(chassis_id, component_id, component)
                      for chassis_id, chassis in raw.items()
                      for component_id, component in chassis.items()
                      if isinstance(component, dict)]
        counts = Counter(component.health for component in components)
        return cls(Health.worst(list(counts)), len(components),
                   counts[Health.OK], counts[Health.WARNING], counts[Health.CRITICAL],
                   counts[Health.UNKNOWN], components)


@dataclass
class Component:
    id: ComponentId
    chassis_id: ChassisId
    health: Health
    state: str

    @classmethod
    def from_raw(cls, chassis_id: ChassisId, component_id: ComponentId,
                 raw: RawComponent) -> 'Component':
        values: Mapping[str, Any] = {**raw, 'slot': _slot(raw)}
        own_fields = fields(cls)[len(fields(Component)):]
        return cls(component_id, chassis_id, _health(raw), _state(raw),
                   **{f.name: values[f.name] for f in own_fields if f.name in values})


@dataclass
class StorageComponent(Component):
    description: Optional[str] = None
    model: Optional[str] = None
    protocol: Optional[str] = None
    serial_number: Optional[str] = None
    capacity_bytes: Optional[IntOrUnknown] = None
    firmware_version: Optional[str] = None
    slot: Optional[Union[int, str]] = None


@dataclass
class ProcessorComponent(Component):
    description: Optional[str] = None
    model: Optional[str] = None
    manufacturer: Optional[str] = None
    total_cores: Optional[IntOrUnknown] = None
    total_threads: Optional[IntOrUnknown] = None
    processor_type: Optional[str] = None


@dataclass
class MemoryComponent(Component):
    description: Optional[str] = None
    memory_device_type: Optional[str] = None
    capacity_mi_b: Optional[IntOrUnknown] = None


@dataclass
class NetworkComponent(Component):
    name: Optional[str] = None
    description: Optional[str] = None
    speed_mbps: Optional[IntOrUnknown] = None


@dataclass
class PowerComponent(Component):
    name: Optional[str] = None
    model: Optional[str] = None
    manufacturer: Optional[str] = None


@dataclass
class SensorComponent(Component):
    name: Optional[str] = None
    physical_context: Optional[str] = None
    reading: Optional[FloatOrUnknown] = None
    reading_units: Optional[str] = None


def _health(raw: RawComponent) -> Health:
    status = raw.get('status')
    if isinstance(status, dict):
        return Health.parse(status.get('health'))
    return Health.parse(status)


def _state(raw: RawComponent) -> str:
    status = raw.get('status')
    return status.get('state', '') if isinstance(status, dict) else ''


def _slot(raw: RawComponent) -> Optional[Union[int, str]]:
    """Drive bay: 'slot' on Atollon, physical_location on other hardware."""
    values = cast(Mapping[str, Any], raw)
    try:
        return values['slot']
    except KeyError:
        pass
    try:
        return values['physical_location']['partlocation']['locationordinalvalue']
    except (KeyError, TypeError):
        return None


def _firmware_versions(firmware: Mapping[str, RawFirmware]) -> Dict[str, str]:
    """
    {name: version} of the installed firmware. The report keys are opaque Redfish
    ids; Dell also lists the rollback image under the same name with a
    'previous-' id, which is skipped. Unknown versions are skipped.
    """
    versions: Dict[str, str] = {}
    for firmware_id, entry in firmware.items():
        if firmware_id.lower().startswith('previous-'):
            continue
        try:
            name, version = entry['name'], entry['version']
        except (KeyError, TypeError):
            continue
        if name and version and version != 'unknown':
            versions[name] = version
    return versions


def _without_absent(items: List[Tuple[str, object]]) -> Dict[str, object]:
    """Fields the report did not send are left out; 'slot' is always present."""
    return {key: value for key, value in items if value is not None or key == 'slot'}

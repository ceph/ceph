from enum import Flag, auto
from typing import Annotated, List, NamedTuple, Optional


class CliFlags(Flag):
    DROP = auto()
    EXCLUSIVE_LIST = auto()
    EXCLUSIVE_RESULT = auto()
    SIZE = auto()


class CliHeader:
    def __init__(self, label: str):
        self.label = label


class GatewayInfo(NamedTuple):
    bool_status: Annotated[bool, CliFlags.DROP]
    status: int
    error_message: str
    hostname: str
    cli_version: Annotated[str, CliFlags.DROP]
    version: str
    name: str
    group: str
    addr: str
    port: int
    load_balancing_group: Annotated[int, CliHeader('LB Group')]
    max_hosts: Annotated[int, CliFlags.DROP]
    max_hosts_per_subsystem: Annotated[int, CliFlags.DROP]
    max_namespaces: Annotated[int, CliFlags.DROP]
    max_namespaces_per_subsystem: Annotated[int, CliFlags.DROP]
    max_subsystems: Annotated[int, CliFlags.DROP]
    spdk_version: Optional[str] = ""


class GatewayVersion(NamedTuple):
    status: int
    error_message: str
    version: str


class GatewayLogLevelInfo(NamedTuple):
    status: int
    error_message: str
    log_level: str


class NvmfLogFLag(NamedTuple):
    name: str
    enabled: bool


class SpdkNvmfLogFlagsAndLevelInfo(NamedTuple):
    status: int
    error_message: str
    log_level: str
    log_print_level: str
    nvmf_log_flags: List[NvmfLogFLag]


class Subsystem(NamedTuple):
    nqn: str
    enable_ha: Annotated[bool, CliFlags.DROP]
    serial_number: str
    model_number: str
    min_cntlid: Annotated[int, CliFlags.DROP]
    max_cntlid: Annotated[int, CliFlags.DROP]
    namespace_count: int
    subtype: str
    max_namespaces: int
    has_dhchap_key: bool
    allow_any_host: bool
    created_without_key: bool = False


class SubsystemList(NamedTuple):
    status: int
    error_message: str
    subsystems: Annotated[List[Subsystem], CliFlags.EXCLUSIVE_LIST]


class SubsystemStatus(NamedTuple):
    status: int
    error_message: str
    nqn: str


class Connection(NamedTuple):
    traddr: str
    trsvcid: int
    trtype: str
    adrfam: int
    connected: bool
    qpairs_count: int
    controller_id: int
    use_psk: Optional[bool]
    use_dhchap: Optional[bool]
    subsystem: Optional[str]
    disconnected_due_to_keepalive_timeout: Optional[bool]


class ConnectionList(NamedTuple):
    status: int
    error_message: str
    subsystem_nqn: str
    connections: Annotated[List[Connection], CliFlags.EXCLUSIVE_LIST]


class NamespaceCreation(NamedTuple):
    status: Annotated[int, CliFlags.EXCLUSIVE_RESULT]
    error_message: str
    nsid: int


class Namespace(NamedTuple):
    bdev_name: str
    rbd_image_name: Annotated[str, CliHeader("RBD Image")]
    rbd_pool_name: Annotated[str, CliHeader("RBD Pool")]
    load_balancing_group: Annotated[int, CliHeader('LB Group')]
    rbd_image_size: Annotated[int, CliFlags.SIZE]
    block_size: Annotated[int, CliFlags.SIZE]
    rw_ios_per_second: Annotated[int, CliHeader('R/W IOs/sec')]
    rw_mbytes_per_second: Annotated[int, CliHeader('R/W MBs/sec')]
    r_mbytes_per_second: Annotated[int, CliHeader('Read MBs/sec')]
    w_mbytes_per_second: Annotated[int, CliHeader('Write MBs/sec')]
    auto_visible: bool
    hosts: List[str]
    nsid: Optional[int]
    uuid: Optional[str]
    ns_subsystem_nqn: Optional[str]
    trash_image: Optional[bool]
    disable_auto_resize: Optional[bool]
    read_only: Optional[bool]


class NamespaceList(NamedTuple):
    status: int
    error_message: str
    namespaces: Annotated[List[Namespace], CliFlags.EXCLUSIVE_LIST]


class NamespaceIOStats(NamedTuple):
    status: Annotated[int, CliFlags.DROP]
    error_message: Annotated[str, CliFlags.DROP]
    subsystem_nqn: str
    nsid: int
    uuid: str
    bdev_name: str
    tick_rate: int
    ticks: int
    bytes_read: Annotated[int, CliFlags.SIZE]
    num_read_ops: int
    bytes_written: Annotated[int, CliFlags.SIZE]
    num_write_ops: int
    bytes_unmapped: Annotated[int, CliFlags.SIZE]
    num_unmap_ops: int
    read_latency_ticks: int
    max_read_latency_ticks: int
    min_read_latency_ticks: int
    write_latency_ticks: int
    max_write_latency_ticks: int
    min_write_latency_ticks: int
    unmap_latency_ticks: int
    max_unmap_latency_ticks: int
    min_unmap_latency_ticks: int
    copy_latency_ticks: int
    max_copy_latency_ticks: int
    min_copy_latency_ticks: int
    io_error: List[int]


class Listener(NamedTuple):
    host_name: str
    trtype: str
    traddr: str
    secure: bool
    adrfam: int = 0  # 0: IPv4, 1: IPv6
    trsvcid: int = 4420


class ListenerList(NamedTuple):
    status: int
    error_message: str
    listeners: Annotated[List[Listener], CliFlags.EXCLUSIVE_LIST]


class Host(NamedTuple):
    nqn: str
    use_psk: Optional[bool]
    use_dhchap: Optional[bool]
    disconnected_due_to_keepalive_timeout: Annotated[Optional[bool], CliFlags.DROP]


class HostsInfo(NamedTuple):
    status: int
    error_message: str
    allow_any_host: bool
    subsystem_nqn: str
    hosts: Annotated[List[Host], CliFlags.EXCLUSIVE_LIST]


class RequestStatus(NamedTuple):
    status: Annotated[int, CliFlags.EXCLUSIVE_RESULT]
    error_message: str

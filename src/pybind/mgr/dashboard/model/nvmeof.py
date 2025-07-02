from typing import List, NamedTuple, Optional


class GatewayInfo(NamedTuple):
    bool_status: bool
    status: int
    error_message: str
    hostname: str
    cli_version: str
    version: str
    name: str
    group: str
    addr: str
    port: int
    load_balancing_group: int
    max_hosts: int
    max_hosts_per_subsystem: int
    max_namespaces: int
    max_namespaces_per_subsystem: int
    max_subsystems: int
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
    enable_ha: bool
    serial_number: str
    model_number: str
    min_cntlid: int
    max_cntlid: int
    namespace_count: int
    subtype: str
    max_namespaces: int


class SubsystemList(NamedTuple):
    status: int
    error_message: str
    subsystems: List[Subsystem]


class Connection(NamedTuple):
    traddr: str
    trsvcid: int
    trtype: str
    adrfam: int
    connected: bool
    qpairs_count: int
    controller_id: int
    subsystem: Optional[str]


class ConnectionList(NamedTuple):
    status: int
    error_message: str
    subsystem_nqn: str
    connections: List[Connection]


class NamespaceCreation(NamedTuple):
    status: int
    error_message: str
    nsid: int


class Namespace(NamedTuple):
    nsid: Optional[int]
    uuid: Optional[str]
    bdev_name: str
    rbd_image_name: str
    rbd_pool_name: str
    load_balancing_group: int
    rbd_image_size: int
    block_size: int
    rw_ios_per_second: int
    rw_mbytes_per_second: int
    r_mbytes_per_second: int
    w_mbytes_per_second: int
    trash_image: bool


class NamespaceList(NamedTuple):
    status: int
    error_message: str
    namespaces: List[Namespace]


class NamespaceIOStats(NamedTuple):
    status: int
    error_message: str
    subsystem_nqn: str
    nsid: int
    uuid: str
    bdev_name: str
    tick_rate: int
    ticks: int
    bytes_read: int
    num_read_ops: int
    bytes_written: int
    num_write_ops: int
    bytes_unmapped: int
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
    adrfam: int = 0  # 0: IPv4, 1: IPv6
    trsvcid: int = 4420


class ListenerList(NamedTuple):
    status: int
    error_message: str
    listeners: List[Listener]


class Host(NamedTuple):
    nqn: str


class HostsInfo(NamedTuple):
    status: int
    error_message: str
    allow_any_host: bool
    subsystem_nqn: str
    hosts: List[Host]


class RequestStatus(NamedTuple):
    status: int
    error_message: str

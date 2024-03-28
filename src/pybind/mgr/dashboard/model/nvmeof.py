from typing import NamedTuple, Optional


class GatewayInfo(NamedTuple):
    cli_version: str
    version: str
    name: str
    group: str
    addr: str
    port: int
    load_balancing_group: int
    spdk_version: Optional[str] = ""


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


class Connection(NamedTuple):
    traddr: str
    trsvcid: int
    trtype: str
    adrfam: int
    connected: bool
    qpairs_count: int
    controller_id: int


class NamespaceCreation(NamedTuple):
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


class NamespaceIOStats(NamedTuple):
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
    # io_error: List[int]


class Listener(NamedTuple):
    gateway_name: str
    trtype: str
    traddr: str
    adrfam: Optional[str] = "ipv4"
    trsvcid: Optional[int] = 4420


class Host(NamedTuple):
    nqn: str

# -*- coding: utf-8 -*-
# This file is moved from the original work of "nvmeof-top" tool in:
# https://github.com/pcuzner/ceph-nvmeof-top
# by Paul Cuzner <pcuzner@ibm.com>
import errno
import ipaddress
import json
import logging
import time
from typing import Any, Optional

from mgr_module import HandleCommandResult

from .. import mgr
from ..cli import DBCLICommand

logger = logging.getLogger(__name__)

NvmeofTopCollector = None

try:
    from .nvmeof_cli import NvmeofGatewaysConfig
    from .nvmeof_client import NVMeoFClient
    from .nvmeof_conf import get_pool_group_name
except ImportError as e:
    logger.error("Failed to import NVMeoFClient and related components: %s", e)
else:
    MAX_SESSION_TTL = 60 * 60

    def get_collector(session_id: Optional[str]):
        return mgr.get_nvmeof_collector(session_id, MAX_SESSION_TTL)

    def get_lbg_gws_map(service_name: str):
        pool_group = get_pool_group_name(service_name)
        if not pool_group:
            logger.error("Error getting pool name and group name of the service")
            return {}
        pool, group = pool_group
        try:
            cmd = {
                'prefix': 'nvme-gw show',
                'pool': pool,
                'group': group,
                'format': 'json'
            }
            ret_status, out, _ = mgr.mon_command(cmd)
            if ret_status == 0 and out is not None:
                lbg_gws_map = {}
                gws_info = json.loads(out)
                for gw in gws_info["Created Gateways:"]:
                    gw_id = str(gw["gw-id"]).removeprefix("client.")
                    gw_lbg = int(gw["anagrp-id"])
                    lbg_gws_map[gw_lbg] = gw_id
                return lbg_gws_map
            return {}
        except Exception:  # pylint: disable=broad-except
            logger.exception('Failed to get nvme-gw show command')
            return {}

    class Health:
        def __init__(self):
            self.rc = 0
            self.msg = ''

    class Counter:
        def __init__(self):
            self.current = 0.0
            self.last = 0.0

        def update(self, new_value: float):
            """Update the stats maintaining current and last"""
            self.last = self.current
            self.current = new_value

        def rate(self, interval: float):
            """Calculate the per second change rate"""
            if not interval:
                return 0.0
            return (self.current - self.last) / interval

    class PerformanceStats:  # pylint: disable=too-many-instance-attributes
        def __init__(self, bdev: str):
            self.bdev = bdev
            self.read_ops = Counter()
            self.read_bytes = Counter()
            self.read_secs = Counter()
            self.write_ops = Counter()
            self.write_bytes = Counter()
            self.write_secs = Counter()

            self.read_ops_rate = 0
            self.write_ops_rate = 0
            self.read_bytes_rate = 0
            self.write_bytes_rate = 0
            self.read_secs_rate = 0.0
            self.write_secs_rate = 0.0
            self.total_ops_rate = 0
            self.rareq_sz = 0.0
            self.wareq_sz = 0.0
            self.r_await = 0.0
            self.w_await = 0.0

        def calculate(self, delay: float):
            self.read_ops_rate = self.read_ops.rate(delay)
            self.read_bytes_rate = self.read_bytes.rate(delay)
            self.read_secs_rate = self.read_secs.rate(delay)
            self.write_ops_rate = self.write_ops.rate(delay)
            self.write_bytes_rate = self.write_bytes.rate(delay)
            self.write_secs_rate = self.write_secs.rate(delay)

            self.total_ops_rate = self.read_ops_rate + self.write_ops_rate

            if self.read_ops_rate:
                self.rareq_sz = (int(self.read_bytes_rate / self.read_ops_rate) / 1024)
                self.r_await = ((self.read_secs_rate / self.read_ops_rate) * 1000)  # for ms
            else:
                self.rareq_sz = 0.0
                self.r_await = 0.0
            if self.write_ops_rate:
                self.wareq_sz = (int(self.write_bytes_rate / self.write_ops_rate) / 1024)
                self.w_await = ((self.write_secs_rate / self.write_ops_rate) * 1000)  # for ms
            else:
                self.wareq_sz = 0.0
                self.w_await = 0.0

    class ReactorStats:
        def __init__(self, thread: str):
            self.thread = thread
            self.busy_secs = Counter()
            self.idle_secs = Counter()

            self.busy_rate = 0.0
            self.idle_rate = 0.0

        def calculate(self, delay: float):
            self.busy_rate = self.busy_secs.rate(delay)
            self.idle_rate = self.idle_secs.rate(delay)

    class NvmeofTopCollector:  # type: ignore[no-redef]  # noqa  # pylint: disable=function-redefined,too-many-instance-attributes
        def __init__(self):
            self.tool: Any = None
            self.subsystem_nqn = ''
            self.service = ''
            self.group = ''
            self.delay: float = 0.0
            self.namespaces = {}
            self.lbg_to_gateway: dict = {}
            self.subsystems: Any = None
            self.reactor_stats = {}
            self.iostats = {}
            self.gw_info: Any = None
            self.client: Any = None
            self.timestamp = time.time()
            self.health = Health()
            self.clients: dict = {}

        @property
        def nqn_list(self):
            return [subsys.nqn for subsys in self.subsystems.subsystems]

        @property
        def ready(self) -> bool:
            return self.health.rc == 0

        @property
        def total_namespaces_defined(self) -> int:
            return len(self.namespaces[self.subsystem_nqn])

        @property
        def total_subsystems(self) -> int:
            return len(self.nqn_list)

        @property
        def total_namespaces_overall(self):
            total = 0
            for subsys in self.subsystems.subsystems:
                total += subsys.namespace_count
            return total

        @property
        def max_namespaces(self):
            for subsys in self.subsystems.subsystems:
                if subsys.nqn == self.subsystem_nqn:
                    return subsys.max_namespaces
            logger.error("Request for max namespaces could not find a "
                         "match against the NQN! Returning 0")
            return 0

        @property
        def load_balancing_group(self):
            return self.gw_info.load_balancing_group

        def get_sorted_namespaces(self, sort_pos: int, reverse_sort: bool):
            logger.debug("get_sorted_namespaces")
            ns_data = []
            for ns in self.namespaces[self.subsystem_nqn]:
                bdev_name = ns.bdev_name

                daemon_name = ""
                if self.tool.args.get('server_address'):
                    # only show namespaces owned by this gateway's LBG
                    if ns.load_balancing_group != self.load_balancing_group:
                        continue
                    daemon_name = self.client.daemon_name
                else:
                    daemon_name = self.lbg_to_gateway.get(ns.load_balancing_group, '')
                if not daemon_name:
                    logger.warning("No gateway found for load balancing group %s, "
                                   "skipping namespace %s",
                                   ns.load_balancing_group, ns.nsid)
                    continue
                perf_stats = self.iostats.get(daemon_name, {}).get(bdev_name)
                if perf_stats is None:
                    logger.warning("No iostats for bdev %s on %s, skipping namespace %s",
                                   bdev_name, daemon_name, ns.nsid)
                    continue
                perf_stats.calculate(self.delay)

                if ns.rados_namespace_name:
                    rbd_image_path = (f"{ns.rbd_pool_name}/{ns.rados_namespace_name}/"
                                      f"{ns.rbd_image_name}")
                else:
                    rbd_image_path = f"{ns.rbd_pool_name}/{ns.rbd_image_name}"

                ns_data.append((
                    ns.nsid,
                    rbd_image_path,
                    int(perf_stats.total_ops_rate),
                    int(perf_stats.read_ops_rate),
                    f"{self.bytes_to_MB(perf_stats.read_bytes_rate):3.2f}",
                    f"{perf_stats.r_await:3.2f}",
                    f"{perf_stats.rareq_sz:4.2f}",
                    int(perf_stats.write_ops_rate),
                    f"{self.bytes_to_MB(perf_stats.write_bytes_rate):3.2f}",
                    f"{perf_stats.w_await:3.2f}",
                    f"{perf_stats.wareq_sz:4.2f}",
                    self.lb_group(ns.load_balancing_group),
                    self.qos_enabled(ns)
                ))

            ns_data.sort(key=lambda t: t[sort_pos], reverse=reverse_sort)
            return ns_data

        def get_reactor_data(self, sort_pos: int, reverse_sort: bool):
            reactor_data = []
            for gw_addr, threads in self.reactor_stats.items():
                for _, thread_stats in threads.items():
                    thread_stats.calculate(self.delay)
                    reactor_data.append((
                        gw_addr,
                        thread_stats.thread,
                        min(thread_stats.busy_rate * 100, 100.0),
                        min(thread_stats.idle_rate * 100, 100.0),
                    ))
            reactor_data.sort(key=lambda t: t[sort_pos], reverse=reverse_sort)
            return reactor_data

        def get_subsystem_summary_data(self):
            return [
                self.subsystem_nqn,
                f'{self.total_namespaces_defined} / {self.max_namespaces}',
            ]

        def get_overall_summary_data(self):
            return [
                self.group,
                self.total_subsystems,
                self.total_namespaces_overall,
            ]

        def get_gateway_summary_data(self):
            return [
                self.client.gateway_addr,
                self.load_balancing_group,
                self.total_subsystems,
                self.total_namespaces_overall,
            ]

        def qos_enabled(self, ns) -> str:
            if (ns.rw_ios_per_second or ns.rw_mbytes_per_second
                    or ns.r_mbytes_per_second or ns.w_mbytes_per_second):
                return 'Yes'
            return 'No'

        def lb_group(self, grp_id: int):
            """Provide a meaningful default when load-balancing is not in use"""
            return "N/A" if grp_id == 0 else f"{grp_id}"

        def bytes_to_MB(self, num_bytes: int, si: int = 1024):
            """Simple conversion of bytes to MiB or MB"""
            return (num_bytes / si) / si

        # grpc methods
        def _call_grpc(self, method_name, request, client=None):
            logger.debug("calling grpc method %s", method_name)
            if not client:
                client = self.client
            try:
                method = getattr(client.stub, method_name)
                response = method(request)
            except Exception as exc:  # pylint: disable=broad-except
                self.health.rc = -errno.ECONNREFUSED
                self.health.msg = f"RPC endpoint unavailable at {client.gateway_addr}"
                logger.error("grpc call to %s failed: %s (%s)", method_name, self.health.msg, exc)
                return None

            self.health.msg = f"{method_name} success"
            logger.debug("call to %s successful", method_name)
            return response

        def _fetch_namespace_iostats(self, client):
            daemon_name = client.daemon_name
            logger.debug("fetching iostats for namespaces from %s", daemon_name)
            stats = self._call_grpc('list_namespaces_io_stats',
                                    NVMeoFClient.pb2.list_namespaces_io_stats_req(), client)
            logger.debug("list_namespaces_io_stats stats=%s", stats)
            if stats is None:
                return
            if daemon_name not in self.iostats:
                self.iostats[daemon_name] = {}

            for ns in stats.namespaces:
                bdev_name = ns.bdev_name
                if bdev_name not in self.iostats[daemon_name]:
                    self.iostats[daemon_name][bdev_name] = PerformanceStats(bdev_name)

                ns_stats = self.iostats[daemon_name][bdev_name]
                ns_stats.read_ops.update(ns.num_read_ops)
                ns_stats.read_bytes.update(ns.bytes_read)
                ns_stats.read_secs.update((ns.read_latency_ticks / stats.tick_rate))
                ns_stats.write_ops.update(ns.num_write_ops)
                ns_stats.write_bytes.update(ns.bytes_written)
                ns_stats.write_secs.update((ns.write_latency_ticks / stats.tick_rate))

        def _fetch_namespaces(self, subsystem_nqn):
            return self._call_grpc(
                'list_namespaces',
                NVMeoFClient.pb2.list_namespaces_req(subsystem=subsystem_nqn))

        def _fetch_thread_stats(self, client):
            gateway_addr = client.gateway_addr
            logger.debug("fetching thread stats for %s", gateway_addr)
            stats = self._call_grpc('get_thread_stats',
                                    NVMeoFClient.pb2.get_thread_stats_req(), client)
            logger.debug("get_thread_stats stats=%s", stats)
            if stats is None:
                return
            if gateway_addr not in self.reactor_stats:
                self.reactor_stats[gateway_addr] = {}
            tick_rate = stats.tick_rate
            for thread in stats.threads:
                name = thread.name
                if name not in self.reactor_stats[gateway_addr]:
                    self.reactor_stats[gateway_addr][name] = ReactorStats(thread.name)
                thread_stats = self.reactor_stats[gateway_addr][name]
                thread_stats.busy_secs.update(thread.busy / tick_rate)
                thread_stats.idle_secs.update(thread.idle / tick_rate)

        def _fetch_gateway_info(self, client):
            return self._call_grpc(
                'get_gateway_info',
                NVMeoFClient.pb2.get_gateway_info_req(), client)

        def _fetch_subsystems(self):
            return self._call_grpc('list_subsystems', NVMeoFClient.pb2.list_subsystems_req())

        def _get_client(self, group, server_addr):
            key = (group, server_addr)
            if key not in self.clients:
                self.clients[key] = NVMeoFClient(group, server_addr)
            return self.clients[key]

        def _set_gateways(self, group_filter: str, addr_filter: str,
                          port_filter: Optional[int] = None):
            if self.service and self.group:
                return

            services = NvmeofGatewaysConfig.get_gateways_config().get("gateways", {})

            if not services:
                self.health.rc = -errno.ENOENT
                self.health.msg = "No NVMeoF gateways configured"
                return

            if not addr_filter and not group_filter and len(services) > 1:
                self.health.rc = -errno.EINVAL
                self.health.msg = (
                    f"Multiple gateway groups found: {', '.join(services.keys())}. "
                    "Provide --gw-group <name>"
                )
                return

            matched_service_name = ''
            matched_gws = []
            for svc_name, svc_gateways in services.items():
                for gw in svc_gateways:
                    gw_host, _, gw_port = gw['service_url'].rpartition(':')
                    gw_host = gw_host.strip('[]')
                    if (addr_filter and addr_filter != gw_host) or \
                            (port_filter and str(port_filter) != gw_port):
                        continue
                    if group_filter and gw.get('group') != group_filter:
                        if addr_filter:
                            self.health.rc = -errno.EINVAL
                            self.health.msg = (
                                f"Address '{addr_filter}' belongs to group "
                                f"'{gw.get('group')}', not '{group_filter}'"
                            )
                            return
                        continue
                    matched_service_name = svc_name
                    matched_gws.append(gw)

            if not matched_gws:
                if addr_filter:
                    self.health.rc = -errno.ENOENT
                    self.health.msg = f"No gateway found matching address: {addr_filter}"
                elif group_filter:
                    self.health.rc = -errno.ENOENT
                    self.health.msg = f"Gateway group '{group_filter}' not found"
                return

            self.service = matched_service_name
            self.group = matched_gws[0].get('group', '')
            for gw in matched_gws:
                self._get_client(self.group, gw['service_url'])

        def initialise(self, tool):
            self.health = Health()
            self.tool = tool

            self._set_gateways(
                group_filter=tool.args.get('gw_group', ''),
                addr_filter=tool.args.get('server_address', ''),
                port_filter=tool.args.get('server_port')
            )
            if not self.ready:
                return

            now = time.time()
            self.delay = (now - self.timestamp)
            self.timestamp = now

            self.client = next(iter(self.clients.values()))

            if self.gw_info is None:
                self.gw_info = self._fetch_gateway_info(self.client)
                if not self.ready:
                    self.health.msg = f"Unable to connect to {self.client.gateway_addr}"
                    return
                logger.debug("Connected to %s", self.client.gateway_addr)

        def collect_cpu_data(self):
            for client in self.clients.values():
                self._fetch_thread_stats(client)
                if not self.ready:
                    return
            logger.debug("collect_cpu_data completed")

        def _set_subsystem_and_namespaces(self):
            if self.subsystems is not None and self.subsystem_nqn in self.namespaces:
                return

            self.subsystems = self._fetch_subsystems()
            if self.subsystems is None or self.subsystems.status > 0:
                logger.error("Failed to retrieve subsystems list")
                self.health.rc = -errno.ECONNREFUSED
                self.health.msg = "Unable to retrieve a list of subsystems"
                return

            if self.total_subsystems == 0:
                self.health.rc = -errno.ENOENT
                self.health.msg = 'No subsystems found'
                return

            if self.subsystem_nqn and self.subsystem_nqn not in self.nqn_list:
                logger.error("nqn provided is not present on the gateway")
                self.health.rc = -errno.ENOENT
                self.health.msg = "Subsystem NQN provided not found"
                return

            namespace_info = self._fetch_namespaces(self.subsystem_nqn)
            if namespace_info is None:
                return

            self.namespaces[self.subsystem_nqn] = namespace_info.namespaces
            logger.debug("Subsystem '%s' has %s namespaces",
                         self.subsystem_nqn, self.total_namespaces_defined)

        def collect_io_data(self):
            self.subsystem_nqn = self.tool.subsystem_nqn

            self._set_subsystem_and_namespaces()
            if not self.ready:
                return

            if not self.tool.args.get('server_address'):
                self.lbg_to_gateway = get_lbg_gws_map(self.service)
                if not self.lbg_to_gateway:
                    self.health.rc = -errno.ENOENT
                    self.health.msg = (
                        f'Failed to retrieve load balancing group '
                        f'mapping for service {self.service}'
                    )
                    return
            for client in self.clients.values():
                self._fetch_namespace_iostats(client)
                if not self.ready:
                    return
            logger.debug("collect_io_data completed")

    class NVMeoFTopTool:
        def __init__(self, args: dict):
            self.args = args
            self.collector: Any = None
            self.reverse_sort = args.get('sort_descending', False)
            self.sort_key = args.get('sort_by')

        def _validate_args(self) -> Optional[tuple]:
            period = self.args.get('period', 1)
            if not 1 <= period <= MAX_SESSION_TTL:
                return (-errno.EINVAL,
                        f"Invalid period '{period}': must be between 1 and {MAX_SESSION_TTL}")
            server_address = self.args.get('server_address', '')
            if server_address:
                try:
                    ipaddress.ip_address(server_address)
                except Exception:  # pylint: disable=broad-except
                    return (-errno.EINVAL,
                            f"Invalid server-address '{server_address}': "
                            "must be a valid IP address")
            server_port = self.args.get('server_port')
            if server_port is not None and not 1 <= server_port <= 65535:
                return (-errno.EINVAL,
                        f"Invalid server-port '{server_port}': "
                        "must be between 1 and 65535")
            return None

        def run(self) -> tuple:
            try:
                err = self._validate_args()
                if err:
                    return err
                self.collector = get_collector(self.args.get('session_id'))
                if self.collector is None:
                    return (-errno.EINVAL, "Unable to initialise collector")
                self.collector.initialise(self)
                if not self.collector.ready:
                    return (self.collector.health.rc,
                            f"nvmeof-top has encountered an error: "
                            f"{self.collector.health.msg}")

                collect_start = time.time()
                self._collect()
                logger.info("collector methods took %.2fs", time.time() - collect_start)

                if not self.collector.ready:
                    return (self.collector.health.rc, self.collector.health.msg)

                output = self.format_output()
                output += "\n ---- "
                return (0, output)
            except Exception as ex:  # pylint: disable=broad-except
                logger.exception("top tool failed to run: %s", ex)
                return (-errno.EINVAL, str(ex))

        def _collect(self):
            raise NotImplementedError

        def format_output(self):
            raise NotImplementedError

    class NVMeoFTopCPU(NVMeoFTopTool):
        reactors_headers = ['Gateway', 'Thread Name', 'Busy Rate%', 'Idle Rate%']
        reactors_template = "{:<30}   {:<30}   {:<20}   {:<20}\n"

        def _collect(self):
            self.collector.collect_cpu_data()

        def format_output(self):
            if self.sort_key not in NVMeoFTopCPU.reactors_headers:
                raise ValueError(
                    f"Invalid sort key '{self.sort_key}'. "
                    f"Valid options: {NVMeoFTopCPU.reactors_headers}"
                )
            sort_pos = NVMeoFTopCPU.reactors_headers.index(self.sort_key)
            reactor_data = self.collector.get_reactor_data(sort_pos=sort_pos,
                                                           reverse_sort=self.reverse_sort)
            rows = []
            if self.args.get('with_timestamp'):
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S',
                                          time.localtime(self.collector.timestamp))
                rows.append(f"{timestamp} (delay: {self.collector.delay:.2f}s)\n")

            if not self.args.get('no_header'):
                rows.append(NVMeoFTopCPU.reactors_template.format(*NVMeoFTopCPU.reactors_headers))
            for gw_addr, thread_name, busy_rate, idle_rate in reactor_data:
                rows.append(NVMeoFTopCPU.reactors_template.format(
                    gw_addr, thread_name, f"{busy_rate:.2f}", f"{idle_rate:.2f}"
                ))
            rows.append("\n")

            return ''.join(rows)

    class NVMeoFTopIO(NVMeoFTopTool):
        subsystem_summary_headers = ['Subsystem', 'Namespaces']
        gateway_summary_headers = ['Gateway', 'Load Balancing Group',
                                   'Total Subsystems', 'Total Namespaces']
        summary_headers = ['Group', 'Total Subsystems', 'Total Namespaces']

        ns_headers = [
            'NSID', 'RBD Image', 'IOPS', 'r/s', 'rMB/s', 'r_await', 'rareq-sz',
            'w/s', 'wMB/s', 'w_await', 'wareq-sz', 'LBGrp', 'QoS'
        ]
        ns_template = (
            "{:>4}   {:<40}   {:>7}   {:>6}   {:>6}   {:>7}   {:>8}"
            "   {:>6}   {:>6}   {:>7}   {:>8}   {:^5}   {:>3}\n"
        )

        def __init__(self, args: dict):
            super().__init__(args)
            self.subsystem_nqn = args.get('nqn')

        def _collect(self):
            self.collector.collect_io_data()

        def format_output(self):
            if self.sort_key not in NVMeoFTopIO.ns_headers:
                raise ValueError(
                    f"Invalid sort key '{self.sort_key}'. "
                    f"Valid options: {NVMeoFTopIO.ns_headers}"
                )
            sort_pos = NVMeoFTopIO.ns_headers.index(self.sort_key)
            ns_data = self.collector.get_sorted_namespaces(sort_pos=sort_pos,
                                                           reverse_sort=self.reverse_sort)
            subsystem_summary_data = self.collector.get_subsystem_summary_data()
            overall_summary_data = self.collector.get_overall_summary_data()

            rows = []
            if self.args.get('with_timestamp'):
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S',
                                          time.localtime(self.collector.timestamp))
                rows.append(f"{timestamp} (delay: {self.collector.delay:.2f}s)\n")
            if self.args.get('summary'):
                summary_row = ""
                if self.args.get('server_address'):
                    gateway_summary_data = self.collector.get_gateway_summary_data()
                    for index, header in enumerate(NVMeoFTopIO.gateway_summary_headers):
                        summary_row += f"{header}: {gateway_summary_data[index]}  "
                else:
                    for index, header in enumerate(NVMeoFTopIO.summary_headers):
                        summary_row += f"{header}: {overall_summary_data[index]}  "
                rows.append(summary_row + "\n")
                subsys_summary_row = ""
                for index, header in enumerate(NVMeoFTopIO.subsystem_summary_headers):
                    subsys_summary_row += f"{header}: {subsystem_summary_data[index]}  "
                rows.append(subsys_summary_row + "\n\n")
            if not self.args.get('no_header'):
                rows.append(NVMeoFTopIO.ns_template.format(*NVMeoFTopIO.ns_headers))
            if ns_data:
                for ns in ns_data:
                    rows.append(NVMeoFTopIO.ns_template.format(*ns))
            else:
                rows.append("<no namespaces defined>\n")

            return ''.join(rows)

    @DBCLICommand.Read('nvmeof top cpu', poll=True)
    def nvmeof_top_cpu(_, server_address: str = '', server_port: Optional[int] = None,
                       gw_group: str = '',
                       descending: bool = False, sort_by: str = 'Thread Name',
                       with_timestamp: bool = False,
                       no_header: bool = False,
                       period: float = 1.0, session_id: Optional[str] = None):
        '''
        NVMeoF Top CPU Tool
        '''
        if sort_by not in NVMeoFTopCPU.reactors_headers:
            return HandleCommandResult(
                stderr=f"Invalid sort-by '{sort_by}': must match a header title: "
                       f"{NVMeoFTopCPU.reactors_headers}",
                retval=-errno.EINVAL
            )
        args = {
            'with_timestamp': with_timestamp,
            'no_header': no_header,
            'sort_descending': descending,
            'sort_by': sort_by,
            'server_address': server_address,
            'server_port': server_port,
            'gw_group': gw_group,
            'period': period,
            'session_id': session_id,
        }
        rc, output = NVMeoFTopCPU(args).run()
        if rc != 0:
            return HandleCommandResult(stderr=output, retval=rc)
        return HandleCommandResult(stdout=output, retval=rc)

    @DBCLICommand.Read('nvmeof top io', poll=True)
    def nvmeof_top_io(_, nqn: str = '',
                      server_address: str = '', server_port: Optional[int] = None,
                      gw_group: str = '',
                      descending: bool = False, sort_by: str = 'NSID',
                      with_timestamp: bool = False,
                      summary: bool = False, no_header: bool = False,
                      period: float = 1.0, session_id: Optional[str] = None):
        '''
        NVMeoF Top IO Tool
        '''
        if not nqn:
            return HandleCommandResult(
                stderr="Required argument '--nqn' missing",
                retval=-errno.EINVAL
            )
        if sort_by not in NVMeoFTopIO.ns_headers:
            return HandleCommandResult(
                stderr=f"Invalid sort-by '{sort_by}': must match a header title: "
                       f"{NVMeoFTopIO.ns_headers}",
                retval=-errno.EINVAL
            )
        args = {
            'nqn': nqn,
            'with_timestamp': with_timestamp,
            'summary': summary,
            'no_header': no_header,
            'sort_descending': descending,
            'sort_by': sort_by,
            'server_address': server_address,
            'server_port': server_port,
            'gw_group': gw_group,
            'period': period,
            'session_id': session_id,
        }
        rc, output = NVMeoFTopIO(args).run()
        if rc != 0:
            return HandleCommandResult(stderr=output, retval=rc)
        return HandleCommandResult(stdout=output, retval=rc)

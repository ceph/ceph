# -*- coding: utf-8 -*-
# This file is moved from the original work of "nvmeof-top" tool in:
# https://github.com/pcuzner/ceph-nvmeof-top 
# by Paul Cuzner <pcuzner@ibm.com>
import threading
import time
import logging
import grpc
import asyncio

from mgr_module import CLIReadCommand, HandleCommandResult

from .nvmeof_client import NVMeoFClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class NVMeoFTop:
    subsystem_summary_headers = ['Subsystem', 'Namespaces', 'Total IOPS', 'Throughput']
    summary_headers = ['Gateway', 'Total Subsystems', 'Total Namespaces']

    reactors_headers = ['Thread Name', 'Busy Rate%', 'Idle Rate%']
    reactors_template = "{:<40}   {:>30}   {:>30}\n"

    ns_headers = ['NSID', 'RBD pool/image', 'IOPS', 'r/s', 'rMB/s', 'r_await', 'rareq-sz', 'w/s', 'wMB/s', 'w_await', 'wareq-sz', 'LBGrp', 'QoS']
    ns_template = "{:>4}   {:<40}   {:>7}   {:>6}   {:>6}   {:>7}   {:>8}   {:>6}   {:>6}   {:>7}   {:>8}   {:^5}   {:>3}\n"

    def __init__(self, args: dict, client: NVMeoFClient):
        self.client = client
        self.args = args
        self.delay = args.get('delay')
        self.subsystem_nqn = args.get('subsystem')
        self.collector: DataCollector
        self.sort_key = args.get('sort_by', 'NSID')
        self.reverse_sort = args.get('sort_descending')
        self.status_code = 0
        self.show_reactor = not args.get('no_reactor')

    def to_stdout(self):
        """Dump namespace performance stats to stdout"""
        logger.debug("writing stats to stdout")
        sort_pos = NVMeoFTop.ns_headers.index(self.sort_key)
        with self.collector.lock:
            ns_data = self.collector.get_sorted_namespaces(sort_pos=sort_pos)
            reactor_data = self.collector.get_reactor_data()
            subsystem_summary_data = self.collector.get_subsystem_summary_data()
            overall_summary_data = self.collector.get_overall_summary_data()

        rows = []
        if self.args.get('with_timestamp'):
            tstamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(self.collector.timestamp))
            rows.append(f"{tstamp}\n")
        if not self.args.get('no_summary'):
            summary_row = ""
            for index, header in enumerate(NVMeoFTop.summary_headers):
                summary_row += f"{header}: {overall_summary_data[index]}  "
            rows.append(summary_row + "\n")
            subsys_summary_row = ""
            for index, header in enumerate(NVMeoFTop.subsystem_summary_headers):
                subsys_summary_row += f"{header}: {subsystem_summary_data[index]}  "
            rows.append(subsys_summary_row + "\n\n")
        if not self.args.get('no_reactor') and reactor_data:
            if not self.args.get('no_headings'):
                rows.append(NVMeoFTop.reactors_template.format(*NVMeoFTop.reactors_headers))
            for reactor in reactor_data:
                rows.append(NVMeoFTop.reactors_template.format(*reactor))
            rows.append("\n")
        if not self.args.get('no_headings'):
            rows.append(NVMeoFTop.ns_template.format(*NVMeoFTop.ns_headers))
        if ns_data:
            for ns in ns_data:
                rows.append(NVMeoFTop.ns_template.format(*ns))
        else:
            rows.append("<no namespaces defined>\n")

        return ''.join(rows) 

    def get_batch(self) -> None:
        logger.info(f"Running nvmeof top tool for {self.args.get('subsystem')}")
        rt_stdout = ""
        try:
            if not self.collector.ready:
                self.status_code = self.collector.health.rc
                return self.collector.health.msg 

            rt_stdout += self.to_stdout()
        except KeyboardInterrupt:
            logger.info("nvmeof-top stopped by user")

        rt_stdout += "\n ---- "
        return rt_stdout
   
    def run(self) -> None:
        self.collector = DataCollector(self)
        logger.info(f"nvmeof-top running with a {self.collector.__class__.__name__} collector")

        self.collector.initialise()
        if not self.collector.ready:
            self.status_code = self.collector.health.rc
            return (self.status_code, f"nvmeof-top has encountered an error: {self.collector.health.msg}")

        t = threading.Thread(target=self.collector.run, daemon=True)
        t.start()

        assert self.args.get('subsystem')
        return (self.status_code, self.get_batch())


@CLIReadCommand('nvmeof top', poll=True)
def nvmeof_top(_, subsystem: str, delay: int = 3,
                server_addr: str = '', group: str = '',
                descending: bool = False, sort_by: str = 'NSID',
                with_timestamp: bool = False, no_headings: bool = False,
                no_summary: bool = False, no_reactor: bool = False):
    '''
    NVMeoF Top Tool
    --subsystem '<nqn>'
    --delay <seconds: int>
    --descending
    --sort-by '<header>'
    --with-timestamp 
    --no-headings
    '''
    args = {
        'subsystem': subsystem,
        'delay': delay,
        'with_timestamp': with_timestamp,
        'no_summary': no_summary,
        'no_reactor': no_reactor,
        'no_headings': no_headings,
        'sort_descending': descending,
        'sort_by': sort_by,
        'server_addr': server_addr,
        'group': group,

    }
    gateway_client = NVMeoFClient(gw_group=group, traddr=server_addr)
    args['server_addr'] = gateway_client.gateway_addr

    top_tool = NVMeoFTop(args, gateway_client) 
    rc, output = top_tool.run()
    return HandleCommandResult(stdout=output, retval=rc)


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
        return (self.current - self.last) / interval


class PerformanceStats:
    def __init__(self, bdev: str, delay: int):
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
        self.total_ops_rate = 0
        self.total_bytes_rate = 0
        self.rareq_sz = 0.0
        self.wareq_sz = 0.0
        self.r_await = 0.0
        self.w_await = 0.0

    def calculate(self, delay: int):
        self.read_ops_rate = self.read_ops.rate(delay)
        self.read_bytes_rate = self.read_bytes.rate(delay)
        self.read_secs_rate = self.read_secs.rate(delay)
        self.write_ops_rate = self.write_ops.rate(delay)
        self.write_bytes_rate = self.write_bytes.rate(delay)
        self.write_secs_rate = self.write_secs.rate(delay)

        self.total_ops_rate = self.read_ops_rate + self.write_ops_rate
        self.total_bytes_rate = self.read_bytes_rate + self.write_bytes_rate

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
        self.tick_rate = Counter()
        self.busy = Counter()
        self.idle = Counter()

        self.busy_secs = Counter()
        self.idle_secs = Counter()

        self.busy_rate = 0.0
        self.idle_rate = 0.0

    def calculate(self, delay: int):
        self.busy_rate = self.busy_secs.rate(delay)
        self.idle_rate = self.idle_secs.rate(delay)


class DataCollector:
    def __init__(self, parent):
        self.parent = parent
        self.client = self.parent.client
        self.subsystem_nqn = self.parent.subsystem_nqn
        self.namespaces = []
        self.subsystems = None
        self.reactor_stats = {}
        self.iostats = {}
        self.iostats_lock = threading.Lock()
        self.lock = threading.Lock()
        self.gw_info = None
        self.timestamp = time.time()
        self.health = Health()

    @property
    def nqn_list(self):
        return [subsys.nqn for subsys in self.subsystems.subsystems]

    @property
    def ready(self) -> bool:
        return self.health.rc == 0

    @property
    def total_namespaces_defined(self) -> int:
        return len(self.namespaces)

    @property
    def total_subsystems(self) -> int:
        return len(self.nqn_list)
    
    @property
    def total_namespaces_overall(self):
        total = 0
        for subsys in self.subsystems.subsystems:
            # if subsys.nqn == self.subsystem_nqn:
            total += subsys.namespace_count
        return total

    @property
    def total_iops(self):
        return int(sum([stats.total_ops_rate for _, stats in self.iostats.items()]))

    @property
    def total_bandwidth(self):
        return sum([stats.total_bytes_rate for _, stats in self.iostats.items()])

    @property
    def max_namespaces(self):
        for subsys in self.subsystems.subsystems:
            if subsys.nqn == self.subsystem_nqn:
                return subsys.max_namespaces
        logger.error("Request for max namespaces could not find a match against the NQN! Returning 0")
        return 0

    def log_connection(self):
        logger.info(f"Connected to {self.parent.args.get('server_addr')}")
        logger.info(f"Gateway has {self.total_subsystems} subsystems defined")

    def get_sorted_namespaces(self, sort_pos: int):
        ns_data = []
        for ns in self.namespaces:

            ns_info = f"{ns.rbd_pool_name}/{ns.rbd_image_name}"
            bdev_name = ns.bdev_name

            perf_stats = self.iostats[bdev_name]
            perf_stats.calculate(self.parent.delay)

            ns_data.append((
                ns.nsid,
                ns_info,
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

        ns_data.sort(key=lambda t: t[sort_pos], reverse=self.parent.reverse_sort)
        return ns_data
    
    def get_reactor_data(self): # TODO
        reactor_data = []
        for thread_name in self.reactor_stats:
            thread_stats = self.reactor_stats[thread_name]
            thread_stats.calculate(self.parent.delay)
            reactor_data.append((
                thread_name,
                thread_stats.busy_rate,
                thread_stats.idle_rate,
            ))
        return reactor_data

    def get_subsystem_summary_data(self):
        # summary_headers = ['Subsystem', 'Namespaces', 'Total IOPS', 'Throughput']
        return [
            self.subsystem_nqn, 
            f'{self.total_namespaces_defined} / {self.max_namespaces}', 
            self.total_iops, 
            f'{(self.total_bandwidth / 1024**2):>7.2f} MiB/s'
        ]

    def get_overall_summary_data(self):
        # summary_headers = ['Gateways', 'Total Subsystems', 'Total Namespaces']
        return [
            self.parent.args.get('server_addr'), # TODO
            self.total_subsystems,
            self.total_namespaces_overall,
        ]

    def qos_enabled(self, ns) -> str:
        if (ns.rw_ios_per_second or ns.rw_mbytes_per_second or ns.r_mbytes_per_second or ns.w_mbytes_per_second):
            return 'Yes'
        return 'No'
    
    def lb_group(self, grp_id: int):
        """Provide a meaningful default when load-balancing is not in use"""
        return "N/A" if grp_id == 0 else f"{grp_id}"

    def bytes_to_MB(self, bytes: int, si: int = 1024):
        """Simple conversion of bytes to with MiB or MB"""
        return (bytes / si) / si

    # grpc methods
    def call_grpc_api(self, method_name, request):
        logger.debug(f"calling gprc method {method_name}")
        try:
            func = getattr(self.client.stub, method_name)
            data = func(request)
        except grpc._channel._InactiveRpcError:
            self.health.rc = 8
            self.health.msg = f"RPC endpoint unavailable at {self.client.gateway_addr}"
            logger.error(f"gprc call to {method_name} failed: {self.health.msg}")
            return None

        self.health.msg = f"{method_name} success"
        logger.debug(f"call to {method_name} successful")
        return data

    def _get_ns_iostats(self, ns):
        logger.debug(f"fetching iostats for namespace {ns.nsid}")
        with self.iostats_lock:
            logger.debug('iostats lock acquired')
            if ns.bdev_name not in self.iostats:
                self.iostats[ns.bdev_name] = PerformanceStats(ns.bdev_name, self.parent.delay)
            logger.debug('calling namespace_get_io_stats')
            stats = self.call_grpc_api('namespace_get_io_stats',
                                       NVMeoFClient.pb2.namespace_get_io_stats_req(
                                           subsystem_nqn=self.subsystem_nqn,
                                           nsid=ns.nsid))
            logger.debug(stats)

            iostats = self.iostats[ns.bdev_name]
            iostats.read_ops.update(stats.num_read_ops)
            iostats.read_bytes.update(stats.bytes_read)
            iostats.read_secs.update((stats.read_latency_ticks / stats.tick_rate))
            iostats.write_ops.update(stats.num_write_ops)
            iostats.write_bytes.update(stats.bytes_written)
            iostats.write_secs.update((stats.write_latency_ticks / stats.tick_rate))

    def _get_namespaces(self):
        return self.call_grpc_api('list_namespaces', NVMeoFClient.pb2.list_namespaces_req(subsystem=self.subsystem_nqn))

    def _get_threads_stats(self):
        with self.iostats_lock:
            logger.debug('calling get_thread_stats')
            stats = self.call_grpc_api('get_thread_stats', NVMeoFClient.pb2.get_spdk_thread_stats_req())
            tick_rate = stats.tick_rate
            for thread in stats.threads:
                name = thread.name
                if name not in self.reactor_stats:
                    self.reactor_stats[name] = ReactorStats(name)
                reactor_data = self.reactor_stats[name]
                reactor_data.busy_secs.update(thread.busy / tick_rate)
                reactor_data.idle_secs.update(thread.idle / tick_rate)
                reactor_data.tick_rate.update(tick_rate)

    def _get_subsystems(self):
        return self.call_grpc_api('list_subsystems', NVMeoFClient.pb2.list_subsystems_req(subsystem_nqn=self.subsystem_nqn))

    def _get_all_subsystems(self):
        return self.call_grpc_api('list_subsystems', NVMeoFClient.pb2.list_subsystems_req())

    # collector methods
    def initialise(self):
        self.subsystems = self._get_all_subsystems()
        if self.subsystems.status > 0:
            logger.error(f"Call to list_subsystems failed, RC={self.subsystems.status}, MSG={self.subsystems.error_message}")
            self.health.rc = 8
            self.health.msg = "Unable to retrieve a list of subsystems"
            return

        if self.total_subsystems == 0:
            self.health.rc = 8
            self.health.msg = 'No subsystems found'
            return

        if self.subsystem_nqn:
            if self.subsystem_nqn not in self.nqn_list:
                logger.error("nqn provided is not present on the gateway")
                self.health.rc = 12
                self.health.msg = "Subsystem NQN provided not found"
                return

        self.log_connection()

    async def collect_data(self):
        namespace_info = self._get_namespaces()
        if not self.ready:
            return

        self.namespaces = namespace_info.namespaces
        logger.debug(f"Subsystem '{self.subsystem_nqn}' has {self.total_namespaces_defined} namespaces")

        tasks = []
        for ns in self.namespaces:
            t = asyncio.create_task(asyncio.to_thread(self._get_ns_iostats, ns))
            tasks.append(t)
        subsystem_task = asyncio.create_task(asyncio.to_thread(self._get_all_subsystems))
        tasks.extend([subsystem_task])
        if self.parent.show_reactor:
            r = asyncio.create_task(asyncio.to_thread(self._get_threads_stats))
            tasks.append(r)

        await asyncio.gather(*tasks)
        self.subsystems = subsystem_task.result()
        logger.debug("tasks completed")

    async def start(self):
        for i in range(2):
            start = time.time()
            await self.collect_data()
            logger.info(f"data collection took (round {i+1}): {(time.time() - start):3.3f} secs")

            if not self.ready:
                logger.error("Error encounted during data collection, terminating async loop")
                return
            self.timestamp = time.time()
            logger.debug(f"nqn_list is : {self.nqn_list}")
            if i == 0:
                await asyncio.sleep(self.parent.delay)  

    def run(self):
        if self.ready:
            with self.lock:
                asyncio.run(self.start())


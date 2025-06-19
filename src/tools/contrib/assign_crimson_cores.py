#!env python3
import argparse
import doctest
import functools
import itertools
import logging
import json
import os
import subprocess
import re
import sys

root_logger = logging.getLogger(__name__)


def get_taskset_output(fname):
    """
    Returns contents of fname, or output of taskset -acp
    if fname is None
    """
    if fname:
        with open(fname, 'r') as f:
            return f.read()
    else:
        return str(
            subprocess.check_output(
                ['taskset', '-acp', str(os.getpid())]
            ), 'utf-8')


def taskset_output_to_allowed_cpus(tsout):
    """
    Parses taskset output like

    pid 3560663's current affinity list: 0-63\n

    into {0,1,...,63}

    >>> sorted(taskset_output_to_allowed_cpus("pid 3560663's current affinity list: 0-3"))
    [0, 1, 2, 3]
    >>> sorted(taskset_output_to_allowed_cpus("pid 3560663's current affinity list: 3"))
    [3]
    >>> sorted(taskset_output_to_allowed_cpus("pid 3560663's current affinity list: 0-5,8"))
    [0, 1, 2, 3, 4, 5, 8]
    >>> sorted(taskset_output_to_allowed_cpus("pid 3560663's current affinity list: 0-5,8,10-13"))
    [0, 1, 2, 3, 4, 5, 8, 10, 11, 12, 13]
    """
    tsout = re.sub("pid.*list: ", "", tsout)
    tsout.strip()

    # at this point, tsout should be just the comma delimited ranges
    ranges = tsout.split(',')

    def parse_range(r):
        if '-' in r:
            bounds = r.split('-')
            if len(bounds) != 2:
                raise Exception("Invalid taskset string")
            return set(range(int(bounds[0]), int(bounds[1]) + 1))
        else:
            return {int(r)}
    return functools.reduce(lambda x, y: x | y, map(parse_range, ranges), set())


def get_lscpu_json(fname):
    """
    Loads the contents of fname as json, or the output
    of lscpu -e --json if fname is None
    """
    if fname:
        with open(fname, 'r') as f:
            return json.load(f)
    else:
        return json.loads(
            subprocess.check_output(
                ['lscpu', '-e', '--json']
            ))


def lscpu_json_to_cpuinfo(lscpu_info, allowed_cpus):
    """
    Converts json output of lscpu -e --json to CPUInfo filtering
    for allowed_cpus
    """
    # [CPU(cpu, node, core)]
    cpus = [
        CPUInfo.CPU(int(cpu['cpu']), int(cpu['node']), int(cpu['core']))
        for cpu in lscpu_info['cpus']
    ]

    if allowed_cpus:
        cpus = list(filter(
            lambda x: x.cpu in allowed_cpus,
            cpus))
    return CPUInfo(cpus)


def to_range_str(s):
    """
    Returns comma delimited range representing s

    >>> to_range_str({100})
    '100'
    >>> to_range_str({1,3,5,7})
    '1,3,5,7'
    >>> to_range_str({1,2,3})
    '1-3'
    >>> to_range_str({1,4,5,6,7,10})
    '1,4-7,10'
    >>> to_range_str({1,3,5,7,8,9})
    '1,3,5,7-9'
    """
    ranges = []
    start = None
    last = None
    for i in sorted(s):
        if start is None:
            assert(last is None)
            start = i
            last = i
            continue
        if i == last + 1:
            last = i
            continue
        ranges.append((start, last))
        start = i
        last = i
    ranges.append((start, last))

    def to_range(item):
        start, last = item
        if start == last:
            return str(start)
        else:
            return f"{start}-{last}"
    return ",".join(map(to_range, ranges))


class CPUInfo:
    class CPU:
        def __init__(self, cpu: int, node: int, core : int):
            # cpu id
            self.cpu = cpu
            # numa node id
            self.node = node
            # physical core id
            self.core = core

    def __init__(self, cpus):
        self.__cpus = cpus

    def get_nodes(self):
        return sorted({x.node for x in self.__cpus})

    def get_cores_for_node(self, node):
        """
        Generates (core_id, [cpu_id]) for each core in the given node

        >>> test_cpuinfo_nodes_interleaved(2,4,2).get_cores_for_node(1)
        [(1, [1, 9]), (3, [3, 11]), (5, [5, 13]), (7, [7, 15])]
        >>> test_cpuinfo_nodes_separate(2,4,2).get_cores_for_node(1)
        [(4, [4, 12]), (5, [5, 13]), (6, [6, 14]), (7, [7, 15])]
        >>> test_cpuinfo_nodes_separate(1,4,2).get_cores_for_node(0)
        [(0, [0, 4]), (1, [1, 5]), (2, [2, 6]), (3, [3, 7])]
        """
        ret = {}
        for cpu in self.__cpus:
            if cpu.node != node:
                continue
            if cpu.core not in ret:
                ret[cpu.core] = []
            ret[cpu.core].append(cpu.cpu)
        return [(core, sorted(cpus)) for core, cpus in sorted(ret.items())]

    def get_cores_by_node(self):
        """
        Generates [(node, (core_id, [cpu_id]))] ordered by node, core_id

        >>> test_cpuinfo_nodes_separate(2,2,2).get_cores_by_node()
        [(0, [(0, [0, 4]), (1, [1, 5])]), (1, [(2, [2, 6]), (3, [3, 7])])]
        >>> test_cpuinfo_nodes_separate(1,4,1).get_cores_by_node()
        [(0, [(0, [0]), (1, [1]), (2, [2]), (3, [3])])]
        """
        return [(node, self.get_cores_for_node(node)) for node in self.get_nodes()]

    def get_cores_by_node_interleaved(self):
        """
        Generates (core_id, [cpu_id]) ordered by core_id, interleaved by
        node

        >>> list(itertools.islice(test_cpuinfo_nodes_separate(1,4,1).get_cores_by_node_interleaved(), 6))
        [(0, [0]), (1, [1]), (2, [2]), (3, [3])]
        >>> list(itertools.islice(test_cpuinfo_nodes_separate(2,8,2).get_cores_by_node_interleaved(), 4))
        [(0, [0, 16]), (8, [8, 24]), (1, [1, 17]), (9, [9, 25])]
        >>> list(itertools.islice(test_cpuinfo_nodes_interleaved(2,8,2).get_cores_by_node_interleaved(), 4))
        [(0, [0, 16]), (1, [1, 17]), (2, [2, 18]), (3, [3, 19])]
        """
        cores_by_node = self.get_cores_by_node()
        idx = 0
        while len(cores_by_node) > 0:
            node, cores = cores_by_node[idx % len(cores_by_node)]
            assert(len(cores) > 0)
            yield cores.pop(0)
            if len(cores) == 0:
                cores_by_node.pop(idx % len(cores_by_node))
            else:
                idx += 1


def test_cpuinfo_nodes_interleaved(num_nodes, cores_per_node, cpus_per_core):
    """
    Generates test CPUInfo such that successive cpus are on successive
    nodes.
    """
    num_cpus = num_nodes * cores_per_node * cpus_per_core
    num_cores = num_nodes * cores_per_node
    cpus = []
    for core_id in range(num_cores):
        node_id = core_id % num_nodes
        for cpu_id in range(core_id, num_cpus, num_cores):
            cpus.append(CPUInfo.CPU(cpu_id, node_id, core_id))
    assert(len(cpus) == (num_nodes * cores_per_node * cpus_per_core))
    return CPUInfo(cpus)


def test_cpuinfo_nodes_separate(num_nodes, cores_per_node, cpus_per_core):
    """
    Generates test CPUInfo such that cpu ids are grouped by node.
    """
    num_cpus = num_nodes * cores_per_node * cpus_per_core
    num_cores = num_nodes * cores_per_node
    cpus = []
    for core_id in range(num_cores):
        node_id = core_id // cores_per_node
        for cpu_id in range(core_id, num_cpus, num_cores):
            cpus.append(CPUInfo.CPU(cpu_id, node_id, core_id))
    assert(len(cpus) == num_cpus)
    return CPUInfo(cpus)


class AllocationParams:
    def __init__(self, num_cpus : int, physical_only : bool):
        self.num_cpus = num_cpus
        self.physical_only = physical_only


class Allocation:
    def __init__(self, cpus):
        self.cpus = cpus

    def __str__(self):
        return to_range_str(self.cpus)

    def __repr__(self):
        return f"allocation({self})"


def balance_by_process(
        cpu_info : CPUInfo,
        num_proc : int,
        params : list[AllocationParams]) -> list[list[Allocation]]:
    """
    Allocates cpus for each param to each process such that each process's
    cpus are within a single numa node.

    >>> balance_by_process(test_cpuinfo_nodes_interleaved(1,8,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]
    >>> balance_by_process(test_cpuinfo_nodes_interleaved(1,16,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(4-7)], [allocation(8-11)]]
    >>> balance_by_process(test_cpuinfo_nodes_interleaved(2,4,2), 3, [AllocationParams(4, False)])
    [[allocation(0,2,8,10)], [allocation(1,3,9,11)], [allocation(4,6,12,14)]]
    >>> balance_by_process(test_cpuinfo_nodes_interleaved(2,8,2), 3, [AllocationParams(4, True)])
    [[allocation(0,2,4,6)], [allocation(1,3,5,7)], [allocation(8,10,12,14)]]

    >>> balance_by_process(test_cpuinfo_nodes_separate(1,8,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]
    >>> balance_by_process(test_cpuinfo_nodes_separate(1,16,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(4-7)], [allocation(8-11)]]
    >>> balance_by_process(test_cpuinfo_nodes_separate(2,4,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(4-5,12-13)], [allocation(2-3,10-11)]]
    >>> balance_by_process(test_cpuinfo_nodes_separate(2,8,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(8-11)], [allocation(4-7)]]

    >>> balance_by_process(test_cpuinfo_nodes_interleaved(2,16,2), 3, [AllocationParams(4, False), AllocationParams(4, True)])
    [[allocation(0,2,32,34), allocation(4,6,8,10)], [allocation(1,3,33,35), allocation(5,7,9,11)], [allocation(12,14,44,46), allocation(16,18,20,22)]]
    """
    cores_by_node = cpu_info.get_cores_by_node()
    ret = [[] for i in range(num_proc)]
    idx = 0
    for proc in range(num_proc):
        node, cores = cores_by_node[idx % len(cores_by_node)]
        for param in params:
            cpus = set()
            while len(cpus) < param.num_cpus:
                if len(cores) == 0:
                    raise Exception("Unable to allocate by proc")
                core, core_cpus = cores.pop(0)
                if param.physical_only:
                    cpus.add(core_cpus[0])
                else:
                    cpus.update(core_cpus)
            ret[proc].append(Allocation(cpus))
        idx += 1
    return ret


def balance_by_socket(
        cpu_info : CPUInfo,
        num_proc : int,
        params : list[AllocationParams]) -> list[list[Allocation]]:
    """
    Allocates cpus for each param to each process such that each process's
    cpus are distributed across all sockets.

    >>> balance_by_socket(test_cpuinfo_nodes_interleaved(1,8,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]
    >>> balance_by_socket(test_cpuinfo_nodes_interleaved(1,16,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(4-7)], [allocation(8-11)]]
    >>> balance_by_socket(test_cpuinfo_nodes_interleaved(2,4,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]
    >>> balance_by_socket(test_cpuinfo_nodes_interleaved(2,8,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(4-7)], [allocation(8-11)]]

    >>> balance_by_socket(test_cpuinfo_nodes_separate(1,8,2), 3, [AllocationParams(4, False)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]
    >>> balance_by_socket(test_cpuinfo_nodes_separate(1,16,2), 3, [AllocationParams(4, True)])
    [[allocation(0-3)], [allocation(4-7)], [allocation(8-11)]]
    >>> balance_by_socket(test_cpuinfo_nodes_separate(2,4,2), 3, [AllocationParams(4, False)])
    [[allocation(0,4,8,12)], [allocation(1,5,9,13)], [allocation(2,6,10,14)]]
    >>> balance_by_socket(test_cpuinfo_nodes_separate(2,8,2), 3, [AllocationParams(4, True)])
    [[allocation(0-1,8-9)], [allocation(2-3,10-11)], [allocation(4-5,12-13)]]

    >>> balance_by_socket(test_cpuinfo_nodes_interleaved(2,16,2), 3, [AllocationParams(4, False), AllocationParams(4, True)])
    [[allocation(0-1,32-33), allocation(2-5)], [allocation(6-7,38-39), allocation(8-11)], [allocation(12-13,44-45), allocation(14-17)]]
    """
    cpu_stream = cpu_info.get_cores_by_node_interleaved()
    def get_cpu():
        try:
            return cpu_stream.__next__()
        except StopIteration:
            raise Exception("Unable to allocate by socket")

    ret = [[] for i in range(num_proc)]
    for proc in range(num_proc):
        for param in params:
            cpus = set()
            while (len(cpus) < param.num_cpus):
                core_id, core_cpus = get_cpu()
                if param.physical_only:
                    cpus.add(core_cpus[0])
                else:
                    cpus.update(core_cpus)
            ret[proc].append(Allocation(cpus))
    return ret


def get_balance_algorithm(arg : str):
    balance_algorithms = {
        'osd': balance_by_process,
        'socket': balance_by_socket
    }
    if arg not in balance_algorithms :
        algs = ", ". join(balance_algorithm_dict.keys())
        raise Exception(
            f"balance_algorithm must be one of {algs}"
        )
    return balance_algorithms[arg]


def main(argv):
    examples = """
    Examples:
    # Produce logical core mappings for use with crimson-osd
    %prog [-u <lscpu.json>] [-t <allowed_cpus>] [-b <osd|socket>] [-v]
          [-o <num_osd>] [-r <num_reactors_per_osd>]
          [--physical-only-seastar] [--physical-only-alienstore]

    Note that allocations may exceed requested cpus if logical siblings are
    included and request is not a multiple of logical cpus per core.
    """

    # skip argument parsing for tests
    if '-x' in argv:
        doctest.testmod()
        return

    logger = root_logger.getChild("main")

    parser = argparse.ArgumentParser(
        description="Creates cpu mappings appropriate for use with crimson-osd",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o",
        "--num_osd",
        type=int,
        required=True,
        help="Number of OSDs",
    )
    parser.add_argument(
        "-u",
        "--lscpu",
        type=str,
        required=False,
        help="Input file: .json file produced by lscpu -e --json",
        default=None,
    )
    parser.add_argument(
        "-t",
        "--taskset",
        type=str,
        required=False,
        help="Allowed CPUs (comma separated ranges format)",
        default=None,
    )
    parser.add_argument(
        "-r",
        "--num-reactors",
        type=int,
        required=True,
        help="Number of Seastar reactors per OSD"
    )
    parser.add_argument(
        "-p",
        "--physical-only-seastar",
        action="store_true",
        help="Only use one logical cpu per physical core for seastar reactors",
        default=False,
    )
    parser.add_argument(
        "-a",
        "--num-alienstore-cores",
        type=int,
        default=0,
        help="Number of alienstore cores per OSD"
    )
    parser.add_argument(
        "-k",
        "--physical-only-alienstore",
        action="store_true",
        help="Only use one logical cpu per physical core for alienstore",
        default=False,
    )
    parser.add_argument(
        "-b",
        "--balance-algorithm",
        type=str,
        required=True,
        help="CPU balance algorithm: osd, socket (NUMA)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="True to enable verbose logging mode",
        default=False,
    )

    options = parser.parse_args(argv)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    logging.basicConfig(level=logLevel, stream=sys.stderr)

    logger.debug(f"Got options: {options}")

    params = [AllocationParams(
        options.num_reactors,
        options.physical_only_seastar)]
    if options.num_alienstore_cores > 0:
        params.append(AllocationParams(
            options.num_alienstore_cores,
            options.physical_only_alienstore))
    cpuinfo = lscpu_json_to_cpuinfo(
        get_lscpu_json(options.lscpu),
        taskset_output_to_allowed_cpus(get_taskset_output(options.taskset))
    )
    osd_mappings = get_balance_algorithm(options.balance_algorithm)(
        cpuinfo, options.num_osd, params
    )
    for idx in range(len(params)):
        for mappings in osd_mappings:
            print(mappings[idx])


if __name__ == "__main__":
    main(sys.argv[1:])

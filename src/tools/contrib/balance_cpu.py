#!/usr/bin/python
"""
This script gets the output from lscpu and produces a list of CPU uids
corresponding to physical cores, intended to use to allocate Seastar reactors
in a balanced way across sockets.

Two strategies of balancing reactors over CPU cores:

1) OSD based: all the reactors of each OSD run in the same CPU NUMA socket (default),
2) Socket based: reactors for the same OSD are distributed evenly across CPU NUMA sockets.

Some auxiliaries:
- given a taskset cpu_set bitmask, identify those active physical CPU core ids and their
  HT siblings,
- for a gfiven OSD id, identify the corresponding CPU core ids to set.
- convert a (decimal) comma separated intervals into a cpu_set bitmask

Apply bitwise operator over each bytes variables:
result=bytes(map (lambda a,b: a ^ b, bytes_all_cpu, bytes_fio_cpu))

Given the list extracted from lscpu, apply the cpu_set bitmask from the taskset argument,
hence disabling some core ids. For each OSD, produce the corresponding bitmask.
"""

import argparse
import logging
import sys
import os
import re
import tempfile
from pprint import pformat
# from typing import Dict, List, Any

from lscpu import LsCpuJson

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


# Some generic bitwise functions to use from the taskset data
def get_bit(value, bit_index):
    """Get a power of 2 if the bit is on, 0 otherwise"""
    return value & (1 << bit_index)


def get_normalized_bit(value, bit_index):
    """Return 1/0 whenever the bit is on"""
    return (value >> bit_index) & 1


def set_bit(value, bit_index):
    """As it says on the tin"""
    return value | (1 << bit_index)


def clear_bit(value, bit_index):
    """As it says on the tin"""
    return value & ~(1 << bit_index)


# Generic functions to query whether a CPU id is enabled/available or not
def is_cpu_avail(bytes_mask, cpuid):
    """
    Return true if the cpuid is on
    CPU id 0 is at the last end of the bytes_mask, the max_cpu is at bit 0
    """
    try:
        return get_normalized_bit(bytes_mask[-1 - (cpuid // 8)], cpuid % 8)
    except IndexError:
        return False


def set_cpu(bytes_mask, cpuid):
    """Set cpuid on bytes_mask"""
    try:
        bytes_mask[-1 - (cpuid // 8)] = set_bit(bytes_mask[-1 - (cpuid // 8)], cpuid % 8)
    except IndexError:
        pass
    return bytes_mask


def get_range(bytes_mask, start, length):
    """
    Given a bytes_mask, return a new bytes_mask with the range of CPU ids
    starting from start and of length, skipping those that are not available
    """
    result = bytearray(b"\x00" * len(bytes_mask))
    max_cpu = 8 * len(bytes_mask)
    while length > 0 and start < max_cpu:
        if is_cpu_avail(bytes_mask, start):
            set_cpu(result, start)
            length -= 1
        start += 1

    return result


def set_range(bytes_mask, start, end):
    """
    Set a range of CPU ids in bytes_mask from start to end
    """
    ba = bytearray(bytes_mask)
    for i in range(start, end):
        set_cpu(ba, i)
    return ba


def set_all_ht_siblings(bytes_mask):
    """
    Set all the HT sibling of the enabled physical CPU ids specified in bytes_mask.

    Physical cores are in the range [-half_length_bytes_mask:]
    HT siblings are in the range [0:half_length_bytes_mask-1]

    Notes:
    result=bytes(map (lambda a,b: a | b, bytes_ht, bytes_phys))
    # result=bytes(map (lambda a,b: a | b, result[0:half_indx], bytes_mask[-half_indx:]))
    # partial = [a | b for a, b in zip(empty[0:half_indx], bytes_mask[-half_indx:])]
    # result = bytes( partial + bytes_mask[-half_indx:] )
    # result = bytearray(b"\x00" * len(bytes_mask))
    """
    result = bytearray(bytes_mask)
    half_indx = len(bytes_mask) // 2
    for i in range(0, half_indx):
        result[i] |= bytes_mask[half_indx + i]
    return result


def count_bits(bytes_mask: bytearray) -> int:
    """
    Using Python 3.9 way
    Python 3.10: i.bit_count()
    """
    count = 0
    for x in bytes_mask:
        count += bin(x).count("1")
    return count


def count_phys_cpus(bytes_mask: bytearray) -> int:
    """
    Count the number of physical CPU from the bitmask
    """
    count = 0
    half_indx = len(bytes_mask) // 2
    count = count_bits(bytes_mask[half_indx:])
    return count


def is_hexadecimal_str(s: str) -> bool:
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


# Defaults to declare, which are values that can be given as options for the script
NUM_OSD = 8
NUM_REACTORS = 3


class CpuCoreAllocator(object):
    """
    Process a sequence of CPU core ids to be used for the allocation of Seastar reactors

    # lscpu --json
    {
    "lscpu": [
      {
        d: { "field": "CPU(s):", "data": "112",}
        d: {'field': 'NUMA node(s):', 'data': '2'}
        d: {'field': 'NUMA node0 CPU(s):', 'data': '0-27,56-83'}
        d: {'field': 'NUMA node1 CPU(s):', 'data': '28-55,84-111'}
      }
      :
    }
    """

    def __init__(
        self,
        lscpu: str = "",
        num_osd: int = NUM_OSD,
        num_react: int = NUM_REACTORS,
        hex_or_range_str: str = "",
        out_hex: bool = False,
    ) -> None:
        """
        This class expects the output from lscpu --json, from there
        it works out a list of physical CPU uids to allocate Seastar reactors
        """
        self.num_osd = num_osd
        self.num_react = num_react
        self.out_hex = out_hex
        self.bytes_avail_cpus = bytes([])
        self.hex_or_range_str = hex_or_range_str
        self.lscpu = LsCpuJson(lscpu)
        assert self.lscpu, f"Invalid {lscpu}"
        # Output to produce: either hex bitmask CPU set or decimal ranges
        self.osds_cpu_out = {"dec_ranges": {}, "hex_cpu_mask": {}}

    def set_cpu_default(self) -> None:
        """
        From lscpu we set the max num CPUs value to indicate how many hex digits
        a valid taskset string should have
        """
        # Number of hex digits required for the cpu_set bitmask
        self.num_hex_digits = self.lscpu.get_num_logical_cpus() // 8
        # Default bitmask: all CPUs available
        self.ALL_CPUS = "ff" * self.num_hex_digits
        self.bytes_all_cpu = bytes.fromhex(self.ALL_CPUS)

    def parse_taskset_arg(self, cpu_range: str) -> str:
        """
        The taskset arg can be an hexstring describing a cpu_set bitmask, or can be
        decimal ranges comma separated. This method parses the second case.
        - split the ',' tuples (or singletons)
        extract the decimal values -- validate they are within max CPU uid. Produce a
        valid hexstring cpu_set bitmask.
        # result_ba = bytearray(b"\x00" * len(self.bytes_all_cpu))
        # cpu_list_int = list(range(start, end + 1))
        # cpu_list_int = [start]
        # cpu_set.update(set(cpu_list_int))
        # Convert the set to an hex string for a cpu_set bitmask
        # for item in cpu_set:
        #    set_cpu(result_ba, item)
        # Compare both approaches match:
        # logging.debug(f"result_ba:{result_ba}, bytes_mask:{bytes_mask}")
        """
        cpu_list_str = cpu_range.split(",")
        regex = re.compile(r"(\d+)([-](\d+))?")
        bytes_mask = bytearray(b"\x00" * len(self.bytes_all_cpu))
        for item in cpu_list_str:
            m = regex.search(item)
            if m:
                start = int(m.group(1))
                if m.group(2):
                    end = int(m.group(3))
                    bytes_mask = set_range(bytes_mask, start, end)
                else:
                    bytes_mask = set_cpu(bytes_mask, start)
        return bytes(bytes_mask).hex()

    def set_available_cpus(self):
        """
        Set the instance attribute self.bytes_avail_cpus
        If valid taskset hex_cpu_mask, use it, otherwise use all CPUs
        """
        # Validate the hex_string/bytes size for the cpuset bitmask
        if self.hex_cpu_mask:
            try:
                self.bytes_avail_cpus = bytes.fromhex(self.hex_cpu_mask)
            except ValueError:
                print(f"Ignoring invalid hex string, using default {self.ALL_CPUS}")
                logger.error(f"Invalid taskset arg: {self.hex_cpu_mask} ")
                self.bytes_avail_cpus = self.bytes_all_cpu
            assert self.num_hex_digits >= len(
                self.bytes_avail_cpus
            ), "Invalid taskset hexstring size"

    def validate_cpu_for_osd(self):
        """
        Validate whether there are enough CPU cores for the required OSD
        Count the number of bits from the physical section of the bytes_avail_cpus

        Note: we could use up to the maximum possible num of OSD instead of the number asked.
        """
        total_phys_cores = self.lscpu.get_total_physical()
        self.num_avail_phys_cores = count_phys_cpus(bytearray(self.bytes_avail_cpus))

        logger.debug(
            f"total_phys_cores: {total_phys_cores}, avail_phys_cores: {self.num_avail_phys_cores}"
        )
        assert (
            total_phys_cores >= self.num_avail_phys_cores
        ), "Invalid available physical CPU cores"
        max_osd_num = self.num_avail_phys_cores // self.num_react
        assert max_osd_num > self.num_osd, "Not enough physical CPU cores"


    def setup(self):
        """
        Preparation and validation of available CPU ids
        """
        self.lscpu.load_json()
        self.lscpu.get_ranges()
        self.set_cpu_default()
        if is_hexadecimal_str(self.hex_or_range_str):
            self.hex_cpu_mask = self.hex_or_range_str
        else:
            self.hex_cpu_mask = self.parse_taskset_arg(self.hex_or_range_str)
        self.set_available_cpus()
        logger.debug(f"self.bytes_avail_cpus: {self.bytes_avail_cpus}")
        self.validate_cpu_for_osd()


    def bitmask_to_range(self, bytes_mask) -> str:
        """
        Produce a list of decimal ranges from the bitmask cpuset
        """
        lista = []
        start = -1
        end = start
        i = 0
        # Do we need to check max_cpu < self.lscpu.get_num_logical_cpus():
        max_cpu = len(bytes_mask) * 8
        logger.debug(f"max_cpu : {max_cpu}")
        while i<max_cpu:
            flag = is_cpu_avail(bytes_mask, i)
            if flag:
                if start == -1:
                    start = i
                logger.debug(f"i: {i}, lista:{pformat(lista)}")
                while is_cpu_avail(bytes_mask,i) and i<max_cpu:
                    end = i
                    i += 1
                if start == end:
                    lista.append(f"{start}")
                else:
                    lista.append(f"{start}-{end}")
                start = -1
            else:
                logger.debug(f"not i: {i}, lista:{pformat(lista)}")
                while not is_cpu_avail(bytes_mask, i) and i<max_cpu:
                    i += 1
        return ",".join(lista)


    def set_osd_cpuset(self, osd, cpuset_ba:bytes) -> None:
        """
        Updates the internal attributes to trace the CPUs assigned to the OSD process
        """
        osd_cpu_s = bytes(cpuset_ba).hex()
        # Update the bitset mask:
        if osd in self.osds_cpu_out["hex_cpu_mask"]:
            self.osds_cpu_out["hex_cpu_mask"][osd] += f",{osd_cpu_s}"
        else:
            self.osds_cpu_out["hex_cpu_mask"].update({osd: osd_cpu_s})

        osd_cpu_str = self.bitmask_to_range(cpuset_ba)
        self.osds_cpu_out["dec_ranges"].update({osd: osd_cpu_str})
        logger.debug(f"self.osds_cpu_out: {pformat(self.osds_cpu_out)}")


    def do_distrib_socket_based(self):
        """
        Distribution criteria: the reactors of each OSD are distributed across the available
        NUMA sockets evenly.
        Each OSD uses step cores from each NUMA socket. Each socket is a pair of ranges (_start,_end)
        for physical and HT. On each allocation we update the physical_start, so the next iteration
        picks the CPU uid accordingly.
        Produces a bitmask cpuset and list of cpu id ranges to use for the ceph config set CLI.

        This method and next definitely can be refactored, possibly by defining a dictionary with callbacks
        for each stage where differ, both use the same general algorithm.
        """
        # Init: common to both strategies
        control = []
        num_sockets = self.lscpu.get_num_sockets()

        # Each OSD uses num reactor//sockets cores
        step = self.num_react // num_sockets
        reminder = self.num_react % num_sockets

        logger.debug(f"do_distrib_socket_based: step:{step}")

        # Copy the original physical ranges to the control dict
        for socket in self.lscpu.get_sockets():
            control.append(socket)

        # This byte array will be transformed for each OSD
        cpu_avail_ba = bytearray(self.bytes_avail_cpus)
        avail_s = bytes(cpu_avail_ba).hex()
        # This dict would hold a bitsetmask in hex per OSD
        osds_ba = {}
        # Traverse the OSD to produce an allocation
        for osd in range(self.num_osd):
            for socket in control:
                _start = socket["physical_start"]
                _step = step
                # If there is a reminder, use a round-robin technique so all
                # sockets are candidate for it
                _candidate = osd % num_sockets
                _so_id = socket["socket"]
                if _candidate == _so_id:
                    _step += reminder
                _end = socket["physical_start"] + _step
                # For cephadm, construct a dictionary for these intervals
                logger.debug(
                    f"osd: {osd}, socket:{_so_id}, _start:{_start}, _end:{_end - 1}"
                )
                # Verify this range is valid, otherwise shift as appropriate
                cpuset_ba = get_range(cpu_avail_ba, _start, _step)
                # Associate their HT siblings of this range
                cpuset_ba = set_all_ht_siblings(cpuset_ba)
                # Update the list of bitmask of this OSD
                if osd in osds_ba:
                    merged = bytes(map(lambda a, b: a | b, osds_ba[osd], cpuset_ba))
                    osds_ba[osd] = merged
                else:
                    osds_ba.update({osd: cpuset_ba})
                # Disable this OSD bitmaskset from the cpu_avail_ba
                cpu_avail_ba = bytes(map(lambda a, b: a & ~b, cpu_avail_ba, cpuset_ba))
                osd_cpu_s = bytes(cpuset_ba).hex()
                osd_cpu_s = bytes(osds_ba[osd]).hex()
                avail_s = bytes(cpu_avail_ba).hex()
                logger.debug(f"-- OSD: {osd}: {osd_cpu_s}, avail:{avail_s}")
                # Update the bitset mask
                self.set_osd_cpuset(osd, osds_ba[osd])

                if _end <= socket["physical_end"]:
                    socket["physical_start"] = _end
                    _ht_start = socket["ht_sibling_start"]
                    _ht_end = socket["ht_sibling_start"] + step
                    plist = list(
                        range(
                            _ht_start,
                            _ht_end,
                            1,
                        )
                    )
                    logger.debug(f"plist: {plist}")
                    socket["ht_sibling_start"] += _step
                else:
                    # bail out
                    _sops = socket["physical_start"] + step
                    logger.debug(f"out of range: {_sops}")
                    break
        # Set the reminder available CPU
        self.set_osd_cpuset('available', bytes(cpu_avail_ba) )
        self.osds_ba = osds_ba

    def do_distrib_osd_based(self):
        """
        Given a number of Seastar reactor threads and number of OSD,
        distributes all the reactors of the same OSD in the same NUMA socket
        using only physical core CPUs.
        Produces a list of ranges to use for the ceph config set CLI.
        """
        control = []
        # Each OSD uses num reactor cores from the same NUMA socket
        num_sockets = self.lscpu.get_num_sockets()
        step = self.num_react

        # Copy the original physical ranges to the control dict
        for socket in self.lscpu.get_sockets():
            control.append(socket)

        # This byte array will be transformed for each OSD
        cpu_avail_ba = bytearray(self.bytes_avail_cpus)
        avail_s = bytes(cpu_avail_ba).hex()
        logger.debug(f"cpu_avail_ba : {avail_s}")
        # This dict would hold a bitsetmask per OSD
        osds_ba = {}
        # Traverse the OSD to produce an allocation
        # even OSD num uses socket0, odd OSD number uses socket 1
        for osd in range(self.num_osd):
            #osds = []  # List of ranges as string
            _so_id = osd % num_sockets
            socket = control[_so_id]
            _start = socket["physical_start"]
            _end = socket["physical_start"] + step
            # For cephadm, construct a dictionary for these intervals
            logger.debug(
                f"osd: {osd}, socket:{_so_id}, _start:{_start}, _end:{_end - 1}"
            )
            # Verify this range is valid, skipping unavailable CPU ids as appropriate
            cpuset_ba = get_range(cpu_avail_ba, _start, step)
            # Associate their HT siblings of this range -- what if some of these are disabled?
            cpuset_ba = set_all_ht_siblings(cpuset_ba)
            # Update the list of bitmask of this OSD
            if osd in osds_ba:
                merged = bytes(map(lambda a, b: a | b, osds_ba[osd], cpuset_ba))
                osds_ba[osd] = merged
            else:
                osds_ba.update({osd: cpuset_ba})
            #osds.append(f"{_start}-{_end - 1}")
            # Disable this OSD bitmaskset from the cpu_avail_ba
            cpu_avail_ba = bytes(map(lambda a, b: a & ~b, cpu_avail_ba, cpuset_ba))
            osd_cpu_s = bytes(cpuset_ba).hex()
            avail_s = bytes(cpu_avail_ba).hex()
            logger.debug(f"-- OSD: {osd}: {osd_cpu_s}, avail:{avail_s}")
            # Update the bitset mask
            self.set_osd_cpuset(osd, osds_ba[osd])

            if _end <= socket["physical_end"]:
                socket["physical_start"] = _end
                _ht_start = socket["ht_sibling_start"]
                _ht_end = socket["ht_sibling_start"] + step
                plist = list(
                    range(
                        _ht_start,
                        _ht_end,
                        1,
                    )
                )
                logger.debug(f"plist: {plist}")
                socket["ht_sibling_start"] += step
            else:
                # bail out
                _sops = socket["physical_start"] + step
                logger.debug(f"Out of range: {_sops}")
                break
        # Set the reminder available CPU
        self.set_osd_cpuset('available', bytes(cpu_avail_ba) )
        # Set the following to exercise the unit tests
        self.osds_ba = osds_ba

    def output_cpusets(self):
        """
        Generic print of the cpuset to use per OSD and the remaining list of CPU available
        to use for everything else, eg Alien threads

        # Convert a bytesarrys back to hex string
        # hex_string = "".join("%02x" % b for b in array_alpha)
        # print(bytes(bytes_array).hex())
        """
        # Output: either hex or decimal ranges
        if self.out_hex:
            for cpuset in self.osds_cpu_out["hex_cpu_mask"].values():
                # one line per OSD-- ensure its sorted, last one must be the available
                print(cpuset)
        else:
            for cpuset in self.osds_cpu_out["dec_ranges"].values():
                print(cpuset)
            #print(" ".join(map(str, self._to_disable)))

        logger.debug(f"osds_cpu_out: {pformat(self.osds_cpu_out)}")


    def run(self, distribute_strat):
        """
        Load the .json from lscpu, get the ranges of CPU cores per socket,
        produce the corresponding balance, print the balance as a list intended to be
        consumed by vstart.sh -- a dictionary will be used for cephadm.
        """
        self.setup()
        if distribute_strat == "socket":
            self.do_distrib_socket_based()
        else:
            self.do_distrib_osd_based()

        self.output_cpusets()


def main(argv):
    examples = """
    Examples:
    # Produce a balanced CPU distribution of physical CPU cores intended for the Seastar
        reactor threads
        %prog [-u <lscpu.json>|-t <taskset_mask>] [-b <osd|socket>] [-d<dir>] [-v]
              [-o <num_OSDs>] [-r <num_reactors>]

    # such a list can be used for vstart.sh/cephadm to issue ceph conf set commands.
    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to produce CPU core balanced allocation""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-o",
        "--num_osd",
        type=int,
        required=False,
        help="Number of OSDs",
        default=NUM_OSD,
    )
    parser.add_argument(
        "-u",
        "--lscpu",
        type=str,
        help="Input file: .json file produced by lscpu --json",
        default=None,
    )
    parser.add_argument(
        "-t",
        "--taskset",
        type=str,
        help="The taskset argument of the parent process (eg. vstart)",
        default=None,
    )
    parser.add_argument(
        "-r",
        "--num_reactor",  # value of --crimson-smp
        type=int,
        required=False,
        help="Number of Seastar reactors",
        default=NUM_REACTORS,
    )
    parser.add_argument(
        "-d", "--directory", type=str, help="Directory to examine", default="./"
    )
    parser.add_argument(
        "-b",
        "--balance",
        type=str,
        required=False,
        help="CPU balance strategy: osd (default), socket (NUMA)",
        default=False,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="True to enable verbose logging mode",
        default=False,
    )
    parser.add_argument(
        "-x",
        "--hexcpuset",
        action="store_true",
        help="True to enable hexadecimal cpuset bitmask output",
        default=False,
    )

    # parser.set_defaults(numosd=1)
    options = parser.parse_args(argv)

    if options.verbose:
        logLevel = logging.DEBUG
    else:
        logLevel = logging.INFO

    with tempfile.NamedTemporaryFile(dir="/tmp", delete=False) as tmpfile:
        logging.basicConfig(filename=tmpfile.name, encoding="utf-8", level=logLevel)

    logger.debug(f"Got options: {options}")
    os.chdir(options.directory)

    cpu_cores = CpuCoreAllocator(
        options.lscpu,
        options.num_osd,
        options.num_reactor,
        options.taskset,
        options.hexcpuset,
    )
    cpu_cores.run(options.balance)


if __name__ == "__main__":
    main(sys.argv[1:])

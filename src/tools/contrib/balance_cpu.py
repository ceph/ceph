#!/usr/bin/python
"""
This script gets the output from lscpu and produces a list of CPU uids
corresponding to physical cores, intended to use to allocate Seastar reactors
in a balanced way across sockets.

Two strategies of balancing reactors over CPU cores:

1) OSD based: all the reactors of each OSD run in the same CPU NUMA socket (default),
2) Socket based: reactors for the same OSD are distributed evenly across CPU NUMA sockets.
"""

import argparse
import logging
import sys
import tempfile
from lscpu import LsCpuJson

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)

# Defaults
NUM_OSD = 8
NUM_REACTORS = 3


class CpuCoreAllocator(object):
    """
    Process a sequence of CPU core ids to be used for the allocation of Seastar reactors

    # lscpu --json
    {
    "lscpu": [
      {
        d: {'field': 'NUMA node(s):', 'data': '2'}
        d: {'field': 'NUMA node0 CPU(s):', 'data': '0-27,56-83'}
        d: {'field': 'NUMA node1 CPU(s):', 'data': '28-55,84-111'}
      }
      :
    }
    """

    def __init__(self, json_file: str, num_osd: int, num_react: int):
        """
        This class expects the output from lscpu --json, from there
        it works out a list of physical CPU uids to allocate Seastar reactors
        """
        self.json_file = json_file
        self.num_osd = num_osd
        self.num_react = num_react
        self._dict = {}
        self.lscpu = LsCpuJson(json_file)
        # self.socket_lst = LsCpuJson(json_file)

    def do_distrib_socket_based(self):
        """
        Distribution criteria: the reactors of each OSD are distributed across the available
        NUMA sockets evenly.
        Each OSD uses step cores from each NUMA socket.
        Produces a list of ranges to use for the ceph config set CLI.
        """
        # Init:
        control = []
        cores_to_disable = set([])
        num_sockets = self.lscpu.get_num_sockets()
        # step = self.num_react
        total_phys_cores = self.lscpu.get_total_physical()
        # Max num of OSD that can be allocated
        max_osd_num = total_phys_cores // self.num_react

        # Each OSD uses num reactor//sockets cores
        step = self.num_react // num_sockets
        reminder = self.num_react % num_sockets

        logger.debug(
            f"total_phys_cores: {total_phys_cores}, max_osd_num: {max_osd_num}, step:{step}"
        )
        assert max_osd_num > self.num_osd, "Not enough physical CPU cores"

        # Copy the original physical ranges to the control dict
        for socket in self.lscpu.get_sockets():  # socket_lst["sockets"]:
            control.append(socket)
        # Traverse the OSD to produce an allocation
        #  f"total_phys_cores: {total_phys_cores}, max_osd_num: {max_osd_num}, step:{step}, rem:{reminder} "
        for osd in range(self.num_osd):
            osds = []
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
                osds.append(f"{_start}-{_end - 1}")

                if _end <= socket["physical_end"]:
                    socket["physical_start"] = _end
                    # Produce the HT sibling list to disable
                    # Consider to use sets to avoid dupes
                    plist = list(
                        range(
                            socket["ht_sibling_start"],
                            (socket["ht_sibling_start"] + _step),
                            1,
                        )
                    )
                    logger.debug(f"plist: {plist}")
                    pset = set(plist)
                    # _to_disable=pset.union(cores_to_disable)
                    cores_to_disable = pset.union(cores_to_disable)
                    logger.debug(f"cores_to_disable: {list(cores_to_disable)}")
                    socket["ht_sibling_start"] += _step
                else:
                    # bail out
                    _sops = socket["physical_start"] + step
                    logger.debug(f"out of range: {_sops}")
                    break
            print(",".join(osds))
        _to_disable = sorted(list(cores_to_disable))
        logger.debug(f"Cores to disable: {_to_disable}")
        print(" ".join(map(str, _to_disable)))

    def do_distrib_osd_based(self):
        """
        Given a number of Seastar reactor threads and number of OSD,
        distributes all the reactors of the same OSD in the same NUMA socket
        using only physical core CPUs.
        Produces a list of ranges to use for the ceph config set CLI.
        """
        control = []
        cores_to_disable = set([])
        # Each OSD uses num reactor cores from the same NUMA socket
        num_sockets = self.lscpu.get_num_sockets()
        step = self.num_react
        total_phys_cores = self.lscpu.get_total_physical()
        # Max num of OSD that can be allocated
        max_osd_num = total_phys_cores // self.num_react

        logger.debug(
            f"total_phys_cores: {total_phys_cores}, max_osd_num: {max_osd_num}, step:{step}"
        )
        assert max_osd_num > self.num_osd, "Not enough physical CPU cores"

        # Copy the original physical ranges to the control dict
        for socket in self.lscpu.get_sockets():
            control.append(socket)
        # Traverse the OSD to produce an allocation
        # even OSD num uses socket0, odd OSD number uses socket 1
        for osd in range(self.num_osd):
            _so_id = osd % num_sockets
            socket = control[_so_id]
            _start = socket["physical_start"]
            _end = socket["physical_start"] + step
            # For cephadm, construct a dictionary for these intervals
            logger.debug(
                f"osd: {osd}, socket:{_so_id}, _start:{_start}, _end:{_end - 1}"
            )
            print(f"{_start}-{_end - 1}")
            if _end <= socket["physical_end"]:
                socket["physical_start"] = _end
                # Produce the HT sibling list to disable
                # Consider to use sets to avoid dupes
                plist = list(
                    range(
                        socket["ht_sibling_start"],
                        (socket["ht_sibling_start"] + step),
                        1,
                    )
                )
                logger.debug(f"plist: {plist}")
                pset = set(plist)
                # _to_disable = pset.union(cores_to_disable)
                cores_to_disable = pset.union(cores_to_disable)
                logger.debug(f"cores_to_disable: {list(cores_to_disable)}")
                socket["ht_sibling_start"] += step
            else:
                # bail out
                _sops = socket["physical_start"] + step
                logger.debug(f"Out of range: {_sops}")
                break
        _to_disable = sorted(list(cores_to_disable))
        logger.debug(f"Cores to disable: {_to_disable}")
        print(" ".join(map(str, _to_disable)))

    def run(self, distribute_strat):
        """
        Load the .json from lscpu, get the ranges of CPU cores per socket,
        produce the corresponding balance, print the balance as a list intended to be
        consumed by vstart.sh -- a dictionary will be used for cephadm.
        """
        self.lscpu.load_json()
        self.lscpu.get_ranges()
        if distribute_strat == "socket":
            self.do_distrib_socket_based()
        else:
            self.do_distrib_osd_based()


def main(argv):
    examples = """
    Examples:
    # Produce a balanced CPU distribution of physical CPU cores intended for the Seastar
        reactor threads
        %prog -u <lscpu.json> [-b <osd|socket>] [-d<dir>] [-v]
                 [-o <num_OSDs>] [-r <num_reactors>]

    # such a list can be used for vstart.sh/cephadm to issue ceph conf set commands.
    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to produce CPU core balanced allocation""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "-u",
        "--lscpu",
        type=str,
        required=True,
        help="Input file: .json file produced by lscpu --json",
        default=None,
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
        default="osd",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="True to enable verbose logging mode",
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

    cpu_cores = CpuCoreAllocator(options.lscpu, options.num_osd, options.num_reactor)
    cpu_cores.run(options.balance)


if __name__ == "__main__":
    main(sys.argv[1:])

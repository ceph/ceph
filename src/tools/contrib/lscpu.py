#!/usr/bin/python
"""
This module gets the output from lscpu and produces a list of CPU uids
corresponding to physical cores.
"""

# import logging
import os
import re
import json
# import tempfile
# import pprint

__author__ = "Jose J Palacios-Perez"

# logger = logging.getLogger(__name__)


class LsCpuJson(object):
    """
    Process a sequence of CPU core ids

    # lscpu --json
    {
    "lscpu": [
      {
        d: { "field": "CPU(s):", "data": "112"}
        d: {"field": "Core(s) per socket:", "data": "28"}
        d: {'field': 'NUMA node(s):', 'data': '2'}
        d: {'field': 'NUMA node0 CPU(s):', 'data': '0-27,56-83'}
        d: {'field': 'NUMA node1 CPU(s):', 'data': '28-55,84-111'}
      }
      :
    }
    """

    def __init__(self, json_file: str):
        """
        This class expects the output from lscpu --json
        """
        self.json_file = json_file
        self._dict = {}
        self.socket_lst = {
            "num_sockets": 0,
            "num_logical_cpus": 0,
            "num_cores_per_socket": 0,
            # or more general, an array, index is the socket number
            "sockets": [],
        }

    def load_json(self):
        """
        Load the lscpu --json output
        """
        json_file = self.json_file
        with open(json_file, "r") as json_data:
            # check for empty file
            f_info = os.fstat(json_data.fileno())
            if f_info.st_size == 0:
                print(f"JSON input file {json_file} is empty")
                return  # Should assert
            self._dict = json.load(json_data)
            json_data.close()
        # logger.debug(f"_dict: {self._dict}")

    def get_num_sockets(self):
        """
        Accessor
        """
        return self.socket_lst["num_sockets"]

    def get_physical_start(self, sindex):
        """
        Accessor: cpu core id start physical
        """
        return self.socket_lst["sockets"][sindex]["physical_start"]

    def get_ht_start(self, sindex):
        """
        Accessor: cpu core id start ht-sibling
        """
        return self.socket_lst["sockets"][sindex]["ht_sibling_start"]

    def get_num_physical(self):
        """
        Accessor: num physical cpu core ids
        """
        return self.socket_lst["num_cores_per_socket"]

    def get_num_logical_cpus(self):
        """
        Accessor: num CPU ids
        """
        return self.socket_lst["num_logical_cpus"]

    def get_total_physical(self):
        """
        Accessor: sum of the physical cores for all sockets
        """
        return self.get_num_sockets() * self.get_num_physical()
        

    def get_socket(self, cpuid: int):
        """
        Accessor: given cpuid returns which socket number and
        whether is a physical (True) or ht-sibling (False)
        """
        for _i, s in enumerate(self.socket_lst["sockets"]):
            if s["physical_start"] <= cpuid and cpuid <= s["physical_end"]:
                return (_i, True)
            if s["ht_sibling_start"] <= cpuid and cpuid <= s["ht_sibling_end"]:
                return (_i, False)

    def get_ranges(self):
        """
        Parse the .json from lscpu
        (we might extend this to parse either version: normal or .json)
        """
        ncpu_re = re.compile(r"^CPU\(s\):$")
        numa_re = re.compile(r"NUMA node\(s\):")
        node_re = re.compile(r"NUMA node(\d+) CPU\(s\):")
        ranges_re = re.compile(r"(\d+)-(\d+),(\d+)-(\d+)")
        cores_re = re.compile(r"^Core\(s\) per socket:$")
        socket_lst = self.socket_lst
        for d in self._dict["lscpu"]:
            # logger.debug(f"d: {d}")
            m = numa_re.search(d["field"])
            if m:
                socket_lst["num_sockets"] = int(d["data"])
                continue
            m = node_re.search(d["field"])
            if m:
                socket = m.group(1)
                m = ranges_re.search(d["data"])
                if m:
                    drange = {
                        "socket": int(socket),
                        "physical_start": int(m.group(1)),
                        "physical_end": int(m.group(2)),
                        "ht_sibling_start": int(m.group(3)),
                        "ht_sibling_end": int(m.group(4)),
                    }
                    socket_lst["sockets"].append(drange)
                    continue
            m = ncpu_re.search(d["field"])
            if m:
                socket_lst["num_logical_cpus"] = int(d["data"])
                continue
            m = cores_re.search(d["field"]) 
            if m:
                socket_lst["num_cores_per_socket"] = int(d["data"])
        # logger.debug(f"result: {socket_lst}")
        assert self.socket_lst["num_sockets"] > 0, "Failed to parse lscpu"

    def get_sockets(self):
        """
        Return the socket_lst["sockets"]
        """
        return self.socket_lst["sockets"]

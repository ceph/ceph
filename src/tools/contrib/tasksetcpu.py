#!/usr/bin/python
"""
This script traverses the ouput from taskset and ps to produce a .JSON
to generate an ascii grid for visualisation.
"""

import argparse
import logging
import os
import sys
import re
import json
import tempfile
from typing import Dict, List, Any, Set
from pprint import pformat
from lscpu import LsCpuJson

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


def to_color(string: str, color: str) -> str:
    """
    Simple basic color ANSI/ASCII Coding
    """
    color_code = {
        "blue": "\033[34m",
        "yellow": "\033[33m",
        "green": "\033[32m",
        "red": "\033[31m",
    }
    return color_code[color] + str(string) + "\033[0m"


def ljust_color(text: str, padding: int, char=" ") -> str:
    """
    Find all matching ANSI sequences, then get the total length
    of all matches to serve as our offset when we add it to the padding value
    """
    pattern = r"(?:\x1B[@-_]|[\x80-\x9F])[0-?]*[ -/]*[@-~]"

    matches = re.findall(pattern, text)
    offset = sum(len(match) for match in matches)
    return text.ljust(padding + offset, char[0])


def serialize_sets(obj) -> Any:
    """
    Serialise sets
    """
    if isinstance(obj, set):
        return list(obj)

    return obj


THREAD_TYPES = {
    # log is a valid thread name for both reactor and aliens
    "reactor": {
        "regex": re.compile(r"(crimson|reactor|syscall|log).*"),
        "color": "red",
        "name": "R",
    },
    "alien": {
        "regex": re.compile(r"(alien-store-tp)"),
        "color": "green",
        "name": "A",
    },
    "bluestore": {
        "regex": re.compile(r"(bstore|rocksdb|cfin).*"),
        "color": "blue",
        "name": "B",
    },
}


class CpuCell(object):
    """
    Single cell representing the threads allocated to a CPU core.
    Each cell is a set of thread types (Reactor, Alien, Bluestore).
    The Reactor type should be mutually exclusive to the other types.
    The type indicates the color code that the thread set should be printed.
    """

    def __init__(self, cpuid=0):
        """
        Construct an empty CpuCell:
        Might need extending to
        { OSD: set(thread_types) }
        """
        self.osd_id = -1
        self.cpuid = cpuid
        self._set = set([])

    def update(self, cpuid, cpuset: Dict[str, Any], osd_id: str) -> None:
        """
        Update the contents of a CpuCell.
        """
        _tlist = []
        self.osd_id = osd_id
        self.cpuid = cpuid
        for thread_id in cpuset:
            if thread_id not in THREAD_TYPES:
                logger.error(f"{thread_id} not in THREAD_TYPES")
            else:
                _tlist.append(thread_id)
        self._set.update(set(_tlist))
        logger.debug(f"--{self._set}--")

    def __str__(self) -> str:
        """
        Method to be called by str() on instances of this class.
        """
        _str = "".join([THREAD_TYPES[item]["name"] for item in self._set])
        return f"{self.osd_id}.{_str}"

    def __repr__(self) -> str:
        """
        Method to represent an instance of this class.
        Used implicitly by eg. pformat()
        """
        _str = "".join([THREAD_TYPES[item]["name"] for item in self._set])
        return f"{self.osd_id}.{_str}"
        # return f"{self.cpuid}.{_str}"

    def print(self, width=0) -> str:
        """
        Print the cell in color coded.
        """
        _str = "."
        _tlen = 1
        for _id in self._set:
            _name = THREAD_TYPES[_id]["name"]
            _color = THREAD_TYPES[_id]["color"]
            _item = f"{_name}{self.osd_id}"
            _tlen += len(_item)
            _str += to_color(
                _item,
                _color,
            )
        return ljust_color(_str, width)


class CpuGrid(object):
    """
    Grid for a Single CPU socket, basicaly a matrix of CpuCells.
    """

    NUM_CPUS = 112
    ROWS = 8
    COLS = 7
    # width       = len(str(max(rows,cols)+1))
    WIDTH = 6

    def __init__(
        self, id: int, socket: Dict[str, int], num_cpus: int = NUM_CPUS
    ) -> None:
        """
        This class expects a single CPU socket, which has two sections:
        physical of size num_cores, and a HT-sibling section with the same number.
        """
        self.id = id
        self.socket = socket
        self.ROWS = num_cpus // (self.COLS * 2)
        self.grid = [
            [CpuCell(self.COLS * row + col) for col in range(self.COLS)]
            for row in range(self.ROWS)
        ]
        self.str_lines: List[str] = []

    def get_cell_coord(self, cpuid: int, is_phys: bool):
        """
        Return the tuple row,col for this cpuid
        """
        if is_phys:
            row = (cpuid - self.socket["phy_start"]) // self.COLS
            col = (cpuid - self.socket["phy_start"]) % self.COLS
        else:
            row = (self.ROWS // 2) + (cpuid - self.socket["ht_start"]) // self.COLS
            col = (cpuid - self.socket["ht_start"]) % self.COLS
        return (row, col)

    def set_cell(
        self, cpuid: int, osd_id, cpuset: Dict[str, List[Any]], is_phys: bool
    ) -> None:
        """
        Fill the cell for cpuid with the values vstr
        """
        row, col = self.get_cell_coord(cpuid, is_phys)
        logger.debug(f"cpu{cpuid}: {row},{col}")
        try:
            self.grid[row][col].update(cpuid, cpuset, osd_id)
        except IndexError:
            logger.error(f"{is_phys}-index_out_of_range for {cpuid}: {row},{col}")

    def set_header(self):
        """
        Set this socket grid header.
        """
        header = " " * self.WIDTH + f" Socket {self.id} ".center(
            (self.WIDTH + 1) * self.COLS, "-"
        )
        self.str_lines = [header]

    def make_rows(self, is_phys):
        """
        Construct the rows body of the Grid.
        Can be the physical section or HT section.
        """
        width = self.WIDTH
        cols = self.COLS
        contentLine = "# | values |"

        if is_phys:
            _startp = self.socket["phy_start"]
            _row = 0
            _endp = self.ROWS // 2  # 4
        else:
            _startp = self.socket["ht_start"]
            _row = self.ROWS // 2  # 4
            _endp = self.ROWS  # 8

        grid_slice = self.grid[_row:_endp]
        for i, row in enumerate(grid_slice):
            values = "+".join(f"{v.print(width)}" for v in row)
            line = contentLine.replace("values", values)
            line = line.replace("#", f"{_startp + cols*i:>{width}d}")
            # This separates the Physical from the HT section
            self.str_lines.append(line)

    def make_grid(self):
        """
        Construct the grid for this socket line by line.
        """
        width = self.WIDTH
        cols = self.COLS

        # build frame
        contentLine = "# | values |"

        dashes = "+".join("-" * width for _ in range(cols))
        frameLine = contentLine.replace("values", dashes)
        frameLine = frameLine.replace("#", " " * width)
        frameLine = frameLine.replace("| ", "+-").replace(" |", "-+")

        # x-axis numbers:
        numLine = contentLine.replace("|", " ")
        numLine = numLine.replace("#", " " * width)
        colNums = " ".join(f"{i:<{width}d}" for i in range(cols))
        numLine = numLine.replace("values", colNums)
        self.str_lines.append(numLine)

        self.str_lines.append(frameLine)
        self.make_rows(True)  # physical section
        self.str_lines.append(frameLine)
        self.make_rows(False)  # HT section
        self.str_lines.append(frameLine)

        # construct an iterator for this grid
        self.itlines = iter(self.str_lines)

    def get_num_lines(self):
        """
        Return the number of lines of the grid
        """
        return len(self.str_lines)

    def next(self):
        """
        Calls iterator to return next line
        """
        return next(self.itlines, "")

    def show_grid(self):
        """
        Debug show grid
        """
        logger.debug(f"Socket: {self.id} Grid: {pformat(self.grid)}")


class TasksetEntry(object):
    """
    Process a sequence of taskset_ps _thread.out files to
    produce a CpuGrid per socket and .JSON

    # Format from the _threads.out files:
    # 1368714 1368714 crimson-osd       0     pid 1368714's current affinity list: 0
    # 1368714 1368720 reactor-1         1     pid 1368720's current affinity list: 1
    """

    LINE_REGEX = re.compile(
        r"""
        ^\d+\s+ # PID
        \d+\s+     # TID
        ([^\s]+)\s+   # thread name
        (\d+)\s+   # CPU id
        pid\s+(\d+)[']s\s+current\s+affinity\s+list:\s+(\d+)$""",
        re.VERBOSE,
    )
    FILE_SUFFIX_LST = re.compile(r"_list$")

    def __init__(
        self, config, directory, num_cpu_client, lscpu: str = "", taskset=None
    ):
        """
        This class expects:
        -   either a list of result files to process into a grid (suffix _list)
            or a single _threads.out file
        -   the number of CPU intended for the clients (eg. FIO)
        -   the .json from lscpu.
        """
        self.config = config
        m = self.FILE_SUFFIX_LST.search(config)
        if m:
            self.mode = "list"
            self.jsonName = config.replace("_list", ".json")
        else:
            self.mode = "single"
            self.jsonName = config.replace(".out", ".json")

        self.directory = directory
        self.num_cpu_client = num_cpu_client
        self.osd_num = 0
        # This is a dict with keys OSD num and values dicts w/keys CPU uid, values list of thread id
        self.entries = {}
        self.sockets: List[CpuGrid] = []
        # We probably might not need this attribute
        self.taskset = taskset
        if lscpu:
            self.lscpu = LsCpuJson(lscpu)

    def traverse_dir(self):
        """
        Traverse the given list (.JSON) use .tex template to generate document
        """
        pass

    def find(self, name, path):
        """
        find a name file in path
        """
        for root, _, files in os.walk(path):
            if name in files:
                return os.path.join(root, name)

    def save_grid_json(self):
        """
        Save the grid into a .JSON
        Shall we use the same name as the config list replaced extension
        """
        if self.jsonName:
            # Ensure the struct is OSD.id: [array of CPU entries]
            # Sorts the CPU entries by numeric order:
            # int_docs_info = {int(k) : v for k, v in dc.items()}
            # sorted_dict = dict(sorted(int_docs_info.items()))
            with open(self.jsonName, "w", encoding="utf-8") as f:
                json.dump(
                    self.entries, f, indent=4, sort_keys=True, default=serialize_sets
                )
                f.close()

    def _get_tgroup(self, tname: str) -> str:
        """
        Get the THREAD_TYPES from the thread name.
        Return the thread name if not registered as part of Crimson OSD process.
        """
        for k in THREAD_TYPES:
            if THREAD_TYPES[k]["regex"].match(tname):
                return k

        return tname

    def _get_cpu_range(self, cpu_uid: str, cpu_range: str) -> Set:
        """
        Get the cpu id range provided by taskset (if exist)
        The first arg is the cpuid from ps field PSR
        Returns the corresponding list as a set.
        We might produce a bitmask for the corresponding range.
        """
        cpu_list = []
        regex = re.compile(r"(\d+)([,-](\d+))?")
        m = regex.search(cpu_range)
        if m:
            start = int(m.group(1))
            if m.group(2):
                end = int(m.group(3))
                cpu_list = list(range(start, end + 1))
            else:
                cpu_list = [start]
        cpu_set = set(cpu_list)
        cpu_set.update({int(cpu_uid)})
        return cpu_set

    def _parse_via_regex(self, line: str):
        """
        Bug in the REGEx, alternative working fine, left for reference if required in the future.
        """
        match = self.LINE_REGEX.search(line)
        if match:
            groups = match.groups()
            tname = self._get_tgroup(groups[0])
            cpuid = str(groups[1])
            return tname, cpuid

    def parse(self, fname: str) -> Dict:
        """
        Parses an individual _thread.out file.
        Returns a dict whose keys are cpuid, values are dicts
        with the threads names, process group association (Reactor, Alien, Bluestore)
        represented as a set.
        """
        entry: Dict[int, Dict[str, List[Any]]] = {}
        with open(fname, "r") as _data:
            f_info = os.fstat(_data.fileno())
            if f_info.st_size == 0:
                print(f"Input file {fname} is empty")
                logger.error(f"Input file {fname} is empty")
                return entry
            lines = _data.read().splitlines()
            _data.close()
            for line in lines:
                lista = line.split()
                tid = lista[1]
                tname = self._get_tgroup(lista[2])
                cpu_range = self._get_cpu_range(lista[3], lista[9])
                for cpuid in cpu_range:
                    if cpuid not in entry:
                        entry.update({cpuid: {tname: []}})
                    if tname not in entry[cpuid]:
                        entry[cpuid].update({tname: []})
                    entry[cpuid][tname].append(tid)

        return entry

    def merge_entries(self, new_entry: dict, osd_id):
        """
        Merges (via set union) with the new entry (eg. OSD num)
        keys of the new_entry are cpuid
        """
        for k in new_entry.keys():  # cpuid
            entry = self.entries[osd_id]
            if k not in entry:
                entry[k] = new_entry[k]
            else:
                entry[k].update(new_entry[k])

    def set_cpu_in_grid(self, cpuid: int, cpuset, osd_id):
        """
        Given a cpuid and its contents, set the corresponding CpuGrid
        for a given OSD.
        """
        sindex, is_phys = self.lscpu.get_socket(cpuid)
        grid = self.sockets[sindex]
        osd = self.get_osd_num(osd_id)
        logger.debug(f"set_cpu_in_grid:{sindex}, {is_phys}")
        grid.set_cell(cpuid, osd, cpuset, is_phys)

    def get_osd_num(self, osd_id):
        """
        Extract the OSD number from the OSD id.
        """
        num = 0  # default
        regex = re.compile(r"^osd_(\d+)$")
        m = regex.search(osd_id)
        if m:
            num = m.group(1)
        return num

    def get_osd_id(self, setup: str):
        """
        Extract the OSD number from the filename (which should follow the expected convention).
        """
        osd_id = "osd_0"  # default
        regex = re.compile(r"^(osd_\d+).*$")
        m = regex.search(setup)
        if m:
            osd_id = m.group(1)
        return osd_id

    def update_grid(self, setup: str, osd_id):
        """
        Update the sockets grid for the given setup that is indicated per OSD process.
        """
        print(f"== {setup} ==")
        logger.debug(f"== {setup} ==")

        entry = self.entries[osd_id]
        for cpuid, cpuset in entry.items():
            self.set_cpu_in_grid(int(cpuid), cpuset, osd_id)
        for _s in self.sockets:
            _s.show_grid()

    def show_grid(self):
        """
        Traverse the array of sockets to join them by row to produce
        each line.
        """
        for _s in self.sockets:
            _s.set_header()
            _s.make_grid()

        # join the grid lines per socket: this should be done in a more Pythonic way...
        for _ in range(self.sockets[0].get_num_lines()):
            line = " + ".join(_s.next() for _s in self.sockets)
            print(line)

    def traverse_files(self):
        """
        Traverses the _thread.out files given in the config
        """
        if self.mode == "single":
            out_files = [self.config]
        else:
            try:
                config_file = open(self.config, "r")
            except IOError as e:
                raise argparse.ArgumentTypeError(str(e))
            out_files = config_file.read().splitlines()
            print(out_files)
            config_file.close()

        self.osd_num = len(out_files)
        print(f"loading {len(out_files)} .out files ...")

        # The number of files should be the same as the number of OSDs
        for fname in out_files:
            # Extract the OSD id from fname
            cpuNodeDict = self.parse(fname)
            osd_id = self.get_osd_id(fname)
            if osd_id not in self.entries:
                self.entries[osd_id] = {}
            self.merge_entries(cpuNodeDict, osd_id)
            self.update_grid(fname, osd_id)

    def run(self):
        """
        Entry point: processes the input files, then produces the grid
        """
        self.lscpu.load_json()
        self.lscpu.get_ranges()
        for s in range(self.lscpu.get_num_sockets()):
            self.sockets.append(
                CpuGrid(
                    s,
                    {
                        "phy_start": self.lscpu.get_physical_start(s),
                        "ht_start": self.lscpu.get_ht_start(s),
                        "num_cores": self.lscpu.get_num_physical(),
                    },
                    # Use this to calculate the size of grid
                    self.lscpu.get_num_logical_cpus(),
                )
            )
        self.traverse_files()
        self.show_grid()
        self.save_grid_json()


def main(argv):
    examples = """
    Examples:
    # Produce a CPU distribution visualisation grid for a single file:
        %prog -c osd_0_crimson_1osd_16reactor_256at_8fio_lt_disable_ht_threads.out

    # Produce a CPU distribution visualisation grid for a _list _of files (located in /tmp) and the output
        from ps and taskset:
        %prog -v -c crimson_1osd_16reactor_lt_disable_list -d /tmp -u numa_nodes.json 

    """
    parser = argparse.ArgumentParser(
        description="""This tool is used to parse output from the combined taskset and ps commands""",
        epilog=examples,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "-c",
        "--config",
        type=str,
        required=True,
        help="Input file: either containing a _list_ of _threads.out files, or a single .out file",
        default=None,
    )
    # Load the NUMA CPU summary from lscpu --json
    cmd_grp = parser.add_mutually_exclusive_group()
    cmd_grp.add_argument(
        "-u",
        "--lscpu",
        type=str,
        help="Input file: .json file produced by lscpu --json",
        default=None,
    )
    cmd_grp.add_argument(
        "-t",
        "--taskset",
        type=str,
        help="The taskset argument of the parent process (eg. vstart)",
        default=None,
    )

    parser.add_argument(
        "-i",
        "--client",
        type=int,
        required=False,
        help="Number of CPU cores required for the FIO client",
        default=8,
    )

    parser.add_argument(
        "-d", "--directory", type=str, help="Directory to examine", default="./"
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

    os.chdir(options.directory)

    grid = TasksetEntry(
        options.config,
        options.directory,
        options.client,
        options.lscpu,
        options.taskset,
    )
    grid.run()


if __name__ == "__main__":
    main(sys.argv[1:])

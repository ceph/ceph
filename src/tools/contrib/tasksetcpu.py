#!/usr/bin/python
"""
This script traverses the ouput from taskset and ps to produce a .JSON
to generate an ascii grid for visualisation.
Returns the suggested CPu cores for the FIO client.
"""

import argparse
import logging
import os
import sys
import re
import json
import tempfile
from pprint import pformat

# import pprint
from lscpu import LsCpuJson

__author__ = "Jose J Palacios-Perez"

logger = logging.getLogger(__name__)


def to_color(string, color):
    """
    Simple basic color ascii coding
    """
    color_code = {
        "blue": "\033[34m",
        "yellow": "\033[33m",
        "green": "\033[32m",
        "red": "\033[31m",
    }
    return color_code[color] + str(string) + "\033[0m"


def serialize_sets(obj):
    """
    Serialise sets
    """
    if isinstance(obj, set):
        return list(obj)

    return obj


class CpuCell(object):
    """
    Single cell representing a CPU core
    Essentially a list of tuples, each tuple is a str (actually a letter) of the type of thread
    running in this Cpu core. The type indicates the color code that it should be printed.
    We only support three types: Reactors, Alien, and Bluestore threads.
    Probably need to refactor the proc_groups dict as a member here, or a class of its own (?)
    """

    def __init__(self, cpuid, atype):
        self.type = atype
        self.cpuid = cpuid


class CpuGrid(object):
    """
    Grid for a Single CPU socket
    Each cell is a CpuCell, which contains a set of threads initials (single letter as per the field 'name')
    and a color code.
    """

    ROWS = 8
    COLS = 7
    # width       = len(str(max(rows,cols)+1))
    WIDTH = 5

    def __init__(self, id, socket):
        """
        This class expects a single CPU socket, which has two sections:
        physical of size num_cores, and a HT-sibling section with the same number
        """
        self.id = id
        self.socket = socket
        # Or more generally, a list of tuples -- these should be CpuCell
        self.grid = [["."] * self.COLS for _ in range(self.ROWS)]
        self.str_lines = []

    def get_cell_coord(self, cpuid, is_phys):
        """
        Return the tuple row,col for this cpuid
        """
        if is_phys:
            row = (cpuid - self.socket["phy_start"]) // self.COLS
            col = (cpuid - self.socket["phy_start"]) % self.COLS
        else:
            row = (
                (self.socket["num_cores"] // self.COLS)  # should be 4
                + (cpuid - self.socket["ht_start"]) // self.COLS
            )
            col = (cpuid - self.socket["ht_start"]) % self.COLS
        return (row, col)

    def set_cell(self, cpuid, is_phys, vstr):
        """
        Fill the cell for cpuid with the values vstr
        """
        # vlen = len(vstr) // 10 # due to control chars
        row, col = self.get_cell_coord(cpuid, is_phys)
        self.grid[row][col] = " " + vstr + " "  # use a new object instead?
        # self.grid[row][col] = " " * (self.WIDTH - vlen) + vstr

    def set_header(self):
        """
        Set this socket grid header
        """
        header = " " * self.WIDTH + f" Socket {self.id} ".center(
            (self.WIDTH + 1) * self.COLS, "-"
        )
        self.str_lines = [header]

    def show_grid(self):
        """
        Debug show grid
        """
        logger.debug(f"Grid: {pformat(self.grid)}")
        # logger.debug(f"{pformat(self.str_lines)}")

    def make_rows(self, is_phys):
        """
        Construct the rows body of the Grid
        Can be the physical section or HT section
        """
        width = self.WIDTH
        cols = self.COLS
        contentLine = "# | values |"

        if is_phys:
            _startp = self.socket["phy_start"]
            _row = 0
            _endp = 4  # self.get_cell_coord(self.socket["num_cores"]+1,is_phys)
        else:
            _startp = self.socket["ht_start"]
            _row = 4  # self.get_cell_coord(self.socket["ht_start"],is_phys) # 4
            _endp = 8  # self.get_cell_coord( self.socket["ht_start"]+self.socket["num_cores"]+1,is_phys) # 8

        logger.debug(f"make_rows: {_row}, {_endp}")
        grid_slice = self.grid[_row:_endp]
        for i, row in enumerate(grid_slice):
            values = "+".join(f"{v}".center(width, " ") for v in row)
            line = contentLine.replace("values", values)
            line = line.replace("#", f"{_startp + cols*i:>{width}d}")
            # This separates the Physical from the HT section
            self.str_lines.append(line)

    def make_grid(self):
        """
        Construct the grid for this socket line by line
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


class TasksetEntry(object):
    """
    Process a sequence of taskset_ps _thread.out files to
    produce a CpuGrid per socket and .JSON
    """

    # Only for OSD/Crimson
    proc_groups = {
        # TODO: log are valid thread names for both reactor and aliens
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
    proc_groups_set = set()

    # Formmat from the _threads.out files:
    # 1368714 1368714 crimson-osd       0     pid 1368714's current affinity list: 0
    # 1368714 1368720 reactor-1         1     pid 1368720's current affinity list: 1
    LINE_REGEX = re.compile(
        r"""
        ^\d+\s+ # PID
        \d+\s+     # TID
        ([^\s]+)\s+   # thread name
        (\d+)\s+   # CPU id
        pid\s+(\d+)[']s\s+current\s+affinity\s+list:\s+(\d+)$""",
        re.VERBOSE,
    )  # |re.DEBUG)
    FILE_SUFFIX_LST = re.compile(r"_list$")  # ,(_list|.out)re.DEBUG)

    def __init__(self, config, directory, num_cpu_client, lscpu):
        """
        This class expects either:
        a list of result files to process into a grid (suffix _list)
        or a single _threads.out file
        the number of CPU intended for the clients (eg. FIO)
        the .json from lscpu
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
        # This should be an array of dicts, size of num OSD
        self.entries = []  # {}
        # From lscpu we can get the ranges
        # self.sockets = [ Cpugrid(_i, ) for _i in range(NUM_SOCKETS)] # array of CpuGrid
        self.sockets = []
        self.lscpu = LsCpuJson(lscpu)
        self.proc_groups_set.update(self.proc_groups.keys())

    def traverse_dir(self):
        """
        Traverse the given list (.JSON) use .tex template to generate document
        """
        pass

    def find(self, name, path):
        """
        find a name file in path
        """
        for root, dirs, files in os.walk(path):
            if name in files:
                return os.path.join(root, name)

    def _get_str(self, cpuset, osd_id):
        """
        Transform a cpu set into a string of chars to indicate
        the thread allocation
        """
        _result = ""
        logger.debug(f"Got cpuset: {cpuset} for {osd_id}:")
        for item in cpuset:
            logger.debug(f"Got {item}:")
            if item not in self.proc_groups:
                logger.error(f"{item} not in proc_groups")
                return _result
            _id = self.proc_groups[item]["name"]
            logger.debug(f"Got {_id}.{osd_id}")
            _result += to_color(
                f"{_id}.{osd_id}",
                self.proc_groups[item]["color"],
                # self.proc_groups[item]["name"], self.proc_groups[item]["color"]
            )
        return _result

    def save_grid_json(self):
        """
        Save the grid into a .JSON
        Shall we use the same name as the config list replaced extension
        """
        if self.jsonName:
            with open(self.jsonName, "w", encoding="utf-8") as f:
                json.dump(
                    self.entries, f, indent=4, sort_keys=True, default=serialize_sets
                )
                f.close()

    def _get_tgroup(self, tname: str):
        """
        Get the proc_groups from the thread name
        """
        for k in self.proc_groups:
            if self.proc_groups[k]["regex"].match(tname):
                return k

        logger.debug(f"{tname}: not registered in groups")
        return tname

    def _get_cpu_range(self, cpu_uid: str, cpu_range: str):
        """
        Get the cpu id range provided by taskset (if exist)
        The first arg is the cpuid from ps field PSR
        Returns the corresponding list as a set
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
        Bug in the REGEx, alternative working fine
        """
        logger.debug(f"Parsing: {line}")
        match = self.LINE_REGEX.search(line)
        if match:
            groups = match.groups()
            logger.debug(f"Got groups: {groups}")
            tname = self._get_tgroup(groups[0])
            cpuid = str(groups[1])
            return tname, cpuid

    def parse(self, fname: str):
        """
        Parses individual _thread.out file
        Returns a dict whose keys are cpuid, values are dicts
        with the threads names, process group association (Reactor, Alien, Bluestore)
        represented as a set (idempotent, we can later look at add info such as occurrences)
        """
        entry = {}
        with open(fname, "r") as _data:
            f_info = os.fstat(_data.fileno())
            if f_info.st_size == 0:
                print(f"input file {fname} is empty")
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

    def merge_entries(self, new_entry: dict, osd_id: int):
        """
        Merges (via set union) with the new entry (eg. OSD num)
        keys of the new_entry are cpuid
        """
        for k in new_entry.keys():
            entry = self.entries[osd_id]
            if k not in entry:
                entry[k] = new_entry[k]
            else:
                entry[k].update(new_entry[k])

    def set_cpu_in_grid(self, cpuid: int, cpuset, osd_id: int):
        """
        Given a cpuid and its contents, set the corresponding CpuGrid
        for given OSD
        """
        vstr = self._get_str(cpuset, osd_id)
        sindex, is_phys = self.lscpu.get_socket(cpuid)
        grid = self.sockets[sindex]
        grid.set_cell(cpuid, is_phys, vstr)

    def get_osd_id(self, setup: str):
        """
        Extract the OSD number from the filename (which should follow the expected convention)
        """
        osd_id = 0  # default
        regex = re.compile(r"^osd_(\d+).*$")
        m = regex.search(setup)
        if m:
            osd_id = int(m.group(1))
        return osd_id

    def update_grid(self, setup: str, osd_id: int):
        """
        Update the sockets grid for the given setup
        """
        print(f"== {setup} ==")  # OSD process

        for _s in self.sockets:
            _s.set_header()

        entry = self.entries[osd_id]
        for cpuid, cpuset in entry.items():
            self.set_cpu_in_grid(int(cpuid), cpuset, osd_id)

    def show_grid(self):
        """
        Traverse the array of sockets to join them by row to produce
        each line.
        """
        for _s in self.sockets:
            _s.make_grid()
            _s.show_grid()

        # join the grid lines per socket: this should be done in a more Pythonic way...
        for i in range(self.sockets[0].get_num_lines()):
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
        for entry in range(self.osd_num):
            self.entries.append({})

        print(f"loading {len(out_files)} .out files ...")

        # pp = pprint.PrettyPrinter(width=41, compact=True)
        # The number of files should be the same as the number of OSDs
        for fname in out_files:
            # Extract the OSD id from fname
            cpuNodeList = self.parse(fname)
            osd_id = self.get_osd_id(fname)
            # pp.pprint(cpuNodeList)# Ok
            # merged = {**self.entries, **cpuNodeList }
            # Show the grid for this fname
            self.merge_entries(cpuNodeList, osd_id)
            self.update_grid(fname, osd_id)
        # logger.debug(f"Got entries: {self.entries}:")

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
                        "num_cores": self.lscpu.get_num_physical(s),
                    },
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

    # Produce a CPU distribution visualisation grid for a _list _of files:
        %prog -c crimson_1osd_16reactor_lt_disable_list
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
    # load the NUMA summary -- or lscpu --json
    parser.add_argument(
        "-u",
        "--lscpu",
        type=str,
        required=True,
        help="Input file: .json file produced by lscpu --json",
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
        # print(f"logname: {tmpfile.name}")

    logger.debug(f"Got options: {options}")

    os.chdir(options.directory)
    grid = TasksetEntry(
        options.config, options.directory, options.client, options.lscpu
    )
    grid.run()


if __name__ == "__main__":
    main(sys.argv[1:])

# -*- mode:python -*-
# vim: ts=4 sw=4 smarttab expandtab

"""
Copyright (C) 2015 Red Hat

This is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public
License version 2, as published by the Free Software
Foundation.  See file COPYING.
"""

import sys
import json
import socket
import struct
import time
from collections import OrderedDict
from fcntl import ioctl
from fnmatch import fnmatch
from prettytable import PrettyTable, HEADER
from signal import signal, Signals, SIGWINCH
from termios import TIOCGWINSZ
from types import FrameType
from typing import Any, Callable, Dict, List, Optional, Sequence, TextIO, Tuple, Union

from ceph_argparse import parse_json_funcsigs, validate_command

COUNTER = 0x8
LONG_RUNNING_AVG = 0x4
READ_CHUNK_SIZE = 4096


def admin_socket(asok_path: str,
                 cmd: List[str],
                 format: Optional[str] = '') -> bytes:
    """
    Send a daemon (--admin-daemon) command 'cmd'.  asok_path is the
    path to the admin socket; cmd is a list of strings; format may be
    set to one of the formatted forms to get output in that form
    (daemon commands don't support 'plain' output).
    """

    def do_sockio(path: str, cmd_bytes: bytes) -> bytes:
        """ helper: do all the actual low-level stream I/O """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        try:
            sock.sendall(cmd_bytes + b'\0')
            len_str = sock.recv(4)
            if len(len_str) < 4:
                raise RuntimeError("no data returned from admin socket")
            l, = struct.unpack(">I", len_str)
            sock_ret = b''

            got = 0
            while got < l:
                # recv() receives signed int, i.e max 2GB
                # workaround by capping READ_CHUNK_SIZE per call.
                want = min(l - got, READ_CHUNK_SIZE)
                bit = sock.recv(want)
                sock_ret += bit
                got += len(bit)

        except Exception as sock_e:
            raise RuntimeError('exception: ' + str(sock_e))
        return sock_ret

    try:
        cmd_json = do_sockio(asok_path,
                             b'{"prefix": "get_command_descriptions"}')
    except Exception as e:
        raise RuntimeError('exception getting command descriptions: ' + str(e))

    sigdict = parse_json_funcsigs(cmd_json.decode('utf-8'), 'cli')
    valid_dict = validate_command(sigdict, cmd)
    if not valid_dict:
        raise RuntimeError('invalid command')

    if format:
        valid_dict['format'] = format

    try:
        ret = do_sockio(asok_path, json.dumps(valid_dict).encode('utf-8'))
    except Exception as e:
        raise RuntimeError('exception: ' + str(e))

    return ret


class Termsize(object):
    DEFAULT_SIZE = (25, 80)

    def __init__(self) -> None:
        self.rows, self.cols = self._gettermsize()
        self.changed = False

    def _gettermsize(self) -> Tuple[int, int]:
        try:
            fd = sys.stdin.fileno()
            sz = struct.pack('hhhh', 0, 0, 0, 0)
            rows, cols = struct.unpack('hhhh', ioctl(fd, TIOCGWINSZ, sz))[:2]
            return rows, cols
        except IOError:
            return self.DEFAULT_SIZE

    def update(self) -> None:
        rows, cols = self._gettermsize()
        if not self.changed:
            self.changed = (self.rows, self.cols) != (rows, cols)
        self.rows, self.cols = rows, cols

    def reset_changed(self) -> None:
        self.changed = False

    def __str__(self) -> str:
        return '%s(%dx%d, changed %s)' % (self.__class__,
                                          self.rows, self.cols, self.changed)

    def __repr__(self) -> str:
        return '%s(%d,%d,%s)' % (self.__class__,
                                 self.rows, self.cols, self.changed)


class DaemonWatcher(object):
    """
    Given a Ceph daemon's admin socket path, poll its performance counters
    and output a series of output lines showing the momentary values of
    counters of interest (those with the 'nick' property in Ceph's schema)
    """
    (
        BLACK,
        RED,
        GREEN,
        YELLOW,
        BLUE,
        MAGENTA,
        CYAN,
        GRAY
    ) = range(8)

    RESET_SEQ = "\033[0m"
    COLOR_SEQ = "\033[1;%dm"
    COLOR_DARK_SEQ = "\033[0;%dm"
    BOLD_SEQ = "\033[1m"
    UNDERLINE_SEQ = "\033[4m"

    def __init__(self,
                 asok: str,
                 statpats: Optional[Sequence[str]] = None,
                 min_prio: int = 0) -> None:
        self.asok_path = asok
        self._colored = False

        self._stats: Optional[Dict[str, dict]] = None
        self._schema = None
        self._statpats = statpats
        self._stats_that_fit: Dict[str, dict] = OrderedDict()
        self._min_prio = min_prio
        self.termsize = Termsize()

    def supports_color(self, ostr: TextIO) -> bool:
        """
        Returns True if the running system's terminal supports color, and False
        otherwise.
        """
        unsupported_platform = (sys.platform in ('win32', 'Pocket PC'))
        # isatty is not always implemented, #6223.
        is_a_tty = hasattr(ostr, 'isatty') and ostr.isatty()
        if unsupported_platform or not is_a_tty:
            return False
        return True

    def colorize(self,
                 msg: str,
                 color: int,
                 dark: bool = False) -> str:
        """
        Decorate `msg` with escape sequences to give the requested color
        """
        return (self.COLOR_DARK_SEQ if dark else self.COLOR_SEQ) % (30 + color) \
               + msg + self.RESET_SEQ

    def bold(self, msg: str) -> str:
        """
        Decorate `msg` with escape sequences to make it appear bold
        """
        return self.BOLD_SEQ + msg + self.RESET_SEQ

    def format_dimless(self, n: int, width: int) -> str:
        """
        Format a number without units, so as to fit into `width` characters, substituting
        an appropriate unit suffix.
        """
        units = [' ', 'k', 'M', 'G', 'T', 'P', 'E', 'Z']
        unit = 0
        while len("%s" % (int(n) // (1000**unit))) > width - 1:
            if unit >= len(units) - 1:
                break
            unit += 1

        if unit > 0:
            truncated_float = ("%f" % (n / (1000.0 ** unit)))[0:width - 1]
            if truncated_float[-1] == '.':
                truncated_float = " " + truncated_float[0:-1]
        else:
            truncated_float = "%{wid}d".format(wid=width-1) % n
        formatted = "%s%s" % (truncated_float, units[unit])

        if self._colored:
            if n == 0:
                color = self.BLACK, False
            else:
                color = self.YELLOW, False
            return self.bold(self.colorize(formatted[0:-1], color[0], color[1])) \
                + self.bold(self.colorize(formatted[-1], self.YELLOW, False))
        else:
            return formatted

    def col_width(self, nick: str) -> int:
        """
        Given the short name `nick` for a column, how many characters
        of width should the column be allocated?  Does not include spacing
        between columns.
        """
        return max(len(nick), 4)

    def get_stats_that_fit(self) -> Tuple[Dict[str, dict], bool]:
        '''
        Get a possibly-truncated list of stats to display based on
        current terminal width.  Allow breaking mid-section.
        '''
        current_fit: Dict[str, dict] = OrderedDict()
        if self.termsize.changed or not self._stats_that_fit:
            width = 0
            assert self._stats is not None
            for section_name, names in self._stats.items():
                for name, stat_data in names.items():
                    width += self.col_width(stat_data) + 1
                    if width > self.termsize.cols:
                        break
                    if section_name not in current_fit:
                        current_fit[section_name] = OrderedDict()
                    current_fit[section_name][name] = stat_data
                if width > self.termsize.cols:
                    break

        self.termsize.reset_changed()
        changed = bool(current_fit) and (current_fit != self._stats_that_fit)
        if changed:
            self._stats_that_fit = current_fit
        return self._stats_that_fit, changed

    def _print_headers(self, ostr: TextIO) -> None:
        """
        Print a header row to `ostr`
        """
        header = ""
        stats, _ = self.get_stats_that_fit()
        for section_name, names in stats.items():
            section_width = \
                sum([self.col_width(x) + 1 for x in names.values()]) - 1
            pad = max(section_width - len(section_name), 0)
            pad_prefix = pad // 2
            header += (pad_prefix * '-')
            header += (section_name[0:section_width])
            header += ((pad - pad_prefix) * '-')
            header += ' '
        header += "\n"
        ostr.write(self.colorize(header, self.BLUE, True))

        sub_header = ""
        for section_name, names in stats.items():
            for stat_name, stat_nick in names.items():
                sub_header += self.UNDERLINE_SEQ \
                              + self.colorize(
                                    stat_nick.ljust(self.col_width(stat_nick)),
                                    self.BLUE) \
                              + ' '
            sub_header = sub_header[0:-1] + self.colorize('|', self.BLUE)
        sub_header += "\n"
        ostr.write(sub_header)

    def _print_vals(self,
                    ostr: TextIO,
                    dump: Dict[str, Any],
                    last_dump: Dict[str, Any]) -> None:
        """
        Print a single row of values to `ostr`, based on deltas between `dump` and
        `last_dump`.
        """
        val_row = ""
        fit, changed = self.get_stats_that_fit()
        if changed:
            self._print_headers(ostr)
        for section_name, names in fit.items():
            for stat_name, stat_nick in names.items():
                assert self._schema is not None
                stat_type = self._schema[section_name][stat_name]['type']
                if bool(stat_type & COUNTER):
                    n = max(dump[section_name][stat_name] -
                            last_dump[section_name][stat_name], 0)
                elif bool(stat_type & LONG_RUNNING_AVG):
                    entries = dump[section_name][stat_name]['avgcount'] - \
                            last_dump[section_name][stat_name]['avgcount']
                    if entries:
                        n = (dump[section_name][stat_name]['sum'] -
                             last_dump[section_name][stat_name]['sum']) \
                            / float(entries)
                        n *= 1000.0  # Present in milliseconds
                    else:
                        n = 0
                else:
                    n = dump[section_name][stat_name]

                val_row += self.format_dimless(int(n),
                                               self.col_width(stat_nick))
                val_row += " "
            val_row = val_row[0:-1]
            val_row += self.colorize("|", self.BLUE)
        val_row = val_row[0:-len(self.colorize("|", self.BLUE))]
        ostr.write("{0}\n".format(val_row))

    def _should_include(self, sect: str, name: str, prio: int) -> bool:
        '''
        boolean: should we output this stat?

        1) If self._statpats exists and the name filename-glob-matches
           anything in the list, and prio is high enough, or
        2) If self._statpats doesn't exist and prio is high enough

        then yes.
        '''
        if self._statpats:
            sectname = '.'.join((sect, name))
            if not any([
                p for p in self._statpats
                if fnmatch(name, p) or fnmatch(sectname, p)
            ]):
                return False

        if self._min_prio is not None and prio is not None:
            return (prio >= self._min_prio)

        return True

    def _load_schema(self) -> None:
        """
        Populate our instance-local copy of the daemon's performance counter
        schema, and work out which stats we will display.
        """
        self._schema = json.loads(
            admin_socket(self.asok_path, ["perf", "schema"]).decode('utf-8'),
            object_pairs_hook=OrderedDict)

        # Build list of which stats we will display
        self._stats = OrderedDict()
        assert self._schema is not None
        for section_name, section_stats in self._schema.items():
            for name, schema_data in section_stats.items():
                prio = schema_data.get('priority', 0)
                if self._should_include(section_name, name, prio):
                    if section_name not in self._stats:
                        self._stats[section_name] = OrderedDict()
                    self._stats[section_name][name] = schema_data['nick']
        if not len(self._stats):
            raise RuntimeError("no stats selected by filters")

    def _handle_sigwinch(self,
                         signo: Union[int, Signals],
                         frame: Optional[FrameType]) -> None:
        self.termsize.update()

    def run(self,
            interval: int,
            count: Optional[int] = None,
            ostr: TextIO = sys.stdout) -> None:
        """
        Print output at regular intervals until interrupted.

        :param ostr: Stream to which to send output
        """

        self._load_schema()
        self._colored = self.supports_color(ostr)

        self._print_headers(ostr)

        last_dump = json.loads(admin_socket(self.asok_path, ["perf", "dump"]).decode('utf-8'))
        rows_since_header = 0

        try:
            signal(SIGWINCH, self._handle_sigwinch)
            while True:
                dump = json.loads(admin_socket(self.asok_path, ["perf", "dump"]).decode('utf-8'))
                if rows_since_header >= self.termsize.rows - 2:
                    self._print_headers(ostr)
                    rows_since_header = 0
                self._print_vals(ostr, dump, last_dump)
                if count is not None:
                    count -= 1
                    if count <= 0:
                        break
                rows_since_header += 1
                last_dump = dump

                # time.sleep() is interrupted by SIGWINCH; avoid that
                end = time.time() + interval
                while time.time() < end:
                    time.sleep(end - time.time())

        except KeyboardInterrupt:
            return

    def list(self, ostr: TextIO = sys.stdout) -> None:
        """
        Show all selected stats with section, full name, nick, and prio
        """
        table = PrettyTable(('section', 'name', 'nick', 'prio'))
        table.align['section'] = 'l'
        table.align['name'] = 'l'
        table.align['nick'] = 'l'
        table.align['prio'] = 'r'
        self._load_schema()
        assert self._stats is not None
        assert self._schema is not None
        for section_name, section_stats in self._stats.items():
            for name, nick in section_stats.items():
                prio = self._schema[section_name][name].get('priority') or 0
                table.add_row((section_name, name, nick, prio))
        ostr.write(table.get_string(hrules=HEADER) + '\n')

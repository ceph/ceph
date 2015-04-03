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
from collections import defaultdict

from ceph_argparse import parse_json_funcsigs, validate_command

COUNTER = 0x8
LONG_RUNNING_AVG = 0x4


def admin_socket(asok_path, cmd, format=''):
    """
    Send a daemon (--admin-daemon) command 'cmd'.  asok_path is the
    path to the admin socket; cmd is a list of strings; format may be
    set to one of the formatted forms to get output in that form
    (daemon commands don't support 'plain' output).
    """

    def do_sockio(path, cmd_bytes):
        """ helper: do all the actual low-level stream I/O """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        try:
            sock.sendall(cmd_bytes + '\0')
            len_str = sock.recv(4)
            if len(len_str) < 4:
                raise RuntimeError("no data returned from admin socket")
            l, = struct.unpack(">I", len_str)
            sock_ret = ''

            got = 0
            while got < l:
                bit = sock.recv(l - got)
                sock_ret += bit
                got += len(bit)

        except Exception as sock_e:
            raise RuntimeError('exception: ' + str(sock_e))
        return sock_ret

    try:
        cmd_json = do_sockio(asok_path,
                             json.dumps({"prefix": "get_command_descriptions"}))
    except Exception as e:
        raise RuntimeError('exception getting command descriptions: ' + str(e))

    if cmd == 'get_command_descriptions':
        return cmd_json

    sigdict = parse_json_funcsigs(cmd_json, 'cli')
    valid_dict = validate_command(sigdict, cmd)
    if not valid_dict:
        raise RuntimeError('invalid command')

    if format:
        valid_dict['format'] = format

    try:
        ret = do_sockio(asok_path, json.dumps(valid_dict))
    except Exception as e:
        raise RuntimeError('exception: ' + str(e))

    return ret


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

    def __init__(self, asok):
        self.asok_path = asok
        self._colored = False

        self._stats = None
        self._schema = None

    def supports_color(self, ostr):
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

    def colorize(self, msg, color, dark=False):
        """
        Decorate `msg` with escape sequences to give the requested color
        """
        return (self.COLOR_DARK_SEQ if dark else self.COLOR_SEQ) % (30 + color) \
               + msg + self.RESET_SEQ

    def bold(self, msg):
        """
        Decorate `msg` with escape sequences to make it appear bold
        """
        return self.BOLD_SEQ + msg + self.RESET_SEQ

    def format_dimless(self, n, width):
        """
        Format a number without units, so as to fit into `width` characters, substituting
        an appropriate unit suffix.
        """
        units = [' ', 'k', 'M', 'G', 'T', 'P']
        unit = 0
        while len("%s" % (int(n) / (1000**unit))) > width - 1:
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
                + self.bold(self.colorize(formatted[-1], self.BLACK, False))
        else:
            return formatted

    def col_width(self, nick):
        """
        Given the short name `nick` for a column, how many characters
        of width should the column be allocated?  Does not include spacing
        between columns.
        """
        return max(len(nick), 4)

    def _print_headers(self, ostr):
        """
        Print a header row to `ostr`
        """
        header = ""
        for section_name, names in self._stats.items():
            section_width = sum([self.col_width(x)+1 for x in names.values()]) - 1
            pad = max(section_width - len(section_name), 0)
            pad_prefix = pad / 2
            header += (pad_prefix * '-')
            header += (section_name[0:section_width])
            header += ((pad - pad_prefix) * '-')
            header += ' '
        header += "\n"
        ostr.write(self.colorize(header, self.BLUE, True))

        sub_header = ""
        for section_name, names in self._stats.items():
            for stat_name, stat_nick in names.items():
                sub_header += self.UNDERLINE_SEQ \
                              + self.colorize(
                                    stat_nick.ljust(self.col_width(stat_nick)),
                                    self.BLUE) \
                              + ' '
            sub_header = sub_header[0:-1] + self.colorize('|', self.BLUE)
        sub_header += "\n"
        ostr.write(sub_header)

    def _print_vals(self, ostr, dump, last_dump):
        """
        Print a single row of values to `ostr`, based on deltas between `dump` and
        `last_dump`.
        """
        val_row = ""
        for section_name, names in self._stats.items():
            for stat_name, stat_nick in names.items():
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

                val_row += self.format_dimless(n, self.col_width(stat_nick))
                val_row += " "
            val_row = val_row[0:-1]
            val_row += self.colorize("|", self.BLUE)
        val_row = val_row[0:-len(self.colorize("|", self.BLUE))]
        ostr.write("{0}\n".format(val_row))

    def _load_schema(self):
        """
        Populate our instance-local copy of the daemon's performance counter
        schema, and work out which stats we will display.
        """
        self._schema = json.loads(admin_socket(self.asok_path, ["perf", "schema"]))

        # Build list of which stats we will display, based on which
        # stats have a nickname
        self._stats = defaultdict(dict)
        for section_name, section_stats in self._schema.items():
            for name, schema_data in section_stats.items():
                if schema_data.get('nick'):
                    self._stats[section_name][name] = schema_data['nick']

    def run(self, interval, count=None, ostr=sys.stdout):
        """
        Print output at regular intervals until interrupted.

        :param ostr: Stream to which to send output
        """

        self._load_schema()
        self._colored = self.supports_color(ostr)

        self._print_headers(ostr)

        last_dump = json.loads(admin_socket(self.asok_path, ["perf", "dump"]))
        rows_since_header = 0
        term_height = 25

        try:
            while True:
                dump = json.loads(admin_socket(self.asok_path, ["perf", "dump"]))
                if rows_since_header > term_height - 2:
                    self._print_headers(ostr)
                    rows_since_header = 0
                self._print_vals(ostr, dump, last_dump)
                if count is not None:
                    count -= 1
                    if count <= 0:
                        break
                rows_since_header += 1
                last_dump = dump
                time.sleep(interval)
        except KeyboardInterrupt:
            return

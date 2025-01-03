# Copyright (C) 2018 Red Hat Inc.
#
# Authors: Sergio Lopez Pascual <slopezpa@redhat.com>
#          Brad Hubbard <bhubbard@redhat.com>
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Library Public License as published by
# the Free Software Foundation; either version 2, or (at your option)
# any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Library Public License for more details.
#

# By default ceph daemons and clients maintain a list of log_max_recent (default
# 10000) log entries at a high debug level. This script will attempt to dump out
# that log from a ceph::log::Log* passed to the ceph-dump-log function or, if no
# object is passed, default to the globally available 'g_ceph_context->_log'
# (thanks Kefu). This pointer may be obtained via the _log member of a
# CephContext object (i.e. *cct->_log) from any thread that contains such a
# CephContext. Normally, you will find a thread waiting in
# ceph::logging::Log::entry and the 'this' pointer from such a frame can also be
# passed to ceph-dump-log.

import gdb
from datetime import datetime

try:
    # Python 2 forward compatibility
    range = xrange
except NameError:
    pass

class CephDumpLog(gdb.Command):
    def __init__(self):
        super(CephDumpLog, self).__init__(
                'ceph-dump-log',
                gdb.COMMAND_DATA, gdb.COMPLETE_SYMBOL, False)

    def invoke(self, args, from_tty):
        arg_list = gdb.string_to_argv(args)
        if len(arg_list) < 1:
            log = gdb.parse_and_eval('g_ceph_context->_log')
        else:
            log = gdb.parse_and_eval(arg_list[0])

        luminous_mimic = None

        try:
            entry = log['m_recent']['m_head']
            size = log['m_recent']['m_len']
            luminous_mimic = True
        except gdb.error:
            entry = log['m_recent']['m_first']
            size = log['m_recent']['m_size']
            end = log['m_recent']['m_end']
            buff = log['m_recent']['m_buff']

        for i in range(size):
            if luminous_mimic:
                try: # early luminous
                    stamp = int(str(entry['m_stamp']['tv']['tv_sec']) + str(entry['m_stamp']['tv']['tv_nsec']))
                    logline = entry['m_streambuf']['m_buf']
                    strlen = int(entry['m_streambuf']['m_buf_len'])
                except gdb.error: # mimic
                    stamp = entry['m_stamp']['__d']['__r']['count']
                    pptr = entry['m_data']['m_pptr']
                    logline = entry['m_data']['m_buf']
                    strlen = int(pptr - logline)
            else:
                stamp = entry['m_stamp']['__d']['__r']['count']
                logline = entry['str']['m_holder']['m_start']
                strlen = int(entry['str']['m_holder']['m_size'])
            thread = entry['m_thread']
            prio = entry['m_prio']
            subsys = entry['m_subsys']
            dt = datetime.fromtimestamp(int(stamp) / 1e9) # Giving up some precision
            gdb.write(dt.strftime('%Y-%m-%d %H:%M:%S.%f '))
            gdb.write("thread: {0:#x} priority: {1} subsystem: {2} ".
                    format(int(thread), prio, subsys))
            gdb.write(logline.string("ascii", errors='ignore')[0:strlen])
            gdb.write("\n")
            if luminous_mimic:
                entry = entry['m_next'].dereference()
            else:
                entry = entry + 1
                if entry >= end:
                    entry = buff

CephDumpLog()

#!/usr/bin/env python3
# coding: utf-8
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2017 OVH
# Copyright (C) 2020 Marc Schöchlin <ms-github@256bit.org>
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License version 2, as published by the Free Software
# Foundation.  See file COPYING.
#

import json
import subprocess
import time
import os
import argparse
import glob
import sys
import textwrap
import datetime


def shorten(val):
    if isinstance(val, str):
        return val
    for u in ((3, ''), (6, 'k'), (9, 'M'), (12, 'G'), (15, 'T')):
        if val < 10**u[0]:
            return "{}{}".format(int(val / (10 ** (u[0]-3))), u[1])
    return val


def create_histogram(sockets, counter, last, seconds, batch):

    current_datasets = {}
    json_d = {}
    for socket in sockets:
       try:
           out = subprocess.check_output(
               "ceph --admin-daemon {} perf histogram dump".format(socket),
               shell=True)
           json_d = json.loads(out.decode('utf-8'))
       except Exception as e:
           return (last,
                   "Couldn't connect to admin socket, result: \n{}".format(e))
       current_datasets[socket] = json_d['osd'][counter]['values']


    axes = json_d['osd'][counter]['axes']
   
    if batch:
        content = "{} : Counter: {} for {}\n\n\n".format(
                datetime.datetime.now().isoformat(), counter,", ".join(sockets))
    else:
        content = "Counter: {} for {}\n(create statistics every {} seconds)\n\n".format(
                counter,", ".join(sockets),seconds)

    content += "{}:\n".format(axes[1]['name'])
    for r in axes[1]['ranges']:
        content += "{0: >4} ".format(
            shorten(r['min']) if 'min' in r else '')
    content += "\n"
    for r in axes[1]['ranges']:
        content += "{0: >4} ".format(
            shorten(r['max']) if 'max' in r else '')
    content += "\n"

    content += ("{0: >"+str(len(axes[1]['ranges'])*5+14)+"}:\n").format(
        axes[0]['name'])

    if batch:
        COL = ''
        ENDC = ''
    else:
        COL = '\033[91m'
        ENDC = '\033[0m'

    current = []

    # initalize with zeros
    for i in range(len(current_datasets[socket])):
       current.append([])
       for j in range(len(current_datasets[socket][i])):
              current[i].append(0)

    # combine data
    for socket, data in current_datasets.items():
       for i in range(len(data)):
           for j in range(len(data[i])):
              current[i][j] += data[i][j]

    for i in range(len(current)):
        for j in range(len(current[i])):
            try:
                diff = current[i][j] - last[i][j]
            except IndexError:
                diff = '-'

            if diff != "-" and diff != 0:
                content += "{0}{1: >4}{2} ".format(COL,shorten(diff),ENDC)
            else:
                content += "{0: >4} ".format(shorten(diff))

        r = axes[0]['ranges'][i]
        content += "{0: >6} : {1}\n".format(
            shorten(r['min']) if 'min' in r else '',
            shorten(r['max']) if 'max' in r else '')
    return (current, content)


def loop_print(sockets, counter, loop_seconds, batch):
    last = []

    try:
       while True:
           last, content = create_histogram(sockets, counter, last, loop_seconds, batch)
           if not batch:
               print(chr(27) + "[2J")
           print(content)
           time.sleep(loop_seconds)
    except KeyboardInterrupt:
       print("...interupted")
       sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='Continuously display ceph performance histogram for selected osd operations')
    parser.add_argument(
        '--asok',
        type=str,
        default=['/var/run/ceph/*.asok'],
        nargs='+',
        help='Path to asok file, you can use wildcards')
    parser.add_argument(
        '--counter',
        type=str,
	help=textwrap.dedent('''\
         Specify name of the counter to calculate statistics
         see "ceph --admin-daemon /var/run/ceph/<osd>.asok  perf histogram dump"
         '''),
        default='op_w_latency_in_bytes_histogram')
    parser.add_argument(
	'--batch',
	help='Disable colors and add timestamps',
	action='store_true',
    )
    parser.add_argument(
        '--loop_seconds',
        type=int,
	help='Cycle time in seconds for statistics generation',
        default=5)

    args = parser.parse_args()

    if not sys.stdout.isatty():
      print("Not running with a tty, automatically switching to batch mode")
      args.batch = True
    
    sockets = []
    for asok in args.asok: 
      sockets = glob.glob(asok) + sockets

    if len(sockets) == 0:
      print("no suitable socket at {}".format(args.asok))
      sys.exit(1)
 
    loop_print(sockets, args.counter, args.loop_seconds, args.batch)

if __name__ == '__main__':
    main()

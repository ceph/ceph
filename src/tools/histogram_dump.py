#!/usr/bin/env python
# coding: utf-8
#
# Ceph - scalable distributed file system
#
# Copyright (C) 2017 OVH
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


def shorten(val):
    if isinstance(val, str):
        return val
    for u in ((3, ''), (6, 'k'), (9, 'M'), (12, 'G'), (15, 'T')):
        if val < 10**u[0]:
            return "{}{}".format(int(val / (10 ** (u[0]-3))), u[1])
    return val


def print_histogram(asok, logger, counter, last):

    try:
        out = subprocess.check_output(
            "ceph --admin-daemon {} perf histogram dump".format(asok),
            shell=True)
        j = json.loads(out.decode('utf-8'))
    except Exception as e:
        return (last,
                "Couldn't connect to admin socket, result: \n{}".format(e))

    current = j['osd'][counter]['values']
    axes = j['osd'][counter]['axes']
    content = ""

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

    for i in range(len(current)):
        for j in range(len(current[i])):
            try:
                diff = current[i][j] - last[i][j]
            except IndexError:
                diff = '-'
            content += "{0: >4} ".format(shorten(diff))

        r = axes[0]['ranges'][i]
        content += "{0: >6} : {1}\n".format(
            shorten(r['min']) if 'min' in r else '',
            shorten(r['max']) if 'max' in r else '')
    return (current, content)


def loop_print(asok, logger, counter):
    last = []
    while True:

        last, content = print_histogram(asok, logger, counter, last)
        print("{}{}".format("\n"*100, content))
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(
        description='Continuously display ceph performance histogram')
    parser.add_argument(
        '--asok',
        type=str,
        default='/var/run/ceph/*.asok',
        help='Path to asok file, can use wildcards')
    parser.add_argument(
        '--logger',
        type=str,
        default='osd')
    parser.add_argument(
        '--counter',
        type=str,
        default='op_w_latency_in_bytes_histogram')
    args = parser.parse_args()

    loop_print(args.asok, args.logger, args.counter)


if __name__ == '__main__':
    main()

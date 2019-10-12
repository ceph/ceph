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
import argcomplete
import argparse
import sys

def shorten(val):
    if isinstance(val, str):
        return val
    for u in ((3, ''), (6, 'k'), (9, 'M'), (12, 'G'), (15, 'T')):
        if val < 10**u[0]:
            return "{}{}".format(int(val / (10 ** (u[0]-3))), u[1])
    return val


def print_histogram(histogram):

    current = histogram['values']
    axes = histogram['axes']
    content = ""

    content += "{}:\n".format(axes[1]['name'])
    for r in axes[1]['ranges']:
        if axes[1]['min'] != 0 or 'min' in r:
            content += "{0: >4} ".format(
                shorten(r['min']) if 'min' in r else '')
    content += "\n"
    for r in axes[1]['ranges']:
        if axes[1]['min'] != 0 or 'min' in r:
            content += "{0: >4} ".format(
                shorten(r['max']) if 'max' in r else '')
    content += "\n"

    content += ("{0: >"+str(len(axes[1]['ranges'])*5+14)+"}:\n").format(
        axes[0]['name'])

    for i in range(len(current)):
        #print('i={}'.format(i));
        if axes[0]['min'] != 0 or 'min' in axes[0]['ranges'][i]:
            for j in range(len(current[i])):
                #print('i={}, j={}'.format(i,j));
                if axes[1]['min'] != 0 or 'min' in axes[1]['ranges'][j]:
                    content += "{0: >4} ".format(shorten(current[i][j]))

            r = axes[0]['ranges'][i]
            content += "{0: >6} : {1}\n".format(
                shorten(r['min']) if 'min' in r else '',
                shorten(r['max']) if 'max' in r else '')
    print content
    #return (current, content)

def print_1D_histogram(histogram):

    current = histogram['values']
    axes = histogram['axes']
    content = ""

    content += "{}:\n".format(axes[0]['name'])
    for r in axes[0]['ranges']:
        if axes[0]['min'] != 0 or 'min' in r:
            content += "{0: >6} ".format(
                shorten(r['min']) if 'min' in r else '')
    content += "\n"
    for r in axes[0]['ranges']:
        if axes[0]['min'] != 0 or 'min' in r:
            content += "{0: >6} ".format(
                shorten(r['max']) if 'max' in r else '')
    content += "\n"

    for i in range(len(current)):
        sum = 0
        if axes[0]['min'] != 0 or 'min' in axes[0]['ranges'][i]:
            for j in range(len(current[i])):
                sum += current[i][j]

            content += "{0: >6} ".format(sum)

    content += "\n"
    print content

def print_histograms(perf_file, combine_sizes):

    try:
        j = json.load(perf_file)
    except Exception as e:
        return ("Couldn't read {}, result: \n{}".format(perf_file,e))

    histograms = j['histograms']
    for rbd_vol_name in histograms.keys():
        #print ('Considering: histograms.{}'.format(rbd_vol_name))
        rbd_vol = histograms[rbd_vol_name]
        for histogram_name in rbd_vol.keys():
            #print ('Considering: histograms.{}.{}'.format(rbd_vol_name, histogram_name))
            histogram = rbd_vol[histogram_name]
            if histogram['axes'] is None:
                #print ('histograms.{}.{} is not a histogram'.format(rbd_vol_name, histogram_name))
                continue
            if histogram['values'] is None:
                #print ('histograms.{}.{} is not a histogram'.format(rbd_vol_name, histogram_name))
                continue
            #print ('histograms.{}.{} is a histogram'.format(rbd_vol_name, histogram_name))
            print ('--- {} (RBD volume {}) ---'.format(histogram_name, rbd_vol_name))
            if combine_sizes:
                print_1D_histogram(histogram)
            else:
                print_histogram(histogram)
            print
            #print "Done"

def main():
    parser = argparse.ArgumentParser(
        description='Dump histograms from an RWL perf dump')
    parser.add_argument('--file', type=str,
                        help='JSON input file (or stdin if omitted)')
    parser.add_argument('--combine-sizes', dest='combine_sizes', action='store_true',
                        help='Sum buckets for all sizes (second axis) and show only the latency (first axis)')
    parser.add_argument('--no-combine-sizes', dest='combine_sizes', action='store_false',
                        help='Display 2-D histogram')
    parser.set_defaults(combine_sizes=True)
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    if args.file is not None:
        perf_file = open(args.file, 'r')
    else:
        perf_file = sys.stdin

    print_histograms(perf_file, args.combine_sizes)


if __name__ == '__main__':
    main()

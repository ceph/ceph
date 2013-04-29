#!/usr/bin/python

import json
import argparse
import logging
import time
import commands

def parse_args():
    parser = argparse.ArgumentParser(description='watch ceph perf')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose',
        )
    parser.add_argument(
        '-s', '--socket',
        help='path to admin socket file',
        )
    parser.add_argument(
        'vars',
        nargs='+',
        help='variable to watch, section.value',
        )
    parser.add_argument(
        '-t', '--time',
        action='store_true',
        default=False,
        help='include relative time column',
        )
    parser.add_argument(
        '--absolute-time',
        action='store_true',
        default=False,
        help='include absolute time column',
        )
    args = parser.parse_args()
    return args


def main():
    ctx = parse_args()
    log = logging.getLogger(__name__)
    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    log.info('ctx %s', ctx)

    # check schema
    (code, raw) = commands.getstatusoutput('./ceph --admin-daemon %s perf schema' % ctx.socket)
    schema = json.loads(raw)

    # normalize var list
    vars = []
    vartype = {}
    for v in ctx.vars:
        if v.find('.') < 0:
            if v in schema:
                for var in schema[v].iterkeys():
                    vv = '%s.%s' % (v, var)
                    vars.append(vv)
                    vartype[vv] = int(schema[v][var]['type'])

        else:
            (sec, var) = v.split('.')
            if sec in schema and var in schema[sec]:
                vars.append(v)
                vartype[v] = int(schema[sec][var]['type'])
            else:
                log.warning('%s not present in schema', v)

    log.info('vars are %s', vars)
    log.info('vartype is %s', vartype)

    varline = '#'
    if ctx.time or ctx.absolute_time:
        varline = varline + ' %8s' % 'time'
    for v in vars:
        varline = varline + (' %8s' % v)

    print_count = 0
    prev = None
    start = time.time()
    while True:
        if print_count % 10 == 0:
            print(varline)
        print_count += 1

        (code, raw) = commands.getstatusoutput('./ceph --admin-daemon %s perf dump' % ctx.socket)
        perfstats = json.loads(raw)
        if prev is None:
            prev = perfstats

        vline = ' '
        now = time.time()
        if ctx.absolute_time:
            vline = '%10d' % int(now)
        elif ctx.time:
            vline = '%10d' % int(now - start)

        for v in vars:
            (sec, var) = v.split('.')
            val = 0
            if sec in perfstats and var in perfstats[sec]:

                formatstr = '%' + str(max(len(v), 8))
                if vartype[v] & 1:
                    formatstr = formatstr + 'f'
                else:
                    formatstr = formatstr + 'd'

                val = 0
                if vartype[v] & 4:
                    den = (perfstats[sec][var]['avgcount'] - perfstats[sec][var]['avgcount'])
                    if den > 0:
                        val = (perfstats[sec][var]['sum'] - prev[sec][var]['sum']) / den
                elif vartype[v] & 8:
                    val = perfstats[sec][var] - prev[sec][var]
                else:
                    val = perfstats[sec][var]

                vline = vline + ' ' + (formatstr % val)
        print(vline)

        prev = perfstats
        time.sleep(1)


main()

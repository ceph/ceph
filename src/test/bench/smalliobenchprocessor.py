import json
import sys
from pylab import hist
import gzip


def get_next_line(line, output):
    val = json.loads(line)
    if val['type'] not in output:
        output[val['type']] = {}
    for (name, value) in val.iteritems():
        if name == "type":
            continue
        if name == "seq":
            continue
        if name not in output[val['type']]:
            output[val['type']][name] = []
        output[val['type']][name] += [float(value)]


def wrapgz(gfilename):
    def retval():
        gfile = gzip.open(gfilename, 'rb')
        gfile.__exit__ = lambda: gfile.close()
        return gfile
    return (gfilename, retval)


def read_all_input(filename):
    cur = {}
    openfn = open
    if ".gz" in filename:
        openfn = wrapgz
    with openfn(filename) as fh:
        for line in fh:
            get_next_line(line, cur)
    return cur


def write_committed_latency(out, bins, **kwargs):
    hist(out['write_committed']['latency'], bins, **kwargs)


def read_latency(out):
    hist(out['read']['latency'], 100)


def com(out):
    return out['write_committed']['latency']


def app(out):
    return out['write_applied']['latency']

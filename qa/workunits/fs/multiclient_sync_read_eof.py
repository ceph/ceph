#!/usr/bin/python

import argparse
import os
import sys
import time

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mnt1')
    parser.add_argument('mnt2')
    parser.add_argument('fn')
    args = parser.parse_args()

    file(os.path.join(args.mnt1, args.fn), 'w')
    f1 = file(os.path.join(args.mnt1, args.fn), 'r+')
    f2 = file(os.path.join(args.mnt2, args.fn), 'r+')

    f1.write('foo')
    f1.flush()
    a = f2.read(3)
    print 'got "%s"' % a
    assert a == 'foo'
    f2.write('bar')
    f2.flush()
    a = f1.read(3)
    print 'got "%s"' % a
    assert a == 'bar'

    ## test short reads
    f1.write('short')
    f1.flush()
    a = f2.read(100)
    print 'got "%s"' % a
    assert a == 'short'
    f2.write('longer')
    f2.flush()
    a = f1.read(1000)
    print 'got "%s"' % a
    assert a == 'longer'

    print 'ok'

main()

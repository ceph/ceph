#!/usr/bin/env python
#
# Copyright (C) 2015 <contact@redhat.com>
# Copyright (C) 2015 SUSE LINUX GmbH
#
# Author: Alfredo Deza <alfredo.deza@inktank.com>
# Author: Owen Synge <osynge@suse.com>
# Author: Loic Dachary <loic@dachary.org>
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
import argparse
import logging

import ceph_detect_init
from ceph_detect_init import exc


def parser():
    parser = argparse.ArgumentParser(
        'ceph-detect-init',
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--use-rhceph",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--default",
        default=None,
    )
    return parser


def run(argv=None, namespace=None):
    args = parser().parse_args(argv, namespace)

    if args.verbose:
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                            level=logging.DEBUG)
    try:
        print(ceph_detect_init.get(args.use_rhceph).init)
    except exc.UnsupportedPlatform:
        if args.default:
            print(args.default)
        else:
            raise

    return 0

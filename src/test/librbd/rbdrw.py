#!/usr/bin/env python
"""
Loop writing/reading the first 4k of image argv[1] in pool rbd,
after acquiring exclusive lock named argv[2].  When an exception
happens, split off the last number in the exception 'args' string
and use it as the process exit code, if it's convertible to a number.

Designed to run against a blacklist operation and verify the
ESHUTDOWN expected from the image operation.

Note: this cannot be run with writeback caching on, currently, as
writeback errors cause reads be marked dirty rather than error, and
even if they were marked as errored, ObjectCacher would retry them
rather than note them as errored.
"""

import rados, rbd, sys

with rados.Rados(conffile='') as r:
    with r.open_ioctx('rbd') as ioctx:
        try:
            with rbd.Image(ioctx, sys.argv[1]) as image:
                image.lock_exclusive(sys.argv[2])
                while True:
                    image.write('A' * 4096, 0)
                    r = image.read(0, 4096)
        except rbd.ConnectionShutdown:
            # it so happens that the errno here is 108, but
            # anything recognizable would do
            exit(108)

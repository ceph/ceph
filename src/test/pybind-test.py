#!/usr/bin/python

import rados

r = rados.Rados()
try:
    r.create_pool("foo2")
    print "created pool foo2"
except rados.ObjectExists:
    print "pool foo2 already exists"

print "opening pool foo2"
foo2_pool = r.open_pool("foo2")
print "deleting pool foo2"
foo2_pool.delete()

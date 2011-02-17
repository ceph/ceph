#!/usr/bin/python

import rados

# Create and destroy a pool
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

# create a pool and some objects
try:
    r.create_pool("foo3")
except rados.ObjectExists:
    pass
foo3_pool = r.open_pool("foo3")
foo3_pool.write("abc", "abc")
foo3_pool.write("def", "def")
for obj in foo3_pool.list_objects():
    print str(obj)

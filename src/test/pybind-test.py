#!/usr/bin/python

import rados

r = rados.Rados()
v = r.version()
print "rados version %s" % str(v)
try:
    r.create_pool("foo2")
    print "created pool foo2"
except rados.ObjectExists:
    print "pool foo2 already exists"

if r.pool_exists("foo2") != True:
    raise RuntimeError("we just created pool 'foo2', but it doesn't exist?")

print "opening pool foo2"
foo2_pool = r.open_pool("foo2")
# give this pool to the anonymous AUID
foo2_pool.change_auid(rados.ANONYMOUS_AUID)
# well, actually, we want it back.
foo2_pool.change_auid(rados.ADMIN_AUID)
# now delete
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
abc_str = foo3_pool.read("abc")
if (abc_str != "abc"):
    raise RuntimeError("error reading object abc: expected value abc, \
got %s" % abc_str)
# write_full replaces the whole 'def' object
foo3_pool.write_full("def", "d")
def_str = foo3_pool.read("def")
if (def_str != "d"):
    raise RuntimeError("error reading object def: expected value d, \
got %s" % def_str)

for obj in foo3_pool.list_objects():
    print str(obj)

# create some snapshots and do stuff with them
print "creating snap bjork"
foo3_pool.create_snap("bjork")
print "creating snap aardvark"
foo3_pool.create_snap("aardvark")
print "creating snap carnuba"
foo3_pool.create_snap("carnuba")
print "listing snaps..."
for snap in foo3_pool.list_snaps():
    print str(snap)

print "removing snap bjork"
foo3_pool.remove_snap("bjork")

# remove foo3
print "deleting foo3"
foo3_pool.delete()

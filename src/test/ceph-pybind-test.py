#!/usr/bin/python

import rados

r = rados.Rados()
r.conf_read_file();
r.connect()
v = r.version()
print "rados version %s" % str(v)

try:
    auid = 100
    r.create_pool("foo1", auid)
    print "created pool foo1 with auid %d" % auid
    r.delete_pool("foo1")
    print "deleted pool foo1"
except rados.ObjectExists:
    print "pool foo1 already exists"

try:
    r.create_pool("foo2")
    print "created pool foo2"
except rados.ObjectExists:
    print "pool foo2 already exists"
if r.pool_exists("foo2") != True:
    raise RuntimeError("we just created pool 'foo2', but it doesn't exist?")
print "opening pool foo2"
foo2_ioctx = r.open_ioctx("foo2")
# give this pool to the anonymous AUID
foo2_ioctx.change_auid(rados.ANONYMOUS_AUID)
# well, actually, we want it back.
foo2_ioctx.change_auid(rados.ADMIN_AUID)
foo2_ioctx.close()
# now delete
print "deleting pool foo2"
r.delete_pool("foo2")

# create a pool and some objects
try:
    r.create_pool("foo3")
except rados.ObjectExists:
    pass
foo3_ioctx = r.open_ioctx("foo3")
foo3_ioctx.write("abc", "abc")
foo3_ioctx.write("def", "def")
abc_str = foo3_ioctx.read("abc")
if (abc_str != "abc"):
    raise RuntimeError("error reading object abc: expected value abc, \
got %s" % abc_str)
# write_full replaces the whole 'def' object
foo3_ioctx.write_full("def", "d")
def_str = foo3_ioctx.read("def")
if (def_str != "d"):
    raise RuntimeError("error reading object def: expected value d, \
got %s" % def_str)

for obj in foo3_ioctx.list_objects():
    print str(obj)

# create some snapshots and do stuff with them
print "creating snap bjork"
foo3_ioctx.create_snap("bjork")
print "creating snap aardvark"
foo3_ioctx.create_snap("aardvark")
print "creating snap carnuba"
foo3_ioctx.create_snap("carnuba")
print "listing snaps..."
for snap in foo3_ioctx.list_snaps():
    print str(snap)

print "removing snap bjork"
foo3_ioctx.remove_snap("bjork")
foo3_ioctx.close()

# remove foo3
print "deleting foo3"
r.delete_pool("foo3")

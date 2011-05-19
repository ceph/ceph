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
b_str = foo3_ioctx.read("abc", 1, 1)
if (b_str != "b"):
    raise RuntimeError("error reading object abc: expected value b, \
got %s" % b_str)
# write_full replaces the whole 'def' object
foo3_ioctx.write_full("def", "d")
def_str = foo3_ioctx.read("def")
if (def_str != "d"):
    raise RuntimeError("error reading object def: expected value d, \
got %s" % def_str)
for obj in foo3_ioctx.list_objects():
    print str(obj)

# do some things with extended attributes
foo3_ioctx.set_xattr("abc", "a", "1")
foo3_ioctx.set_xattr("def", "b", "2")
foo3_ioctx.set_xattr("abc", "c", "3")
ret = foo3_ioctx.get_xattr("abc", "a")
if (ret != "1"):
  raise RuntimeError("error: expected object abc to have a=1")
ret = foo3_ioctx.get_xattr("def", "b")
if (ret != "2"):
  raise RuntimeError("error: expected object def to have b=2")
ret = foo3_ioctx.get_xattr("abc", "c")
if (ret != "3"):
  raise RuntimeError("error: expected object abc to have c=3")
found = {}
for k,v in foo3_ioctx.get_xattrs("abc"):
  found[k] = v
if (len(found) != 2):
  raise RuntimeError("error: expected two extended attributes on abc")
if (found["a"] != "1"):
  raise RuntimeError("error: expected object abc to have a=1")
if (found["c"] != "3"):
  raise RuntimeError("error: expected object abc to have c=3")

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

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <array>
#include <set>
#include <string>

#include "gtest/gtest.h"

#include "crimson/osd/attr_cache_update.h"

using crimson::osd::attr_map_t;
using crimson::osd::tracked_attrs_t;
using crimson::osd::update_attr_cache;

namespace {

ceph::bufferlist bl_of(const std::string& s)
{
  ceph::bufferlist bl;
  bl.append(s);
  return bl;
}

attr_map_t attrs_of(
  std::initializer_list<std::pair<const std::string, std::string>> kv)
{
  attr_map_t attrs;
  for (const auto& [k, v] : kv) {
    attrs[k] = bl_of(v);
  }
  return attrs;
}

// compares the names and the contents of the values
void expect_attrs(const attr_map_t& expected, const attr_map_t& actual)
{
  ASSERT_EQ(expected.size(), actual.size());
  for (const auto& [name, val] : expected) {
    auto it = actual.find(name);
    ASSERT_NE(it, actual.end()) << "missing " << name;
    EXPECT_TRUE(val.contents_equal(it->second)) << "value of " << name;
  }
}

struct AttrCacheUpdate : public ::testing::Test {
  const coll_t cid{spg_t{pg_t{1, 1}}};
  const hobject_t head{object_t{"obj"}, "", CEPH_NOSNAP, 1, 1, ""};
  const hobject_t clone{object_t{"obj"}, "", 4, 1, 1, ""};
  const hobject_t other{object_t{"other"}, "", CEPH_NOSNAP, 2, 1, ""};
  attr_map_t head_attrs;
  attr_map_t clone_attrs;

  ghobject_t g(const hobject_t& o) const {
    return ghobject_t{o};
  }

  // Transaction::setattr() takes a non-const bufferlist
  void setattr(ceph::os::Transaction& t, const hobject_t& o,
               const std::string& name, const std::string& value) {
    ceph::bufferlist bl = bl_of(value);
    t.setattr(cid, g(o), name.c_str(), bl);
  }

  // tracks the head and the clone, not other
  void apply(ceph::os::Transaction& t) {
    std::array<tracked_attrs_t, 2> objs{
      tracked_attrs_t{&head, &head_attrs},
      tracked_attrs_t{&clone, &clone_attrs}};
    update_attr_cache(t, objs);
  }
};

} // anonymous namespace

TEST_F(AttrCacheUpdate, SetattrAndRmattr)
{
  head_attrs = attrs_of({{"_a", "1"}, {"_b", "2"}});
  ceph::os::Transaction t;
  setattr(t, head, "_a", "10");
  setattr(t, head, "_c", "3");
  t.rmattr(cid, g(head), "_b");
  apply(t);
  expect_attrs(attrs_of({{"_a", "10"}, {"_c", "3"}}), head_attrs);
}

TEST_F(AttrCacheUpdate, SetattrsMerges)
{
  head_attrs = attrs_of({{"_a", "1"}, {"_b", "2"}});
  ceph::os::Transaction t;
  std::map<std::string, ceph::bufferlist, std::less<>> aset;
  aset["_b"] = bl_of("20");
  aset["_c"] = bl_of("3");
  t.setattrs(cid, g(head), aset);
  apply(t);
  expect_attrs(attrs_of({{"_a", "1"}, {"_b", "20"}, {"_c", "3"}}), head_attrs);
}

TEST_F(AttrCacheUpdate, RmattrsAndRemoveClear)
{
  head_attrs = attrs_of({{"_a", "1"}});
  clone_attrs = attrs_of({{"_a", "1"}});
  ceph::os::Transaction t;
  t.rmattrs(cid, g(head));
  t.remove(cid, g(clone));
  apply(t);
  EXPECT_TRUE(head_attrs.empty());
  EXPECT_TRUE(clone_attrs.empty());
}

// as RGW does: DELETE, CREATE, then new xattrs
TEST_F(AttrCacheUpdate, RemoveCreateSetattr)
{
  head_attrs = attrs_of({{"_", "oi"}, {"_old", "x"}});
  ceph::os::Transaction t;
  t.remove(cid, g(head));
  t.create(cid, g(head));
  setattr(t, head, "_", "oi2");
  setattr(t, head, "_new", "y");
  apply(t);
  expect_attrs(attrs_of({{"_", "oi2"}, {"_new", "y"}}), head_attrs);
}

// as a write after a snapshot does: the clone gets the xattrs from before
// the write, without the snapset, and the head gets the new value
TEST_F(AttrCacheUpdate, CloneBeforeWrite)
{
  head_attrs = attrs_of({{"_", "oi"}, {"snapset", "ss"}, {"_key", "v1"}});
  clone_attrs = attrs_of({{"_stale", "z"}});
  ceph::os::Transaction t;
  t.clone(cid, g(head), g(clone));
  t.rmattr(cid, g(clone), "snapset");
  setattr(t, head, "_key", "v2");
  setattr(t, clone, "_", "clone-oi");
  apply(t);
  expect_attrs(attrs_of({{"_", "oi"}, {"snapset", "ss"}, {"_key", "v2"}}),
	       head_attrs);
  expect_attrs(attrs_of({{"_", "clone-oi"}, {"_key", "v1"}}), clone_attrs);
}

// as ROLLBACK does: remove the head, then clone the snapshot to the head
TEST_F(AttrCacheUpdate, RollbackFromClone)
{
  head_attrs = attrs_of({{"_", "oi"}, {"_key", "v2"}});
  clone_attrs = attrs_of({{"_", "clone-oi"}, {"_key", "v1"}});
  ceph::os::Transaction t;
  t.remove(cid, g(head));
  t.clone(cid, g(clone), g(head));
  apply(t);
  expect_attrs(attrs_of({{"_", "clone-oi"}, {"_key", "v1"}}), head_attrs);
  expect_attrs(attrs_of({{"_", "clone-oi"}, {"_key", "v1"}}), clone_attrs);
}

TEST_F(AttrCacheUpdate, UntrackedObjectIsIgnored)
{
  head_attrs = attrs_of({{"_a", "1"}});
  ceph::os::Transaction t;
  setattr(t, other, "_a", "x");
  t.remove(cid, g(other));
  // a clone from a tracked object to an untracked one
  t.clone(cid, g(head), g(other));
  apply(t);
  expect_attrs(attrs_of({{"_a", "1"}}), head_attrs);
}

// Every op must decode exactly its own data, or the ops after it read the
// wrong data. Put an xattr op after each op that has data.
TEST_F(AttrCacheUpdate, DataOpsKeepDecodingInStep)
{
  ceph::os::Transaction t;
  const auto marker = [&](int n) {
    setattr(t, head, "_m" + std::to_string(n), std::to_string(n));
  };
  // a write larger than a page, to use the aligned data buffer too
  ceph::bufferlist big;
  big.append(std::string(3 * CEPH_PAGE_SIZE + 123, 'w'));
  t.write(cid, g(head), 17, big.length(), big);
  marker(1);
  ceph::bufferlist small = bl_of("small");
  t.write(cid, g(other), 0, small.length(), small);
  marker(2);
  std::map<std::string, ceph::bufferlist> keys;
  keys["k1"] = bl_of("v1");
  keys["k2"] = bl_of("v2");
  t.omap_setkeys(cid, g(head), keys);
  marker(3);
  std::set<std::string> rm_keys{"k1"};
  t.omap_rmkeys(cid, g(head), rm_keys);
  marker(4);
  t.omap_rmkeyrange(cid, g(head), "a", "z");
  marker(5);
  t.omap_setheader(cid, g(head), bl_of("header"));
  marker(6);
  t.collection_hint(cid, 1, bl_of("hint"));
  marker(7);
  t.zero(cid, g(head), 0, 4);
  t.truncate(cid, g(head), 8);
  t.touch(cid, g(other));
  t.omap_clear(cid, g(other));
  t.set_alloc_hint(cid, g(head), 4096, 4096, 0);
  t.clone_range(cid, g(head), g(other), 0, 4, 0);
  t.nop();
  marker(8);
  apply(t);
  expect_attrs(attrs_of({{"_m1", "1"}, {"_m2", "2"}, {"_m3", "3"},
                         {"_m4", "4"}, {"_m5", "5"}, {"_m6", "6"},
                         {"_m7", "7"}, {"_m8", "8"}}),
               head_attrs);
}

// collection_move() emits OP_COLL_ADD and OP_COLL_REMOVE, which have no
// dest_oid. Their dest_oid is 0, which here is the head: the update must
// not look at it.
TEST_F(AttrCacheUpdate, CollAddRemoveIgnoreDestOid)
{
  head_attrs = attrs_of({{"_a", "1"}});
  ceph::os::Transaction t;
  setattr(t, head, "_a", "2");  // the head gets object id 0
  const coll_t other_cid{spg_t{pg_t{2, 1}}};
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  t.collection_move(other_cid, cid, g(other));
#pragma GCC diagnostic pop
  apply(t);
  expect_attrs(attrs_of({{"_a", "2"}}), head_attrs);
}

TEST_F(AttrCacheUpdate, RenameOfTempObjectIsIgnored)
{
  head_attrs = attrs_of({{"_a", "1"}});
  const hobject_t temp = other.make_temp_hobject("temp");
  ceph::os::Transaction t;
  t.touch(cid, g(temp));
  setattr(t, temp, "_a", "x");
  t.try_rename(cid, g(temp), g(other));
  t.collection_move_rename(cid, g(other), cid, g(temp));
  apply(t);
  expect_attrs(attrs_of({{"_a", "1"}}), head_attrs);
}

TEST_F(AttrCacheUpdate, CloneFromUntrackedObjectAborts)
{
  ceph::os::Transaction t;
  t.clone(cid, g(other), g(head));
  EXPECT_DEATH(apply(t), "");
}

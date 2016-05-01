// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "include/types.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/Cond.h"
#include "test/librados/test.h"
#include "test/librados/TestCase.h"
#include "json_spirit/json_spirit.h"

#include "osd/HitSet.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

using namespace librados;
using std::map;
using std::ostringstream;
using std::string;

typedef RadosTestPP LibRadosTierPP;
typedef RadosTestECPP LibRadosTierECPP;

void flush_evict_all(librados::Rados& cluster, librados::IoCtx& cache_ioctx)
{
  bufferlist inbl;
  cache_ioctx.set_namespace(all_nspaces);
  for (NObjectIterator it = cache_ioctx.nobjects_begin();
       it != cache_ioctx.nobjects_end(); ++it) {
    cache_ioctx.locator_set_key(it->get_locator());
    cache_ioctx.set_namespace(it->get_nspace());
    {
      ObjectReadOperation op;
      op.cache_flush();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      cache_ioctx.aio_operate(
        it->get_oid(), completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL);
      completion->wait_for_safe();
      completion->get_return_value();
      completion->release();
    }
    {
      ObjectReadOperation op;
      op.cache_evict();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      cache_ioctx.aio_operate(
        it->get_oid(), completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL);
      completion->wait_for_safe();
      completion->get_return_value();
      completion->release();
    }
  }
}

class LibRadosTwoPoolsPP : public RadosTestPP
{
public:
  LibRadosTwoPoolsPP() {};
  virtual ~LibRadosTwoPoolsPP() {};
protected:
  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
  }
  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
  }
  static std::string cache_pool_name;

  virtual void SetUp() {
    cache_pool_name = get_temp_pool_name();
    ASSERT_EQ(0, s_cluster.pool_create(cache_pool_name.c_str()));
    RadosTestPP::SetUp();
    ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
    cache_ioctx.set_namespace(nspace);
  }
  virtual void TearDown() {
    // flush + evict cache
    flush_evict_all(cluster, cache_ioctx);

    bufferlist inbl;
    // tear down tiers
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

    // wait for maps to settle before next test
    cluster.wait_for_latest_osdmap();

    RadosTestPP::TearDown();

    cleanup_default_namespace(cache_ioctx);
    cleanup_namespace(cache_ioctx, nspace);

    cache_ioctx.close();
    ASSERT_EQ(0, s_cluster.pool_delete(cache_pool_name.c_str()));
  }
  librados::IoCtx cache_ioctx;
};

std::string LibRadosTwoPoolsPP::cache_pool_name;

TEST_F(LibRadosTierPP, Dirty) {
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op)); // still get 0 if it dne
  }
  {
    ObjectWriteOperation op;
    op.create(true);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op));  // still 0 if already clean
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }
  {
    ObjectWriteOperation op;
    op.truncate(0);  // still a write even tho it is a no-op
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }
}

TEST_F(LibRadosTwoPoolsPP, Overlay) {
  // create objects
  {
    bufferlist bl;
    bl.append("base");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("cache");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // by default, the overlay sends us to cache pool
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // unless we say otherwise
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(0, 1, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
    ASSERT_EQ('b', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsPP, Promote) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
  }

  // read, trigger a whiteout
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsPP, PromoteSnap) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  ioctx.snap_set_read(my_snaps[0]);

  // read foo snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // read bar snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // read baz snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  ioctx.snap_set_read(librados::SNAP_HEAD);

  // read foo
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // read bar
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // read baz
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("baz", bl, 1, 0));
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsPP, PromoteSnapScrub) {
  int num = 100;

  // create objects
  for (int i=0; i<num; ++i) {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate(string("foo") + stringify(i), &op));
  }

  vector<uint64_t> my_snaps;
  for (int snap=0; snap<4; ++snap) {
    // create a snapshot, clone
    vector<uint64_t> ns(1);
    ns.insert(ns.end(), my_snaps.begin(), my_snaps.end());
    my_snaps.swap(ns);
    ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
    cout << "my_snaps " << my_snaps << std::endl;
    ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
						      my_snaps));
    for (int i=0; i<num; ++i) {
      bufferlist bl;
      bl.append(string("ciao! snap") + stringify(snap));
      ObjectWriteOperation op;
      op.write_full(bl);
      ASSERT_EQ(0, ioctx.operate(string("foo") + stringify(i), &op));
    }
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on _some_ heads to make sure we handle cases
  // where snaps are present and where they are not.
  cout << "promoting some heads" << std::endl;
  for (int i=0; i<num; ++i) {
    if (i % 5 == 0 || i > num - 3) {
      bufferlist bl;
      ASSERT_EQ(1, ioctx.read(string("foo") + stringify(i), bl, 1, 0));
      ASSERT_EQ('c', bl[0]);
    }
  }

  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    cout << "promoting from clones for snap " << my_snaps[snap] << std::endl;
    ioctx.snap_set_read(my_snaps[snap]);

    // read some snaps, semi-randomly
    for (int i=0; i<50; ++i) {
      bufferlist bl;
      string o = string("foo") + stringify((snap * i * 137) % 80);
      //cout << o << std::endl;
      ASSERT_EQ(1, ioctx.read(o, bl, 1, 0));
    }
  }

  // ok, stop and scrub this pool (to make sure scrub can handle
  // missing clones in the cache tier).
  {
    IoCtx cache_ioctx;
    ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
    for (int i=0; i<10; ++i) {
      ostringstream ss;
      ss << "{\"prefix\": \"pg scrub\", \"pgid\": \""
	 << cache_ioctx.get_id() << "." << i
	 << "\"}";
      cluster.mon_command(ss.str(), inbl, NULL, NULL);
    }

    // give it a few seconds to go.  this is sloppy but is usually enough time
    cout << "waiting for scrubs..." << std::endl;
    sleep(30);
    cout << "done waiting" << std::endl;
  }

  ioctx.snap_set_read(librados::SNAP_HEAD);

  //cleanup
  for (unsigned snap = 0; snap < my_snaps.size(); ++snap) {
    ioctx.selfmanaged_snap_remove(my_snaps[snap]);
  }
}

TEST_F(LibRadosTwoPoolsPP, PromoteSnapTrimRace) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // delete the snap
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[0]));

  ioctx.snap_set_read(my_snaps[0]);

  // read foo snap
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("foo", bl, 1, 0));
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsPP, Whiteout) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create some whiteouts, verify they behave
  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(-ENOENT, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(-ENOENT, ioctx.operate("bar", &op));
  }

  // verify the whiteouts are there in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // delete a whiteout and verify it goes away
  ASSERT_EQ(-ENOENT, ioctx.remove("foo"));
  {
    ObjectWriteOperation op;
    op.remove();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("bar", completion, &op,
				   librados::OPERATION_IGNORE_CACHE));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();

    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // recreate an object and verify we can read it
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsPP, WhiteoutDeleteCreate) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create an object
  {
    bufferlist bl;
    bl.append("foo");
    ASSERT_EQ(0, ioctx.write_full("foo", bl));
  }

  // do delete + create operation
  {
    ObjectWriteOperation op;
    op.remove();
    bufferlist bl;
    bl.append("bar");
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify it still "exists" (w/ new content)
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('b', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsPP, Evict) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
  }

  // read, trigger a whiteout, and a dirty object
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(0, ioctx.write("bar", bl, bl.length(), 0));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
					 librados::OPERATION_IGNORE_CACHE,
					 NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
					 librados::OPERATION_IGNORE_CACHE,
					 NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
}

TEST_F(LibRadosTwoPoolsPP, EvictSnap) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // evict bam
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bam", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bam", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }

  // read foo snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // evict foo snap
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // snap is gone...
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }
  // head is still there...
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // promote head + snap of bar
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // evict bar head (fail)
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }

  // evict bar snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // ...and then head
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

// this test case reproduces http://tracker.ceph.com/issues/8629
TEST_F(LibRadosTwoPoolsPP, EvictSnap2) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify the snapdir is not present in the cache pool
  {
    ObjectReadOperation op;
    librados::snap_set_t snapset;
    op.list_snaps(&snapset, NULL);
    ioctx.snap_set_read(librados::SNAP_DIR);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op,
				   librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }
}

TEST_F(LibRadosTwoPoolsPP, TryFlush) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // verify dirty
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // verify in base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it != ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // evict it
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsPP, Flush) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  uint64_t user_version = 0;

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // verify dirty
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
    user_version = cache_ioctx.get_last_version();
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // verify in base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it != ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // evict it
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // read it again and verify the version is consistent
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ(user_version, cache_ioctx.get_last_version());
  }

  // erase it
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush whiteout
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
  // or base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsPP, FlushSnap) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("a");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("b");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("c");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // flush on head (should fail)
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
  // flush on recent snap (should fail)
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
  // flush on oldest snap
  ioctx.snap_set_read(my_snaps[1]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush on next oldest snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush on head
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify i can read the snaps from the cache pool
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('b', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[1]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('a', bl[0]);
  }

  // remove overlay
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // verify i can read the snaps from the base pool
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('b', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[1]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('a', bl[0]);
  }

  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTierPP, FlushWriteRaces) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  bufferlist bl;
  bl.append("hi there");
  {
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + write
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectWriteOperation op2;
    op2.write_full(bl);
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion2, &op2, 0));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  int tries = 1000;
  do {
    // create/dirty object
    {
      bufferlist bl;
      bl.append("hi there");
      ObjectWriteOperation op;
      op.write_full(bl);
      ASSERT_EQ(0, ioctx.operate("foo", &op));
    }

    // try-flush + write
    {
      ObjectReadOperation op;
      op.cache_try_flush();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY |
	librados::OPERATION_SKIPRWLOCKS, NULL));

      ObjectWriteOperation op2;
      op2.write_full(bl);
      librados::AioCompletion *completion2 = cluster.aio_create_completion();
      ASSERT_EQ(0, ioctx.aio_operate("foo", completion2, &op2, 0));

      completion->wait_for_safe();
      completion2->wait_for_safe();
      int r = completion->get_return_value();
      ASSERT_TRUE(r == -EBUSY || r == 0);
      ASSERT_EQ(0, completion2->get_return_value());
      completion->release();
      completion2->release();
      if (r == -EBUSY)
	break;
      cout << "didn't get EBUSY, trying again" << std::endl;
    }
    ASSERT_TRUE(--tries);
  } while (true);

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();

  ASSERT_EQ(0, cluster.pool_delete(cache_pool_name.c_str()));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST_F(LibRadosTwoPoolsPP, FlushTryFlushRaces) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectReadOperation op2;
    op2.cache_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + try-flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectReadOperation op2;
    op2.cache_try_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  // create/dirty object
  int tries = 1000;
  do {
    {
      bufferlist bl;
      bl.append("hi there");
      ObjectWriteOperation op;
      op.write_full(bl);
      ASSERT_EQ(0, ioctx.operate("foo", &op));
    }

    // try-flush + flush
    //  (flush will not piggyback on try-flush)
    {
      ObjectReadOperation op;
      op.cache_try_flush();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY |
	librados::OPERATION_SKIPRWLOCKS, NULL));

      ObjectReadOperation op2;
      op2.cache_flush();
      librados::AioCompletion *completion2 = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion2, &op2,
	librados::OPERATION_IGNORE_OVERLAY, NULL));

      completion->wait_for_safe();
      completion2->wait_for_safe();
      int r = completion->get_return_value();
      ASSERT_TRUE(r == -EBUSY || r == 0);
      ASSERT_EQ(0, completion2->get_return_value());
      completion->release();
      completion2->release();
      if (r == -EBUSY)
	break;
      cout << "didn't get EBUSY, trying again" << std::endl;
    }
    ASSERT_TRUE(--tries);
  } while (true);

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // try-flush + try-flush
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    ObjectReadOperation op2;
    op2.cache_try_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }
}


IoCtx *read_ioctx = 0;
Mutex test_lock("FlushReadRaces::lock");
Cond cond;
int max_reads = 100;
int num_reads = 0; // in progress

void flush_read_race_cb(completion_t cb, void *arg);

void start_flush_read()
{
  //cout << " starting read" << std::endl;
  ObjectReadOperation op;
  op.stat(NULL, NULL, NULL);
  librados::AioCompletion *completion =
    librados::Rados::aio_create_completion();
  completion->set_complete_callback(0, flush_read_race_cb);
  read_ioctx->aio_operate("foo", completion, &op, NULL);
}

void flush_read_race_cb(completion_t cb, void *arg)
{
  //cout << " finished read" << std::endl;
  test_lock.Lock();
  if (num_reads > max_reads) {
    num_reads--;
    cond.Signal();
  } else {
    start_flush_read();
  }
  // fixme: i'm leaking cb...
  test_lock.Unlock();
}

TEST_F(LibRadosTwoPoolsPP, TryFlushReadRace) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    bufferptr bp(4000000);  // make it big!
    bp.zero();
    bl.append(bp);
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // start a continuous stream of reads
  read_ioctx = &ioctx;
  test_lock.Lock();
  for (int i = 0; i < max_reads; ++i) {
    start_flush_read();
    num_reads++;
  }
  test_lock.Unlock();

  // try-flush
  ObjectReadOperation op;
  op.cache_try_flush();
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

  completion->wait_for_safe();
  ASSERT_EQ(0, completion->get_return_value());
  completion->release();

  // stop reads
  test_lock.Lock();
  max_reads = 0;
  while (num_reads > 0)
    cond.Wait(test_lock);
  test_lock.Unlock();
}

TEST_F(LibRadosTierPP, HitSetNone) {
  {
    list< pair<time_t,time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, ioctx.hit_set_list(123, c, &ls));
    c->wait_for_complete();
    ASSERT_EQ(0, c->get_return_value());
    ASSERT_TRUE(ls.empty());
    c->release();
  }
  {
    bufferlist bl;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, ioctx.hit_set_get(123, c, 12345, &bl));
    c->wait_for_complete();
    ASSERT_EQ(-ENOENT, c->get_return_value());
    c->release();
  }
}

string set_pool_str(string pool, string var, string val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + val + string("\"}");
}

string set_pool_str(string pool, string var, int val)
{
  return string("{\"prefix\": \"osd pool set\",\"pool\":\"") + pool
    + string("\",\"var\": \"") + var + string("\",\"val\": \"")
    + stringify(val) + string("\"}");
}

TEST_F(LibRadosTwoPoolsPP, HitSetRead) {
  // make it a tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_count", 2),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_period", 600),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_type",
						"explicit_object"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  cache_ioctx.set_namespace("");

  // keep reading until we see our object appear in the HitSet
  utime_t start = ceph_clock_now(NULL);
  utime_t hard_stop = start + utime_t(600, 0);

  while (true) {
    utime_t now = ceph_clock_now(NULL);
    ASSERT_TRUE(now < hard_stop);

    string name = "foo";
    uint32_t hash; 
    ASSERT_EQ(0, cache_ioctx.get_object_hash_position2(name, &hash));
    hobject_t oid(sobject_t(name, CEPH_NOSNAP), "", hash,
		  cluster.pool_lookup(cache_pool_name.c_str()), "");

    bufferlist bl;
    ASSERT_EQ(-ENOENT, cache_ioctx.read("foo", bl, 1, 0));

    bufferlist hbl;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.hit_set_get(hash, c, now.sec(), &hbl));
    c->wait_for_complete();
    c->release();

    if (hbl.length()) {
      bufferlist::iterator p = hbl.begin();
      HitSet hs;
      ::decode(hs, p);
      if (hs.contains(oid)) {
	cout << "ok, hit_set contains " << oid << std::endl;
	break;
      }
      cout << "hmm, not in HitSet yet" << std::endl;
    } else {
      cout << "hmm, no HitSet yet" << std::endl;
    }

    sleep(1);
  }
}

static int _get_pg_num(Rados& cluster, string pool_name)
{
  bufferlist inbl;
  string cmd = string("{\"prefix\": \"osd pool get\",\"pool\":\"")
    + pool_name
    + string("\",\"var\": \"pg_num\",\"format\": \"json\"}");
  bufferlist outbl;
  int r = cluster.mon_command(cmd, inbl, &outbl, NULL);
  assert(r >= 0);
  string outstr(outbl.c_str(), outbl.length());
  json_spirit::Value v;
  if (!json_spirit::read(outstr, v)) {
    cerr <<" unable to parse json " << outstr << std::endl;
    return -1;
  }

  json_spirit::Object& o = v.get_obj();
  for (json_spirit::Object::size_type i=0; i<o.size(); i++) {
    json_spirit::Pair& p = o[i];
    if (p.name_ == "pg_num") {
      cout << "pg_num = " << p.value_.get_int() << std::endl;
      return p.value_.get_int();
    }
  }
  cerr << "didn't find pg_num in " << outstr << std::endl;
  return -1;
}


TEST_F(LibRadosTwoPoolsPP, HitSetWrite) {
  int num_pg = _get_pg_num(cluster, pool_name);
  assert(num_pg > 0);

  // make it a tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_count", 8),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_period", 600),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_type",
						"explicit_hash"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  cache_ioctx.set_namespace("");

  int num = 200;

  // do a bunch of writes
  for (int i=0; i<num; ++i) {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, cache_ioctx.write(stringify(i), bl, 1, 0));
  }

  // get HitSets
  std::map<int,HitSet> hitsets;
  for (int i=0; i<num_pg; ++i) {
    list< pair<time_t,time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.hit_set_list(i, c, &ls));
    c->wait_for_complete();
    c->release();
    std::cout << "pg " << i << " ls " << ls << std::endl;
    ASSERT_FALSE(ls.empty());

    // get the latest
    c = librados::Rados::aio_create_completion();
    bufferlist bl;
    ASSERT_EQ(0, cache_ioctx.hit_set_get(i, c, ls.back().first, &bl));
    c->wait_for_complete();
    c->release();

    //std::cout << "bl len is " << bl.length() << "\n";
    //bl.hexdump(std::cout);
    //std::cout << std::endl;

    bufferlist::iterator p = bl.begin();
    ::decode(hitsets[i], p);

    // cope with racing splits by refreshing pg_num
    if (i == num_pg - 1)
      num_pg = _get_pg_num(cluster, cache_pool_name);
  }

  for (int i=0; i<num; ++i) {
    string n = stringify(i);
    uint32_t hash;
    ASSERT_EQ(0, cache_ioctx.get_object_hash_position2(n, &hash));
    hobject_t oid(sobject_t(n, CEPH_NOSNAP), "", hash,
		  cluster.pool_lookup(cache_pool_name.c_str()), "");
    std::cout << "checking for " << oid << std::endl;
    bool found = false;
    for (int p=0; p<num_pg; ++p) {
      if (hitsets[p].contains(oid)) {
	found = true;
	break;
      }
    }
    ASSERT_TRUE(found);
  }
}

TEST_F(LibRadosTwoPoolsPP, HitSetTrim) {
  unsigned count = 3;
  unsigned period = 3;

  // make it a tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_count", count),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_period", period),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
				   inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_fpp", ".01"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  cache_ioctx.set_namespace("");

  // do a bunch of writes and make sure the hitsets rotate
  utime_t start = ceph_clock_now(NULL);
  utime_t hard_stop = start + utime_t(count * period * 50, 0);

  time_t first = 0;
  while (true) {
    string name = "foo";
    uint32_t hash; 
    ASSERT_EQ(0, cache_ioctx.get_object_hash_position2(name, &hash));
    hobject_t oid(sobject_t(name, CEPH_NOSNAP), "", hash, -1, "");

    bufferlist bl;
    bl.append("f");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, 1, 0));

    list<pair<time_t, time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.hit_set_list(hash, c, &ls));
    c->wait_for_complete();
    c->release();

    cout << " got ls " << ls << std::endl;
    if (!ls.empty()) {
      if (!first) {
	first = ls.front().first;
	cout << "first is " << first << std::endl;
      } else {
	if (ls.front().first != first) {
	  cout << "first now " << ls.front().first << ", trimmed" << std::endl;
	  break;
	}
      }
    }

    utime_t now = ceph_clock_now(NULL);
    ASSERT_TRUE(now < hard_stop);

    sleep(1);
  }
}

TEST_F(LibRadosTwoPoolsPP, PromoteOn2ndRead) {
  // create object
  for (int i=0; i<20; ++i) {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo" + stringify(i), &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_count", 2),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_period", 600),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "min_read_recency_for_promote", 1),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_grade_decay_rate", 20),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_search_last_n", 1),
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  int fake = 0;  // set this to non-zero to test spurious promotion,
		 // e.g. from thrashing
  int attempt = 0;
  string obj;
  while (true) {
    // 1st read, don't trigger a promote
    obj = "foo" + stringify(attempt);
    cout << obj << std::endl;
    {
      bufferlist bl;
      ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));
      if (--fake >= 0) {
	sleep(1);
	ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));
	sleep(1);
      }
    }

    // verify the object is NOT present in the cache tier
    {
      bool found = false;
      NObjectIterator it = cache_ioctx.nobjects_begin();
      while (it != cache_ioctx.nobjects_end()) {
	cout << " see " << it->get_oid() << std::endl;
	if (it->get_oid() == string(obj.c_str())) {
	  found = true;
	  break;
	}
	++it;
      }
      if (!found)
	break;
    }

    ++attempt;
    ASSERT_LE(attempt, 20);
    cout << "hrm, object is present in cache on attempt " << attempt
	 << ", retrying" << std::endl;
  }

  // Read until the object is present in the cache tier
  cout << "verifying " << obj << " is eventually promoted" << std::endl;
  while (true) {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));

    bool there = false;
    NObjectIterator it = cache_ioctx.nobjects_begin();
    while (it != cache_ioctx.nobjects_end()) {
      if (it->get_oid() == string(obj.c_str())) {
	there = true;
	break;
      }
      ++it;
    }
    if (there)
      break;

    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ProxyRead) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"readproxy\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // Verify 10 times the object is NOT present in the cache tier
  uint32_t i = 0;
  while (i++ < 10) {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, CachePin) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
  }

  // verify the objects are present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    for (uint32_t i = 0; i < 4; i++) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("bar") ||
                  it->get_oid() == string("baz") ||
                  it->get_oid() == string("bam"));
      ++it;
    }
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // pin objects
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("baz", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // enable agent
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_count", 2),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_period", 600),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "min_read_recency_for_promote", 1),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "target_max_objects", 1),
    inbl, NULL, NULL));

  sleep(10);

  // Verify the pinned object 'foo' is not flushed/evicted
  uint32_t count = 0;
  while (true) {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));

    count = 0;
    NObjectIterator it = cache_ioctx.nobjects_begin();
    while (it != cache_ioctx.nobjects_end()) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("bar") ||
                  it->get_oid() == string("baz") ||
                  it->get_oid() == string("bam"));
      ++count;
      ++it;
    }
    if (count == 2) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("baz"));
      break;
    }

    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}


int notify_sleep = 0;
bufferlist notify_bl;
std::set<uint64_t> notify_cookies;
const char *notify_oid = 0;
int notify_err = 0;
IoCtx *notify_ioctx;

class WatchNotifyTestCtx2 : public WatchCtx2
{
public:
  void handle_notify(uint64_t notify_id, uint64_t cookie, uint64_t notifier_gid,
		     bufferlist& bl) {
    std::cout << __func__ << " cookie " << cookie << " notify_id " << notify_id
	      << " notifier_gid " << notifier_gid << std::endl;
    notify_bl = bl;
    notify_cookies.insert(cookie);
    bufferlist reply;
    reply.append("reply", 5);
    if (notify_sleep)
      sleep(notify_sleep);
    notify_ioctx->notify_ack(notify_oid, notify_id, cookie, reply);
  }

  void handle_error(uint64_t cookie, int err) {
    std::cout << __func__ << " cookie " << cookie
	      << " err " << err << std::endl;
    assert(cookie > 1000);
    notify_err = err;
  }
};

TEST_F(LibRadosTwoPoolsPP, EvictWatchedObject) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  notify_oid = "foo";
  notify_ioctx = &cache_ioctx;
  notify_cookies.clear();
  notify_err = 0;
  uint64_t handle;
  WatchNotifyTestCtx2 ctx;
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  int left = 300;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  ioctx.unwatch2(handle);
}

class LibRadosTwoPoolsECPP : public RadosTestECPP
{
public:
  LibRadosTwoPoolsECPP() {};
  virtual ~LibRadosTwoPoolsECPP() {};
protected:
  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
  }
  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
  }
  static std::string cache_pool_name;

  virtual void SetUp() {
    cache_pool_name = get_temp_pool_name();
    ASSERT_EQ(0, s_cluster.pool_create(cache_pool_name.c_str()));
    RadosTestECPP::SetUp();
    ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
    cache_ioctx.set_namespace(nspace);
  }
  virtual void TearDown() {
    // flush + evict cache
    flush_evict_all(cluster, cache_ioctx);

    bufferlist inbl;
    // tear down tiers
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
      "\"}",
      inbl, NULL, NULL));
    ASSERT_EQ(0, cluster.mon_command(
      "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
      "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

    // wait for maps to settle before next test
    cluster.wait_for_latest_osdmap();

    RadosTestECPP::TearDown();

    cleanup_default_namespace(cache_ioctx);
    cleanup_namespace(cache_ioctx, nspace);

    cache_ioctx.close();
    ASSERT_EQ(0, s_cluster.pool_delete(cache_pool_name.c_str()));
  }

  librados::IoCtx cache_ioctx;
};

std::string LibRadosTwoPoolsECPP::cache_pool_name;

TEST_F(LibRadosTierECPP, Dirty) {
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op)); // still get 0 if it dne
  }
  {
    ObjectWriteOperation op;
    op.create(true);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    ObjectWriteOperation op;
    op.undirty();
    ASSERT_EQ(0, ioctx.operate("foo", &op));  // still 0 if already clean
  }
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }
  //{
  //  ObjectWriteOperation op;
  //  op.truncate(0);  // still a write even tho it is a no-op
  //  ASSERT_EQ(0, ioctx.operate("foo", &op));
  //}
  //{
  //  bool dirty = false;
  //  int r = -1;
  //  ObjectReadOperation op;
  //  op.is_dirty(&dirty, &r);
  //  ASSERT_EQ(0, ioctx.operate("foo", &op, NULL));
  //  ASSERT_TRUE(dirty);
  //  ASSERT_EQ(0, r);
  //}
}

TEST_F(LibRadosTwoPoolsECPP, Overlay) {
  // create objects
  {
    bufferlist bl;
    bl.append("base");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("cache");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // by default, the overlay sends us to cache pool
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // unless we say otherwise
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(0, 1, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
    ASSERT_EQ('b', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsECPP, Promote) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
  }

  // read, trigger a whiteout
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsECPP, PromoteSnap) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  ioctx.snap_set_read(my_snaps[0]);

  // stop and scrub this pg (to make sure scrub can handle missing
  // clones in the cache tier)
  // This test requires cache tier and base tier to have the same pg_num/pgp_num
  {
    for (int tries = 0; tries < 5; ++tries) {
      IoCtx cache_ioctx;
      ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
      uint32_t hash;
      ASSERT_EQ(0, ioctx.get_object_pg_hash_position2("foo", &hash));
      ostringstream ss;
      ss << "{\"prefix\": \"pg scrub\", \"pgid\": \""
	 << cache_ioctx.get_id() << "."
	 << hash
	 << "\"}";
      int r = cluster.mon_command(ss.str(), inbl, NULL, NULL);
      if (r == -EAGAIN)
	continue;
      ASSERT_EQ(0, r);
      break;
    }
    // give it a few seconds to go.  this is sloppy but is usually enough time
    cout << "waiting for scrub..." << std::endl;
    sleep(15);
    cout << "done waiting" << std::endl;
  }

  // read foo snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // read bar snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // read baz snap
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  ioctx.snap_set_read(librados::SNAP_HEAD);

  // read foo
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // read bar
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // read baz
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("baz", bl, 1, 0));
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsECPP, PromoteSnapTrimRace) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // delete the snap
  ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(my_snaps[0]));

  ioctx.snap_set_read(my_snaps[0]);

  // read foo snap
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("foo", bl, 1, 0));
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsECPP, Whiteout) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create some whiteouts, verify they behave
  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(-ENOENT, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.assert_exists();
    op.remove();
    ASSERT_EQ(-ENOENT, ioctx.operate("bar", &op));
  }

  // verify the whiteouts are there in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // delete a whiteout and verify it goes away
  ASSERT_EQ(-ENOENT, ioctx.remove("foo"));
  {
    ObjectWriteOperation op;
    op.remove();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("bar", completion, &op,
				   librados::OPERATION_IGNORE_CACHE));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();

    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // recreate an object and verify we can read it
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsECPP, Evict) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
  }

  // read, trigger a whiteout, and a dirty object
  {
    bufferlist bl;
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(-ENOENT, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(0, ioctx.write("bar", bl, bl.length(), 0));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it->get_oid() == string("foo") || it->get_oid() == string("bar"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
					 librados::OPERATION_IGNORE_CACHE,
					 NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
					 librados::OPERATION_IGNORE_CACHE,
					 NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
}

TEST_F(LibRadosTwoPoolsECPP, EvictSnap) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("ciao!");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // evict bam
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bam", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "bam", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }

  // read foo snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // evict foo snap
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // snap is gone...
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }
  // head is still there...
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // promote head + snap of bar
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // evict bar head (fail)
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }

  // evict bar snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // ...and then head
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "bar", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsECPP, TryFlush) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // verify dirty
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // verify in base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it != ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // evict it
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsECPP, FailedFlush) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // set omap
  {
    ObjectWriteOperation op;
    std::map<std::string, bufferlist> omap;
    omap["somekey"] = bufferlist();
    op.omap_set(omap);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_NE(0, completion->get_return_value());
    completion->release();
  }

  // get omap
  {
    ObjectReadOperation op;
    bufferlist bl;
    int prval = 0;
    std::set<std::string> keys;
    keys.insert("somekey");
    std::map<std::string, bufferlist> map;

    op.omap_get_vals_by_keys(keys, &map, &prval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op, &bl));
    sleep(5);
    bool completed = completion->is_complete();
    if( !completed ) {
      cache_ioctx.aio_cancel(completion); 
      std::cerr << "Most probably test case will hang here, please reset manually" << std::endl;
      ASSERT_TRUE(completed); //in fact we are locked forever at test case shutdown unless fix for http://tracker.ceph.com/issues/14511 is applied. Seems there is no workaround for that
    }
    completion->release();
  }
  // verify still not in base tier
  {
    ASSERT_TRUE(ioctx.nobjects_begin() == ioctx.nobjects_end());
  }
  // erase it
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  // flush whiteout
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
  // or base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsECPP, Flush) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  uint64_t user_version = 0;

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // verify dirty
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_TRUE(dirty);
    ASSERT_EQ(0, r);
    user_version = cache_ioctx.get_last_version();
  }

  // pin
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush the pinned object with -EPERM
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify clean
  {
    bool dirty = false;
    int r = -1;
    ObjectReadOperation op;
    op.is_dirty(&dirty, &r);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op, NULL));
    ASSERT_FALSE(dirty);
    ASSERT_EQ(0, r);
  }

  // verify in base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it != ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // evict it
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // read it again and verify the version is consistent
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ(user_version, cache_ioctx.get_last_version());
  }

  // erase it
  {
    ObjectWriteOperation op;
    op.remove();
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush whiteout
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify no longer in cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }
  // or base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }
}

TEST_F(LibRadosTwoPoolsECPP, FlushSnap) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("a");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("b");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));
  {
    bufferlist bl;
    bl.append("c");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // verify the object is NOT present in the base tier
  {
    NObjectIterator it = ioctx.nobjects_begin();
    ASSERT_TRUE(it == ioctx.nobjects_end());
  }

  // flush on head (should fail)
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
  // flush on recent snap (should fail)
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
  // flush on oldest snap
  ioctx.snap_set_read(my_snaps[1]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush on next oldest snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush on head
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // verify i can read the snaps from the cache pool
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('b', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[1]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('a', bl[0]);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // verify i can read the snaps from the base pool
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('b', bl[0]);
  }
  ioctx.snap_set_read(my_snaps[1]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('a', bl[0]);
  }

  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTierECPP, FlushWriteRaces) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  bufferlist bl;
  bl.append("hi there");
  {
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + write
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectWriteOperation op2;
    op2.write_full(bl);
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion2, &op2, 0));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  int tries = 1000;
  do {
    // create/dirty object
    {
      bufferlist bl;
      bl.append("hi there");
      ObjectWriteOperation op;
      op.write_full(bl);
      ASSERT_EQ(0, ioctx.operate("foo", &op));
    }

    // try-flush + write
    {
      ObjectReadOperation op;
      op.cache_try_flush();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY |
	librados::OPERATION_SKIPRWLOCKS, NULL));

      ObjectWriteOperation op2;
      op2.write_full(bl);
      librados::AioCompletion *completion2 = cluster.aio_create_completion();
      ASSERT_EQ(0, ioctx.aio_operate("foo", completion2, &op2, 0));

      completion->wait_for_safe();
      completion2->wait_for_safe();
      int r = completion->get_return_value();
      ASSERT_TRUE(r == -EBUSY || r == 0);
      ASSERT_EQ(0, completion2->get_return_value());
      completion->release();
      completion2->release();
      if (r == -EBUSY)
	break;
      cout << "didn't get EBUSY, trying again" << std::endl;
    }
    ASSERT_TRUE(--tries);
  } while (true);

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();

  ASSERT_EQ(0, cluster.pool_delete(cache_pool_name.c_str()));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST_F(LibRadosTwoPoolsECPP, FlushTryFlushRaces) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectReadOperation op2;
    op2.cache_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush + try-flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));

    ObjectReadOperation op2;
    op2.cache_try_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }

  // create/dirty object
  int tries = 1000;
  do {
    {
      bufferlist bl;
      bl.append("hi there");
      ObjectWriteOperation op;
      op.write_full(bl);
      ASSERT_EQ(0, ioctx.operate("foo", &op));
    }

    // try-flush + flush
    //  (flush will not piggyback on try-flush)
    {
      ObjectReadOperation op;
      op.cache_try_flush();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY |
	librados::OPERATION_SKIPRWLOCKS, NULL));

      ObjectReadOperation op2;
      op2.cache_flush();
      librados::AioCompletion *completion2 = cluster.aio_create_completion();
      ASSERT_EQ(0, cache_ioctx.aio_operate(
        "foo", completion2, &op2,
	librados::OPERATION_IGNORE_OVERLAY, NULL));

      completion->wait_for_safe();
      completion2->wait_for_safe();
      int r = completion->get_return_value();
      ASSERT_TRUE(r == -EBUSY || r == 0);
      ASSERT_EQ(0, completion2->get_return_value());
      completion->release();
      completion2->release();
      if (r == -EBUSY)
	break;
      cout << "didn't get EBUSY, trying again" << std::endl;
    }
    ASSERT_TRUE(--tries);
  } while (true);

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // try-flush + try-flush
  {
    ObjectReadOperation op;
    op.cache_try_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    ObjectReadOperation op2;
    op2.cache_try_flush();
    librados::AioCompletion *completion2 = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion2, &op2,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

    completion->wait_for_safe();
    completion2->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }
}

TEST_F(LibRadosTwoPoolsECPP, TryFlushReadRace) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  {
    bufferlist bl;
    bl.append("hi there");
    bufferptr bp(4000000);  // make it big!
    bp.zero();
    bl.append(bp);
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // start a continuous stream of reads
  read_ioctx = &ioctx;
  test_lock.Lock();
  for (int i = 0; i < max_reads; ++i) {
    start_flush_read();
    num_reads++;
  }
  test_lock.Unlock();

  // try-flush
  ObjectReadOperation op;
  op.cache_try_flush();
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

  completion->wait_for_safe();
  ASSERT_EQ(0, completion->get_return_value());
  completion->release();

  // stop reads
  test_lock.Lock();
  max_reads = 0;
  while (num_reads > 0)
    cond.Wait(test_lock);
  test_lock.Unlock();
}

TEST_F(LibRadosTierECPP, CallForcesPromote) {
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  IoCtx ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(pool_name.c_str(), ioctx));

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // set things up such that the op would normally be proxied
  ASSERT_EQ(0, cluster.mon_command(
	      set_pool_str(cache_pool_name, "hit_set_count", 2),
	      inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	      set_pool_str(cache_pool_name, "hit_set_period", 600),
	      inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	      set_pool_str(cache_pool_name, "hit_set_type",
			   "explicit_object"),
	      inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	      set_pool_str(cache_pool_name, "min_read_recency_for_promote",
			   "4"),
	      inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create/dirty object
  bufferlist bl;
  bl.append("hi there");
  {
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
					 librados::OPERATION_IGNORE_CACHE,
					 NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // call
  {
    ObjectReadOperation op;
    bufferlist bl;
    op.exec("rbd", "get_id", bl);
    bufferlist out;
    // should get EIO (not an rbd object), not -EOPNOTSUPP (we didn't promote)
    ASSERT_EQ(-5, ioctx.operate("foo", &op, &out));
  }

  // make sure foo is back in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();

  ASSERT_EQ(0, cluster.pool_delete(cache_pool_name.c_str()));
  ASSERT_EQ(0, destroy_one_pool_pp(pool_name, cluster));
}

TEST_F(LibRadosTierECPP, HitSetNone) {
  {
    list< pair<time_t,time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, ioctx.hit_set_list(123, c, &ls));
    c->wait_for_complete();
    ASSERT_EQ(0, c->get_return_value());
    ASSERT_TRUE(ls.empty());
    c->release();
  }
  {
    bufferlist bl;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, ioctx.hit_set_get(123, c, 12345, &bl));
    c->wait_for_complete();
    ASSERT_EQ(-ENOENT, c->get_return_value());
    c->release();
  }
}

TEST_F(LibRadosTwoPoolsECPP, HitSetRead) {
  // make it a tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_count", 2),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_period", 600),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_type",
						"explicit_object"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  cache_ioctx.set_namespace("");

  // keep reading until we see our object appear in the HitSet
  utime_t start = ceph_clock_now(NULL);
  utime_t hard_stop = start + utime_t(600, 0);

  while (true) {
    utime_t now = ceph_clock_now(NULL);
    ASSERT_TRUE(now < hard_stop);

    string name = "foo";
    uint32_t hash;
    ASSERT_EQ(0, cache_ioctx.get_object_hash_position2(name, &hash));
    hobject_t oid(sobject_t(name, CEPH_NOSNAP), "", hash,
		  cluster.pool_lookup(cache_pool_name.c_str()), "");

    bufferlist bl;
    ASSERT_EQ(-ENOENT, cache_ioctx.read("foo", bl, 1, 0));

    bufferlist hbl;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.hit_set_get(hash, c, now.sec(), &hbl));
    c->wait_for_complete();
    c->release();

    if (hbl.length()) {
      bufferlist::iterator p = hbl.begin();
      HitSet hs;
      ::decode(hs, p);
      if (hs.contains(oid)) {
	cout << "ok, hit_set contains " << oid << std::endl;
	break;
      }
      cout << "hmm, not in HitSet yet" << std::endl;
    } else {
      cout << "hmm, no HitSet yet" << std::endl;
    }

    sleep(1);
  }
}

// disable this test until hitset-get reliably works on EC pools
#if 0
TEST_F(LibRadosTierECPP, HitSetWrite) {
  int num_pg = _get_pg_num(cluster, pool_name);
  assert(num_pg > 0);

  // enable hitset tracking for this pool
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(pool_name, "hit_set_count", 8),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(pool_name, "hit_set_period", 600),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(pool_name, "hit_set_type",
						"explicit_hash"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  ioctx.set_namespace("");

  // do a bunch of writes
  for (int i=0; i<1000; ++i) {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write(stringify(i), bl, 1, 0));
  }

  // get HitSets
  std::map<int,HitSet> hitsets;
  for (int i=0; i<num_pg; ++i) {
    list< pair<time_t,time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, ioctx.hit_set_list(i, c, &ls));
    c->wait_for_complete();
    c->release();
    std::cout << "pg " << i << " ls " << ls << std::endl;
    ASSERT_FALSE(ls.empty());

    // get the latest
    c = librados::Rados::aio_create_completion();
    bufferlist bl;
    ASSERT_EQ(0, ioctx.hit_set_get(i, c, ls.back().first, &bl));
    c->wait_for_complete();
    c->release();

    //std::cout << "bl len is " << bl.length() << "\n";
    //bl.hexdump(std::cout);
    //std::cout << std::endl;

    bufferlist::iterator p = bl.begin();
    ::decode(hitsets[i], p);

    // cope with racing splits by refreshing pg_num
    if (i == num_pg - 1)
      num_pg = _get_pg_num(cluster, pool_name);
  }

  for (int i=0; i<1000; ++i) {
    string n = stringify(i);
    uint32_t hash = ioctx.get_object_hash_position(n);
    hobject_t oid(sobject_t(n, CEPH_NOSNAP), "", hash,
		  cluster.pool_lookup(pool_name.c_str()), "");
    std::cout << "checking for " << oid << std::endl;
    bool found = false;
    for (int p=0; p<num_pg; ++p) {
      if (hitsets[p].contains(oid)) {
	found = true;
	break;
      }
    }
    ASSERT_TRUE(found);
  }
}
#endif

TEST_F(LibRadosTwoPoolsECPP, HitSetTrim) {
  unsigned count = 3;
  unsigned period = 3;

  // make it a tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_count", count),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_period", period),
						inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
				   inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(set_pool_str(cache_pool_name, "hit_set_fpp", ".01"),
				   inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  cache_ioctx.set_namespace("");

  // do a bunch of writes and make sure the hitsets rotate
  utime_t start = ceph_clock_now(NULL);
  utime_t hard_stop = start + utime_t(count * period * 50, 0);

  time_t first = 0;
  int bsize = alignment;
  char *buf = (char *)new char[bsize];
  memset(buf, 'f', bsize);

  while (true) {
    string name = "foo";
    uint32_t hash;
    ASSERT_EQ(0, cache_ioctx.get_object_hash_position2(name, &hash));
    hobject_t oid(sobject_t(name, CEPH_NOSNAP), "", hash, -1, "");

    bufferlist bl;
    bl.append(buf, bsize);
    ASSERT_EQ(0, cache_ioctx.append("foo", bl, bsize));

    list<pair<time_t, time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.hit_set_list(hash, c, &ls));
    c->wait_for_complete();
    c->release();

    cout << " got ls " << ls << std::endl;
    if (!ls.empty()) {
      if (!first) {
	first = ls.front().first;
	cout << "first is " << first << std::endl;
      } else {
	if (ls.front().first != first) {
	  cout << "first now " << ls.front().first << ", trimmed" << std::endl;
	  break;
	}
      }
    }

    utime_t now = ceph_clock_now(NULL);
    ASSERT_TRUE(now < hard_stop);

    sleep(1);
  }
  delete[] buf;
}

TEST_F(LibRadosTwoPoolsECPP, PromoteOn2ndRead) {
  // create object
  for (int i=0; i<20; ++i) {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo" + stringify(i), &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // enable hitset tracking for this pool
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_count", 2),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_period", 600),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "min_read_recency_for_promote", 1),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_grade_decay_rate", 20),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_search_last_n", 1),
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  int fake = 0;  // set this to non-zero to test spurious promotion,
		 // e.g. from thrashing
  int attempt = 0;
  string obj;
  while (true) {
    // 1st read, don't trigger a promote
    obj = "foo" + stringify(attempt);
    cout << obj << std::endl;
    {
      bufferlist bl;
      ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));
      if (--fake >= 0) {
	sleep(1);
	ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));
	sleep(1);
      }
    }

    // verify the object is NOT present in the cache tier
    {
      bool found = false;
      NObjectIterator it = cache_ioctx.nobjects_begin();
      while (it != cache_ioctx.nobjects_end()) {
	cout << " see " << it->get_oid() << std::endl;
	if (it->get_oid() == string(obj.c_str())) {
	  found = true;
	  break;
	}
	++it;
      }
      if (!found)
	break;
    }

    ++attempt;
    ASSERT_LE(attempt, 20);
    cout << "hrm, object is present in cache on attempt " << attempt
	 << ", retrying" << std::endl;
  }

  // Read until the object is present in the cache tier
  cout << "verifying " << obj << " is eventually promoted" << std::endl;
  while (true) {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read(obj.c_str(), bl, 1, 0));

    bool there = false;
    NObjectIterator it = cache_ioctx.nobjects_begin();
    while (it != cache_ioctx.nobjects_end()) {
      if (it->get_oid() == string(obj.c_str())) {
	there = true;
	break;
      }
      ++it;
    }
    if (there)
      break;

    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, ProxyRead) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"readproxy\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // Verify 10 times the object is NOT present in the cache tier
  uint32_t i = 0;
  while (i++ < 10) {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, CachePin) {
  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("baz", &op));
  }
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bam", &op));
  }

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // read, trigger promote
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));
    ASSERT_EQ(1, ioctx.read("bam", bl, 1, 0));
  }

  // verify the objects are present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    for (uint32_t i = 0; i < 4; i++) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("bar") ||
                  it->get_oid() == string("baz") ||
                  it->get_oid() == string("bam"));
      ++it;
    }
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  // pin objects
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("baz", completion, &op));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // enable agent
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_count", 2),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_period", 600),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "min_read_recency_for_promote", 1),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "target_max_objects", 1),
    inbl, NULL, NULL));

  sleep(10);

  // Verify the pinned object 'foo' is not flushed/evicted
  uint32_t count = 0;
  while (true) {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("baz", bl, 1, 0));

    count = 0;
    NObjectIterator it = cache_ioctx.nobjects_begin();
    while (it != cache_ioctx.nobjects_end()) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("bar") ||
                  it->get_oid() == string("baz") ||
                  it->get_oid() == string("bam"));
      ++count;
      ++it;
    }
    if (count == 2) {
      ASSERT_TRUE(it->get_oid() == string("foo") ||
                  it->get_oid() == string("baz"));
      break;
    }

    sleep(1);
  }

  // tear down tiers
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove-overlay\", \"pool\": \"" + pool_name +
    "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, EvictWatchedObject) {
  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("hi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // verify the object is present in the cache tier
  {
    NObjectIterator it = cache_ioctx.nobjects_begin();
    ASSERT_TRUE(it != cache_ioctx.nobjects_end());
    ASSERT_TRUE(it->get_oid() == string("foo"));
    ++it;
    ASSERT_TRUE(it == cache_ioctx.nobjects_end());
  }

  notify_oid = "foo";
  notify_ioctx = &cache_ioctx;
  notify_cookies.clear();
  notify_err = 0;
  uint64_t handle;
  WatchNotifyTestCtx2 ctx;
  ASSERT_EQ(0, ioctx.watch2(notify_oid, &handle, &ctx));
  ASSERT_GT(ioctx.watch_check(handle), 0);

  // flush
  {
    ObjectReadOperation op;
    op.cache_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // evict
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
	 "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_safe();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  int left = 300;
  std::cout << "waiting up to " << left << " for disconnect notification ..."
	    << std::endl;
  while (notify_err == 0 && --left) {
    sleep(1);
  }
  ASSERT_TRUE(left > 0);
  ASSERT_EQ(-ENOTCONN, notify_err);
  ioctx.unwatch2(handle);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  vector<const char*> args;
  argv_to_vec(argc, (const char **)argv, args);
  env_to_vec(args),

  global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
  common_init_finish(g_ceph_context);

  return RUN_ALL_TESTS();
}

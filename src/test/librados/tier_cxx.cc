// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "gtest/gtest.h"

#include "mds/mdstypes.h"
#include "include/buffer.h"
#include "include/rbd_types.h"
#include "include/rados/librados.hpp"
#include "include/stringify.h"
#include "include/types.h"
#include "global/global_context.h"
#include "common/Cond.h"
#include "common/ceph_crypto.h"
#include "test/librados/test_cxx.h"
#include "test/librados/testcase_cxx.h"
#include "json_spirit/json_spirit.h"
#include "cls/cas/cls_cas_ops.h"
#include "cls/cas/cls_cas_internal.h"

#include "osd/HitSet.h"

#include <errno.h>
#include <map>
#include <sstream>
#include <string>

#include "cls/cas/cls_cas_client.h"
#include "cls/cas/cls_cas_internal.h"
#include "crimson_utils.h"

using namespace std;
using namespace librados;
using ceph::crypto::SHA1;

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
      completion->wait_for_complete();
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
      completion->wait_for_complete();
      completion->get_return_value();
      completion->release();
    }
  }
}

static string _get_required_osd_release(Rados& cluster)
{
  bufferlist inbl;
  string cmd = string("{\"prefix\": \"osd dump\",\"format\":\"json\"}");
  bufferlist outbl;
  int r = cluster.mon_command(cmd, inbl, &outbl, NULL);
  ceph_assert(r >= 0);
  string outstr(outbl.c_str(), outbl.length());
  json_spirit::Value v;
  if (!json_spirit::read(outstr, v)) {
    cerr <<" unable to parse json " << outstr << std::endl;
    return "";
  }

  json_spirit::Object& o = v.get_obj();
  for (json_spirit::Object::size_type i=0; i<o.size(); i++) {
    json_spirit::Pair& p = o[i];
    if (p.name_ == "require_osd_release") {
      cout << "require_osd_release = " << p.value_.get_str() << std::endl;
      return p.value_.get_str();
    }
  }
  cerr << "didn't find require_osd_release in " << outstr << std::endl;
  return "";
}

void manifest_set_chunk(Rados& cluster, librados::IoCtx& src_ioctx, 
			librados::IoCtx& tgt_ioctx,
			uint64_t src_offset, uint64_t length, 
			std::string src_oid, std::string tgt_oid)
{
  ObjectReadOperation op;
  op.set_chunk(src_offset, length, src_ioctx, src_oid, 0,
      CEPH_OSD_OP_FLAG_WITH_REFERENCE);
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ASSERT_EQ(0, tgt_ioctx.aio_operate(tgt_oid, completion, &op,
	    librados::OPERATION_IGNORE_CACHE, NULL));
  completion->wait_for_complete();
  ASSERT_EQ(0, completion->get_return_value());
  completion->release();
}

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

void check_fp_oid_refcount(librados::IoCtx& ioctx, std::string foid, uint64_t count,
			   std::string fp_algo = std::string{})
{
  bufferlist t;
  int size = foid.length();
  if (fp_algo == "sha1") {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    sha1_gen.Update((const unsigned char *)foid.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
  } else if (fp_algo.empty()) {
    ioctx.getxattr(foid, CHUNK_REFCOUNT_ATTR, t);
  } else if (!fp_algo.empty()) {
    ceph_assert(0 == "unrecognized fingerprint algorithm");
  }

  chunk_refs_t refs;
  try {
    auto iter = t.cbegin();
    decode(refs, iter);
  } catch (buffer::error& err) {
    ASSERT_TRUE(0);
  }
  ASSERT_LE(count, refs.count());
}

string get_fp_oid(string oid, std::string fp_algo = std::string{})
{
  if (fp_algo == "sha1") {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = oid.length();
    sha1_gen.Update((const unsigned char *)oid.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    return string(p_str);
  } 
 
  return string();
}

void is_intended_refcount_state(librados::IoCtx& src_ioctx,
				std::string src_oid,
				librados::IoCtx& dst_ioctx,
				std::string dst_oid,
				int expected_refcount)
{
  int src_refcount = 0, dst_refcount = 0;
  bufferlist t;
  int r = dst_ioctx.getxattr(dst_oid, CHUNK_REFCOUNT_ATTR, t);
  if (r == -ENOENT) {
    dst_refcount = 0;
  } else {
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ceph_assert(0);
    }
    dst_refcount = refs.count();
  }
  int tries = 0;
  for (; tries < 30; ++tries) {
    r = cls_cas_references_chunk(src_ioctx, src_oid, dst_oid);
    if (r == -ENOENT || r == -ENOLINK) {
      src_refcount = 0;
    } else if (r == -EBUSY) {
      sleep(20);
      continue;
    } else {
      src_refcount = r;
    }
    break;
  }
  ASSERT_TRUE(tries < 30);
  ASSERT_TRUE(src_refcount >= 0);
  ASSERT_TRUE(src_refcount == expected_refcount);
  ASSERT_TRUE(src_refcount <= dst_refcount);
}

class LibRadosTwoPoolsPP : public RadosTestPP
{
public:
  LibRadosTwoPoolsPP() {};
  ~LibRadosTwoPoolsPP() override {};
protected:
  static void SetUpTestCase() {
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_pool_pp(pool_name, s_cluster));
  }
  static void TearDownTestCase() {
    ASSERT_EQ(0, destroy_one_pool_pp(pool_name, s_cluster));
  }
  static std::string cache_pool_name;

  void SetUp() override {
    SKIP_IF_CRIMSON();
    cache_pool_name = get_temp_pool_name();
    ASSERT_EQ(0, s_cluster.pool_create(cache_pool_name.c_str()));
    RadosTestPP::SetUp();

    ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
    cache_ioctx.application_enable("rados", true);
    cache_ioctx.set_namespace(nspace);
  }
  void TearDown() override {
    SKIP_IF_CRIMSON();
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

class Completions
{
public:
  Completions() = default;
  librados::AioCompletion* getCompletion() {
    librados::AioCompletion* comp = librados::Rados::aio_create_completion();
    m_completions.push_back(comp);
    return comp;
  }

  ~Completions() {
    for (auto& comp : m_completions) {
      comp->release();
    }
  }

private:
  vector<librados::AioCompletion *> m_completions;
};

Completions completions;

std::string LibRadosTwoPoolsPP::cache_pool_name;

TEST_F(LibRadosTierPP, Dirty) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
    ASSERT_EQ('b', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsPP, Promote) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
      do {
	ostringstream ss;
	ss << "{\"prefix\": \"pg scrub\", \"pgid\": \""
	   << cache_ioctx.get_id() << "." << i
	   << "\"}";
	int r = cluster.mon_command(ss.str(), inbl, NULL, NULL);
	if (r == -ENOENT ||  // in case mgr osdmap is stale
	    r == -EAGAIN) {
	  sleep(5);
	  continue;
	}
      } while (false);
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
  SKIP_IF_CRIMSON();
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

  // read foo snap.  the OSD may or may not realize that this snap has
  // been logically deleted; either response is valid.
  {
    bufferlist bl;
    int r = ioctx.read("foo", bl, 1, 0);
    ASSERT_TRUE(r == 1 || r == -ENOENT);
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsPP, Whiteout) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
}

TEST_F(LibRadosTwoPoolsPP, EvictSnap) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

// this test case reproduces http://tracker.ceph.com/issues/8629
TEST_F(LibRadosTwoPoolsPP, EvictSnap2) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }
}

//This test case reproduces http://tracker.ceph.com/issues/17445
TEST_F(LibRadosTwoPoolsPP, ListSnap){
  SKIP_IF_CRIMSON();
  // Create object
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

  // Create a snapshot, clone
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

  // Configure cache
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

  // Wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // Read, trigger a promote on the head
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('c', bl[0]);
  }

  // Read foo snap
  ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('h', bl[0]);
  }

  // Evict foo snap
  {
    ObjectReadOperation op;
    op.cache_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // Snap is gone...
  {
    bufferlist bl;
    ObjectReadOperation op;
    op.read(1, 0, &bl, NULL);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }

  // Do list-snaps
  ioctx.snap_set_read(CEPH_SNAPDIR);
  {
    snap_set_t snap_set;
    int snap_ret;
    ObjectReadOperation op;
    op.list_snaps(&snap_set, &snap_ret);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      0, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, snap_ret);
    ASSERT_LT(0u, snap_set.clones.size());
    for (vector<librados::clone_info_t>::const_iterator r = snap_set.clones.begin();
	r != snap_set.clones.end();
	++r) {
      if (r->cloneid != librados::SNAP_HEAD) {
	ASSERT_LT(0u, r->snaps.size());
      }
    }
  }

  // Cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

// This test case reproduces https://tracker.ceph.com/issues/49409
TEST_F(LibRadosTwoPoolsPP, EvictSnapRollbackReadRace) {
  SKIP_IF_CRIMSON();
  // create object
  {
    bufferlist bl;
    int len = string("hi there").length() * 2;
    // append more chrunk data make sure the second promote
    // op coming before the first promote op finished
    for (int i=0; i<4*1024*1024/len; ++i)
      bl.append("hi therehi there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // create two snapshot, a clone
  vector<uint64_t> my_snaps(2);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[1]));
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

  // try more times
  int retries = 50;
  for (int i=0; i<retries; ++i)
  {
    {
      librados::AioCompletion * completion = cluster.aio_create_completion();
      librados::AioCompletion * completion1 = cluster.aio_create_completion();

      // send a snap rollback op and a snap read op parallel
      // trigger two promote(copy) to the same snap clone obj
      // the second snap read op is read-ordered make sure
      // op not wait for objects_blocked_on_snap_promotion
      ObjectWriteOperation op;
      op.selfmanaged_snap_rollback(my_snaps[0]);
      ASSERT_EQ(0, ioctx.aio_operate(
        "foo", completion, &op));

      ioctx.snap_set_read(my_snaps[1]);
      std::map<uint64_t, uint64_t> extents;
      bufferlist read_bl;
      int rval = -1;
      ObjectReadOperation op1;
      op1.sparse_read(0, 8, &extents, &read_bl, &rval);
      ASSERT_EQ(0, ioctx.aio_operate("foo", completion1, &op1, &read_bl));
      ioctx.snap_set_read(librados::SNAP_HEAD);

      completion->wait_for_complete();
      ASSERT_EQ(0, completion->get_return_value());
      completion->release();

      completion1->wait_for_complete();
      ASSERT_EQ(0, completion1->get_return_value());
      completion1->release();
    }

    // evict foo snap
    ioctx.snap_set_read(my_snaps[0]);
    {
      ObjectReadOperation op;
      op.cache_evict();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, ioctx.aio_operate(
        "foo", completion, &op,
        librados::OPERATION_IGNORE_CACHE, NULL));
      completion->wait_for_complete();
      ASSERT_EQ(0, completion->get_return_value());
      completion->release();
    }
    ioctx.snap_set_read(librados::SNAP_HEAD);
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
  ioctx.selfmanaged_snap_remove(my_snaps[1]);
}

TEST_F(LibRadosTwoPoolsPP, TryFlush) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  cache_ioctx.application_enable("rados", true);
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

      completion->wait_for_complete();
      completion2->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

      completion->wait_for_complete();
      completion2->wait_for_complete();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }
}


IoCtx *read_ioctx = 0;
ceph::mutex test_lock = ceph::make_mutex("FlushReadRaces::lock");
ceph::condition_variable cond;
int max_reads = 100;
int num_reads = 0; // in progress

void flush_read_race_cb(completion_t cb, void *arg);

void start_flush_read()
{
  //cout << " starting read" << std::endl;
  ObjectReadOperation op;
  op.stat(NULL, NULL, NULL);
  librados::AioCompletion *completion = completions.getCompletion();
  completion->set_complete_callback(0, flush_read_race_cb);
  read_ioctx->aio_operate("foo", completion, &op, NULL);
}

void flush_read_race_cb(completion_t cb, void *arg)
{
  //cout << " finished read" << std::endl;
  std::lock_guard l{test_lock};
  if (num_reads > max_reads) {
    num_reads--;
    cond.notify_all();
  } else {
    start_flush_read();
  }
}

TEST_F(LibRadosTwoPoolsPP, TryFlushReadRace) {
  SKIP_IF_CRIMSON();
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
  test_lock.lock();
  for (int i = 0; i < max_reads; ++i) {
    start_flush_read();
    num_reads++;
  }
  test_lock.unlock();

  // try-flush
  ObjectReadOperation op;
  op.cache_try_flush();
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

  completion->wait_for_complete();
  ASSERT_EQ(0, completion->get_return_value());
  completion->release();

  // stop reads
  std::unique_lock locker{test_lock};
  max_reads = 0;  
  cond.wait(locker, [] { return num_reads == 0;});
}

TEST_F(LibRadosTierPP, HitSetNone) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  utime_t start = ceph_clock_now();
  utime_t hard_stop = start + utime_t(600, 0);

  while (true) {
    utime_t now = ceph_clock_now();
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
      auto p = hbl.cbegin();
      HitSet hs;
      decode(hs, p);
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
  ceph_assert(r >= 0);
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

int make_hitset(Rados& cluster, librados::IoCtx& cache_ioctx, int num_pg, 
    int num, std::map<int, HitSet>& hitsets, std::string& cache_pool_name) 
{
  int pg = num_pg;
  // do a bunch of writes
  for (int i=0; i<num; ++i) {
    bufferlist bl;
    bl.append("a");
    ceph_assert(0 == cache_ioctx.write(stringify(i), bl, 1, 0));
  }

  // get HitSets
  for (int i=0; i<pg; ++i) {
    list< pair<time_t,time_t> > ls;
    AioCompletion *c = librados::Rados::aio_create_completion();
    ceph_assert(0 == cache_ioctx.hit_set_list(i, c, &ls));
    c->wait_for_complete();
    c->release();
    std::cout << "pg " << i << " ls " << ls << std::endl;
    ceph_assert(!ls.empty());

    // get the latest
    c = librados::Rados::aio_create_completion();
    bufferlist bl;
    ceph_assert(0 == cache_ioctx.hit_set_get(i, c, ls.back().first, &bl));
    c->wait_for_complete();
    c->release();

    try {
      auto p = bl.cbegin();
      decode(hitsets[i], p);
    }
    catch (buffer::error& e) {
      std::cout << "failed to decode hit set; bl len is " << bl.length() << "\n";
      bl.hexdump(std::cout);
      std::cout << std::endl;
      throw e;
    }

    // cope with racing splits by refreshing pg_num
    if (i == pg - 1)
      pg = _get_pg_num(cluster, cache_pool_name);
  }
  return pg;
}

TEST_F(LibRadosTwoPoolsPP, HitSetWrite) {
  SKIP_IF_CRIMSON();
  int num_pg = _get_pg_num(cluster, pool_name);
  ceph_assert(num_pg > 0);

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

  std::map<int,HitSet> hitsets;

  num_pg = make_hitset(cluster, cache_ioctx, num_pg, num, hitsets, cache_pool_name);

  int retry = 0;

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
    if (!found && retry < 5) {
      num_pg = make_hitset(cluster, cache_ioctx, num_pg, num, hitsets, cache_pool_name);
      i--;
      retry++;
      continue;
    }
    ASSERT_TRUE(found);
  }
}

TEST_F(LibRadosTwoPoolsPP, HitSetTrim) {
  SKIP_IF_CRIMSON();
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
  utime_t start = ceph_clock_now();
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

    utime_t now = ceph_clock_now();
    ASSERT_TRUE(now < hard_stop);

    sleep(1);
  }
}

TEST_F(LibRadosTwoPoolsPP, PromoteOn2ndRead) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("baz", completion, &op));
    completion->wait_for_complete();
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

TEST_F(LibRadosTwoPoolsPP, SetRedirectRead) {
  SKIP_IF_CRIMSON();
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
    bl.append("there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('t', bl[0]);
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ManifestPromoteRead) {
  SKIP_IF_CRIMSON();
  // skip test if not yet mimic
  if (_get_required_osd_release(cluster) < "mimic") {
    GTEST_SKIP() << "cluster is not yet mimic, skipping test";
  }

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
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo-chunk", &op));
  }
  {
    bufferlist bl;
    bl.append("there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("CHUNK");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar-chunk", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-redirect
  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // set-chunk
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 2, "bar-chunk", "foo-chunk");

  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object (redirect)
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('t', bl[0]);
  }
  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo-chunk", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo-chunk", bl, 1, 0));
    ASSERT_EQ('C', bl[0]);
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ManifestRefRead) {
  SKIP_IF_CRIMSON();
  // note: require >= mimic

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
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo-chunk", &op));
  }
  {
    bufferlist bl;
    bl.append("there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("CHUNK");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar-chunk", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-redirect
  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // set-chunk
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, cache_ioctx, "bar-chunk", 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo-chunk", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // redirect's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar", CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_EQ(1U, refs.count());
  }
  // chunk's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar-chunk", CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_EQ(1u, refs.count());
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ManifestUnset) {
  SKIP_IF_CRIMSON();
  // skip test if not yet nautilus
  if (_get_required_osd_release(cluster) < "nautilus") {
    GTEST_SKIP() << "cluster is not yet nautilus, skipping test";
  }

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
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo-chunk", &op));
  }
  {
    bufferlist bl;
    bl.append("there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("CHUNK");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar-chunk", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-redirect
  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // set-chunk
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, cache_ioctx, "bar-chunk", 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo-chunk", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // redirect's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar", CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_EQ(1u, refs.count());
  }
  // chunk's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar-chunk", CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_EQ(1u, refs.count());
  }

  // unset-manifest for set-redirect
  {
    ObjectWriteOperation op;
    op.unset_manifest();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // unset-manifest for set-chunk
  {
    ObjectWriteOperation op;
    op.unset_manifest();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo-chunk", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // redirect's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar-chunk", CHUNK_REFCOUNT_ATTR, t);
    if (t.length() != 0U) {
      ObjectWriteOperation op;
      op.unset_manifest();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
      completion->wait_for_complete();
      ASSERT_EQ(-EOPNOTSUPP, completion->get_return_value());
      completion->release();
    }
  }
  // chunk's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr("bar-chunk", CHUNK_REFCOUNT_ATTR, t);
    if (t.length() != 0U) {
      ObjectWriteOperation op;
      op.unset_manifest();
      librados::AioCompletion *completion = cluster.aio_create_completion();
      ASSERT_EQ(0, ioctx.aio_operate("foo-chunk", completion, &op));
      completion->wait_for_complete();
      ASSERT_EQ(-EOPNOTSUPP, completion->get_return_value());
      completion->release();
    }
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ManifestDedupRefRead) {
  SKIP_IF_CRIMSON();
  // skip test if not yet nautilus
  if (_get_required_osd_release(cluster) < "nautilus") {
    GTEST_SKIP() << "cluster is not yet nautilus, skipping test";
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	    set_pool_str(pool_name, "fingerprint_algorithm", "sha1"),
	    inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();
  string tgt_oid;

  // get fp_oid
  tgt_oid = get_fp_oid("There hi", "sha1");

  // create object
  {
    bufferlist bl;
    bl.append("There hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("There hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo-dedup", &op));
  }

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("There hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(tgt_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 8, tgt_oid, "foo-dedup");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 8, tgt_oid, "foo");
  // chunk's refcount 
  {
    bufferlist t;
    cache_ioctx.getxattr(tgt_oid, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsPP, ManifestSnapRefcount) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("there hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  string er_fp_oid, hi_fp_oid, bb_fp_oid;

  // get fp_oid
  er_fp_oid = get_fp_oid("er", "sha1");
  hi_fp_oid = get_fp_oid("hi", "sha1");
  bb_fp_oid = get_fp_oid("bb", "sha1");

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("er");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(er_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(hi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("bb");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(bb_fp_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, er_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, hi_fp_oid, "foo");

  // make all chunks dirty --> flush
  // foo: [er] [hi]

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("er");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"er", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(1u, refs.count());
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // foo: [bb] [hi]
  // create a clone
  {
    bufferlist bl;
    bl.append("Thbbe");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // make clean
  {
    bufferlist bl;
    bl.append("Thbbe");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, bb_fp_oid, "foo");

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // foo: [er] [hi]
  // create a clone
  {
    bufferlist bl;
    bl.append("There");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // make clean
  {
    bufferlist bl;
    bl.append("There");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, er_fp_oid, "foo");

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("er");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"er", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }
  
  // and another
  my_snaps.resize(3);
  my_snaps[2] = my_snaps[1];
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // foo: [bb] [hi]
  // create a clone
  {
    bufferlist bl;
    bl.append("Thbbe");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // make clean
  {
    bufferlist bl;
    bl.append("Thbbe");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, bb_fp_oid, "foo");

  /*
   *  snap[2]: [er] [hi]
   *  snap[1]: [bb] [hi]
   *  snap[0]: [er] [hi]
   *  head:    [bb] [hi]
   */

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("hi");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"hi", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 1);
  }

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("er");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"er", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }

  // remove snap
  ioctx.selfmanaged_snap_remove(my_snaps[2]);

  /*
   *  snap[1]: [bb] [hi]
   *  snap[0]: [er] [hi]
   *  head:    [bb] [hi]
   */

  sleep(10);

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("hi");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"hi", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 1);
  }

  // remove snap
  ioctx.selfmanaged_snap_remove(my_snaps[0]);

  /*
   *  snap[1]: [bb] [hi]
   *  head:    [bb] [hi]
   */

  sleep(10);

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("bb");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"bb", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 1);
  }

  // remove snap
  ioctx.selfmanaged_snap_remove(my_snaps[1]);

  /*
   *  snap[1]: [bb] [hi]
   */

  sleep(10);

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("bb");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"bb", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 1);
  }

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("hi");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"hi", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 1);
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestSnapRefcount2) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("Thabe cdHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  string ab_fp_oid, cd_fp_oid, ef_fp_oid, BB_fp_oid;

  // get fp_oid
  ab_fp_oid = get_fp_oid("ab", "sha1");
  cd_fp_oid = get_fp_oid("cd", "sha1");
  ef_fp_oid = get_fp_oid("ef", "sha1");
  BB_fp_oid = get_fp_oid("BB", "sha1");

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ab");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ab_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("cd");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(cd_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ef");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ef_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("BB");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(BB_fp_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, ab_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, cd_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, ef_fp_oid, "foo");


  // make all chunks dirty --> flush
  // foo: [ab] [cd] [ef]

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // foo: [BB] [BB] [ef]
  // create a clone
  {
    bufferlist bl;
    bl.append("ThBBe BB");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // make clean
  {
    bufferlist bl;
    bl.append("ThBBe BB");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, BB_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, BB_fp_oid, "foo");

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // foo: [ab] [cd] [ef]
  // create a clone
  {
    bufferlist bl;
    bl.append("Thabe cd");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // make clean
  {
    bufferlist bl;
    bl.append("Thabe cd");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, ab_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, cd_fp_oid, "foo");

  /*
   *  snap[1]: [ab] [cd] [ef]
   *  snap[0]: [BB] [BB] [ef]
   *  head:    [ab] [cd] [ef]
   */

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("ab");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"ab", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("cd");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"cd", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("BB");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"BB", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    cache_ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(2u, refs.count());
  }

  // remove snap
  ioctx.selfmanaged_snap_remove(my_snaps[0]);

  /*
   *  snap[1]: [ab] [cd] [ef]
   *  head:    [ab] [cd] [ef]
   */

  sleep(10);

  // check chunk's refcount
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("BB");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"BB", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 0);
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestTestSnapCreate) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    GTEST_SKIP() << "cluster is not yet octopus, skipping test";
  }

  // create object
  {
    bufferlist bl;
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("CHUNKS CHUNKS");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  string ba_fp_oid, se_fp_oid, ch_fp_oid;

  // get fp_oid
  ba_fp_oid = get_fp_oid("ba", "sha1");
  se_fp_oid = get_fp_oid("se", "sha1");
  ch_fp_oid = get_fp_oid("ch", "sha1");

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ba");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ba_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("se");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(se_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ch");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ch_fp_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 2, ba_fp_oid, "foo");

  // try to create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
							 my_snaps));

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, se_fp_oid, "foo");

  // check whether clone is created
  ioctx.snap_set_read(librados::SNAP_DIR);
  {
    snap_set_t snap_set;
    int snap_ret;
    ObjectReadOperation op;
    op.list_snaps(&snap_set, &snap_ret);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
      "foo", completion, &op,
      0, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, snap_ret);
    ASSERT_LT(0u, snap_set.clones.size());
    ASSERT_EQ(1, snap_set.clones.size());
  }

  // create a clone
  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    bl.append("B");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 0));
  }

  ioctx.snap_set_read(my_snaps[0]);
  // set-chunk to clone
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, ch_fp_oid, "foo");
}

TEST_F(LibRadosTwoPoolsPP, ManifestRedirectAfterPromote) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus 
  if (_get_required_osd_release(cluster) < "octopus") {
    GTEST_SKIP() << "cluster is not yet octopus, skipping test";
  }

  // create object
  {
    bufferlist bl;
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("BASE CHUNK");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  // set-redirect
  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // write
  {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 0));
  }

  // read and verify the object (redirect)
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('a', bl[0]);
  }

  // read and verify the object (redirect)
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("bar", bl, 1, 0));
    ASSERT_EQ('B', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestCheckRefcountWhenModification) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus 
  if (_get_required_osd_release(cluster) < "octopus") {
    GTEST_SKIP() << "cluster is not yet octopus, skipping test";
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  string er_fp_oid, hi_fp_oid, HI_fp_oid, ai_fp_oid, bi_fp_oid,
	  Er_fp_oid, Hi_fp_oid, Si_fp_oid;

  // get fp_oid
  er_fp_oid = get_fp_oid("er", "sha1");
  hi_fp_oid = get_fp_oid("hi", "sha1");
  HI_fp_oid = get_fp_oid("HI", "sha1");
  ai_fp_oid = get_fp_oid("ai", "sha1");
  bi_fp_oid = get_fp_oid("bi", "sha1");
  Er_fp_oid = get_fp_oid("Er", "sha1");
  Hi_fp_oid = get_fp_oid("Hi", "sha1");
  Si_fp_oid = get_fp_oid("Si", "sha1");

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("er");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(er_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(hi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("HI");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(HI_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ai");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ai_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("bi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(bi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("Er");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(Er_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("Hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(Hi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("Si");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(Si_fp_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, er_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, hi_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, HI_fp_oid, "foo");

  // foo head: [er] [hi] [HI]
  
  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));


  // foo snap[0]: [er] [hi] [HI]
  // foo head   : [er] [ai] [HI]
  // create a clone
  {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }
  // write
  {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, ai_fp_oid, "foo");

  // foo snap[0]: [er] [hi] [HI]
  // foo head   : [er] [bi] [HI]
  // create a clone
  {
    bufferlist bl;
    bl.append("b");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }
  // write
  {
    bufferlist bl;
    bl.append("b");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, bi_fp_oid, "foo");
  
  sleep(10);

  // check chunk's refcount
  // [ai]'s refcount should be 0
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("ai");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"ai", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 0);
  }

  // foo snap[0]: [er] [hi] [HI]
  // foo head   : [Er] [Hi] [Si]
  // create a clone
  {
    bufferlist bl;
    bl.append("thEre HiSi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  // write
  {
    bufferlist bl;
    bl.append("thEre HiSi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, Er_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, Hi_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, Si_fp_oid, "foo");

  // foo snap[0]: [er] [hi] [HI]
  // foo head   : [ER] [HI] [SI]
  // write
  {
    bufferlist bl;
    bl.append("thERe HISI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  sleep(10);

  // check chunk's refcount
  // [Er]'s refcount should be 0
  {
    bufferlist t;
    SHA1 sha1_gen;
    int size = strlen("Er");
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1];
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    sha1_gen.Update((const unsigned char *)"Er", size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(ioctx, "foo", cache_ioctx, p_str, 0);
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestSnapIncCount) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk4", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk4", "foo");
  // foo snap[1]: 
  // foo snap[0]:  
  // foo head   : [chunk1]          [chunk4]

  ioctx.snap_set_read(my_snaps[1]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, "chunk2", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk4", "foo");
  // foo snap[1]:          [chunk2] [chunk4]
  // foo snap[0]:      
  // foo head   : [chunk1]          [chunk4]
 
  ioctx.snap_set_read(my_snaps[0]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, "chunk2", "foo");
  // foo snap[1]:          [chunk2] [chunk4]
  // foo snap[0]:          [chunk2]
  // foo head   : [chunk1]          [chunk4]
  
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk3", "foo");
  // foo snap[1]:          [chunk2] [chunk4]
  // foo snap[0]: [chunk3] [chunk2]
  // foo head   : [chunk1]          [chunk4]
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk4", "foo");
  // foo snap[1]:          [chunk2] [chunk4]
  // foo snap[0]: [chunk3] [chunk2] [chunk4]
  // foo head   : [chunk1]          [chunk4]

  // check chunk's refcount
  check_fp_oid_refcount(cache_ioctx, "chunk1", 1u, "");

  // check chunk's refcount
  check_fp_oid_refcount(cache_ioctx, "chunk2", 1u, "");

  // check chunk's refcount
  check_fp_oid_refcount(cache_ioctx, "chunk3", 1u, "");
  sleep(10);

  // check chunk's refcount
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk4", 1);
}

TEST_F(LibRadosTwoPoolsPP, ManifestEvict) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk4", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk4", "foo");
  // foo snap[1]: 
  // foo snap[0]:  
  // foo head   :          [chunk1]                    [chunk4]

  ioctx.snap_set_read(my_snaps[1]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 10, "chunk2", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:      
  // foo head   :          [chunk1]                    [chunk4]
 
  ioctx.snap_set_read(my_snaps[0]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, "chunk2", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:                             [chunk2]
  // foo head   :           [chunk1]                   [chunk4]
  
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk3", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:	    [chunk3]          [chunk2]
  // foo head   :	    [chunk1]                   [chunk4]
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk4", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:	    [chunk3]          [chunk2] [chunk4]
  // foo head   :	    [chunk1]                   [chunk4]
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 2, "chunk4", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]: [chunk4] [chunk3]           [chunk2] [chunk4]
  // foo head   :          [chunk1]                    [chunk4]
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 4, 2, "chunk1", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]: [chunk4] [chunk3] [chunk1]  [chunk2] [chunk4]
  // foo head   :          [chunk1]                    [chunk4]

  {
    ObjectReadOperation op, stat_op;
    uint64_t size;
    op.tier_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());

    stat_op.stat(&size, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("foo", &stat_op, NULL));
    ASSERT_EQ(10, size);
  }

  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op, stat_op;
    uint64_t size;
    op.tier_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());

    stat_op.stat(&size, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("foo", &stat_op, NULL));
    ASSERT_EQ(strlen("there hiHI"), size);
  }

}

TEST_F(LibRadosTwoPoolsPP, ManifestEvictPromote) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("EREHT hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("THERE HIHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk2", "foo");
  // foo snap[0]:  
  // foo head   :  [chunk1]                           [chunk2]

  ioctx.snap_set_read(my_snaps[0]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 10, "chunk3", "foo");
  // foo snap[0]: [                  chunk3                   ] 
  // foo head   : [chunk1]                             [chunk2]
 

  {
    ObjectReadOperation op, stat_op;
    uint64_t size;
    op.tier_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());

    stat_op.stat(&size, NULL, NULL);
    ASSERT_EQ(0, ioctx.operate("foo", &stat_op, NULL));
    ASSERT_EQ(10, size);
    
  }
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('T', bl[0]);
  }

  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(10, ioctx.read("foo", bl, 10, 0));
    ASSERT_EQ('H', bl[8]);
  }
}


TEST_F(LibRadosTwoPoolsPP, ManifestSnapSizeMismatch) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("there HIHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("chunk2", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
       my_snaps));

  {
    bufferlist bl;
    bl.append("There hiHI");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
       my_snaps));

  {
    bufferlist bl;
    bl.append("tHere hiHI");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 0, 10, "chunk1", "foo");

  cache_ioctx.snap_set_read(my_snaps[1]);

  // set-chunk 
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 0, 10, "chunk2", "foo");

  // evict
  {
    ObjectReadOperation op, stat_op;
    op.tier_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
       "foo", completion, &op,
       librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
  }

  uint32_t hash;
  ASSERT_EQ(0, cache_ioctx.get_object_pg_hash_position2("foo", &hash));

  // scrub
  {
    for (int tries = 0; tries < 5; ++tries) {
      bufferlist inbl;
      ostringstream ss;
      ss << "{\"prefix\": \"pg deep-scrub\", \"pgid\": \""
        << cache_ioctx.get_id() << "."
        << std::hex << hash
        << "\"}";
      int r = cluster.mon_command(ss.str(), inbl, NULL, NULL);
      if (r == -ENOENT ||  
         r == -EAGAIN) {
       sleep(5);
       continue;
      }
      ASSERT_EQ(0, r);
      break;
    }
    cout << "waiting for scrubs..." << std::endl;
    sleep(20);
    cout << "done waiting" << std::endl;
  }

  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('t', bl[0]);
  }
}

#include <common/CDC.h>
TEST_F(LibRadosTwoPoolsPP, DedupFlushRead) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    GTEST_SKIP() << "cluster is not yet octopus, skipping test";
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_tier", pool_name),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_chunk_algorithm", "fastcdc"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
	inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  bufferlist gbl;
  {
    generate_buffer(1024*8, &gbl);
    ObjectWriteOperation op;
    op.write_full(gbl);
    ASSERT_EQ(0, cache_ioctx.operate("foo-chunk", &op));
  }
  {
    bufferlist bl;
    bl.append("DDse chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar-chunk", &op));
  }

  // set-chunk to set manifest object
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, ioctx, "bar-chunk", 0,
		CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo-chunk", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo-chunk", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  std::unique_ptr<CDC> cdc = CDC::create("fastcdc", cbits(1024)-1);
  vector<pair<uint64_t, uint64_t>> chunks;
  bufferlist chunk;
  cdc->calc_chunks(gbl, &chunks);
  chunk.substr_of(gbl, chunks[1].first, chunks[1].second);
  string tgt_oid;
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = chunk.length();
    sha1_gen.Update((const unsigned char *)chunk.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
  }

  // read and verify the chunked object
  {
    bufferlist test_bl;
    ASSERT_EQ(2, ioctx.read(tgt_oid, test_bl, 2, 0));
    ASSERT_EQ(test_bl[1], chunk[1]);
  }

  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 512),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("hi");
    ASSERT_EQ(0, cache_ioctx.write("foo-chunk", bl, bl.length(), 0));
  }

  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo-chunk", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  cdc = CDC::create("fastcdc", cbits(512)-1);
  chunks.clear();
  cdc->calc_chunks(gbl, &chunks);
  bufferlist chunk_512;
  chunk_512.substr_of(gbl, chunks[3].first, chunks[3].second);
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = chunk_512.length();
    sha1_gen.Update((const unsigned char *)chunk_512.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
  }

  // read and verify the chunked object
  {
    bufferlist test_bl;
    ASSERT_EQ(2, ioctx.read(tgt_oid, test_bl, 2, 0));
    ASSERT_EQ(test_bl[1], chunk_512[1]);
  }

  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 16384),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("hi");
    ASSERT_EQ(0, cache_ioctx.write("foo-chunk", bl, bl.length(), 0));
    gbl.begin(0).copy_in(bl.length(), bl);
  }
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo-chunk", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  cdc = CDC::create("fastcdc", cbits(16384)-1);
  chunks.clear();
  cdc->calc_chunks(gbl, &chunks);
  bufferlist chunk_16384;
  chunk_16384.substr_of(gbl, chunks[0].first, chunks[0].second);
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = chunk_16384.length();
    sha1_gen.Update((const unsigned char *)chunk_16384.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
  }
  // read and verify the chunked object
  {
    bufferlist test_bl;
    ASSERT_EQ(2, ioctx.read(tgt_oid, test_bl, 2, 0));
    ASSERT_EQ(test_bl[0], chunk_16384[0]);
  }

  // less than object size
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // make a dirty chunks
  // a chunk_info is deleted by write, which converts the manifest object to non-manifest object
  {
    bufferlist bl;
    bl.append("hi");
    ASSERT_EQ(0, cache_ioctx.write("foo-chunk", bl, bl.length(), 0));
  }

  // reset set-chunk
  {
    bufferlist bl;
    bl.append("DDse chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar-chunk", &op));
  }
  // set-chunk to set manifest object
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, ioctx, "bar-chunk", 0,
		CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo-chunk", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo-chunk", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  cdc = CDC::create("fastcdc", cbits(1024)-1);
  chunks.clear();
  cdc->calc_chunks(gbl, &chunks);
  bufferlist small_chunk;
  small_chunk.substr_of(gbl, chunks[1].first, chunks[1].second);
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = small_chunk.length();
    sha1_gen.Update((const unsigned char *)small_chunk.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
  }
  // read and verify the chunked object
  {
    bufferlist test_bl;
    ASSERT_EQ(2, ioctx.read(tgt_oid, test_bl, 2, 0));
    ASSERT_EQ(test_bl[0], small_chunk[0]);
  }

}

TEST_F(LibRadosTwoPoolsPP, ManifestFlushSnap) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_tier", pool_name),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_chunk_algorithm", "fastcdc"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
	inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create object
  bufferlist gbl;
  {
    //bufferlist bl;
    //bl.append("there hi");
    generate_buffer(1024*8, &gbl);
    ObjectWriteOperation op;
    op.write_full(gbl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 2, 2, "bar", "foo");
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 6, 2, "bar", "foo");

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("Thbbe");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("Thcce");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  // flush on head (should fail)
  cache_ioctx.snap_set_read(librados::SNAP_HEAD);
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }

  // flush on recent snap (should fail)
  cache_ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }

  // flush on oldest snap
  cache_ioctx.snap_set_read(my_snaps[1]);
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush on oldest snap
  cache_ioctx.snap_set_read(my_snaps[0]);
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush on oldest snap
  cache_ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // check chunk's refcount
  std::unique_ptr<CDC> cdc = CDC::create("fastcdc", cbits(1024)-1);
  vector<pair<uint64_t, uint64_t>> chunks;
  bufferlist chunk;
  cdc->calc_chunks(gbl, &chunks);
  chunk.substr_of(gbl, chunks[1].first, chunks[1].second);
  string tgt_oid;
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    SHA1 sha1_gen;
    int size = chunk.length();
    sha1_gen.Update((const unsigned char *)chunk.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
  }
  // read and verify the chunked object
  {
    bufferlist test_bl;
    ASSERT_EQ(2, ioctx.read(tgt_oid, test_bl, 2, 0));
    ASSERT_EQ(test_bl[1], chunk[1]);
  }

  cache_ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(4, cache_ioctx.read("foo", bl, 4, 0));
    ASSERT_EQ('c', bl[2]);
  }

  cache_ioctx.snap_set_read(my_snaps[0]);
  {
    bufferlist bl;
    ASSERT_EQ(4, cache_ioctx.read("foo", bl, 4, 0));
    ASSERT_EQ('b', bl[2]);
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestFlushDupCount) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_tier", pool_name),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_chunk_algorithm", "fastcdc"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
	inbl, NULL, NULL));

  // create object
  bufferlist gbl;
  {
    //bufferlist bl;
    generate_buffer(1024*8, &gbl);
    ObjectWriteOperation op;
    op.write_full(gbl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-chunk to set manifest object
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, ioctx, "bar", 0,
		CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("Thbbe hi");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  // and another
  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, cache_ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("Thcce hi");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  //flush on oldest snap
  cache_ioctx.snap_set_read(my_snaps[1]);
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush on oldest snap
  cache_ioctx.snap_set_read(my_snaps[0]);
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  cache_ioctx.snap_set_read(librados::SNAP_HEAD);
  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  std::unique_ptr<CDC> cdc = CDC::create("fastcdc", cbits(1024)-1);
  vector<pair<uint64_t, uint64_t>> chunks;
  bufferlist chunk;
  cdc->calc_chunks(gbl, &chunks);
  chunk.substr_of(gbl, chunks[1].first, chunks[1].second);
  string tgt_oid;
  // check chunk's refcount
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    bufferlist t;
    SHA1 sha1_gen;
    int size = chunk.length();
    sha1_gen.Update((const unsigned char *)chunk.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
    ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(1u, refs.count());
  }

  bufferlist chunk2;
  chunk2.substr_of(gbl, chunks[0].first, chunks[0].second);
  // check chunk's refcount
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    bufferlist t;
    SHA1 sha1_gen;
    int size = chunk2.length();
    sha1_gen.Update((const unsigned char *)chunk2.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    tgt_oid = string(p_str);
    ioctx.getxattr(p_str, CHUNK_REFCOUNT_ATTR, t);
    chunk_refs_t refs;
    try {
      auto iter = t.cbegin();
      decode(refs, iter);
    } catch (buffer::error& err) {
      ASSERT_TRUE(0);
    }
    ASSERT_LE(1u, refs.count());
  }

  // make a dirty chunks
  {
    bufferlist bl;
    bl.append("ThDDe hi");
    ASSERT_EQ(0, cache_ioctx.write("foo", bl, bl.length(), 0));
  }

  // flush
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  bufferlist tmp;
  tmp.append("Thcce hi");
  gbl.begin(0).copy_in(tmp.length(), tmp);
  bufferlist chunk3;
  cdc->calc_chunks(gbl, &chunks);
  chunk3.substr_of(gbl, chunks[0].first, chunks[0].second);
  // check chunk's refcount
  {
    unsigned char fingerprint[CEPH_CRYPTO_SHA1_DIGESTSIZE + 1] = {0};
    char p_str[CEPH_CRYPTO_SHA1_DIGESTSIZE*2+1] = {0};
    bufferlist t;
    SHA1 sha1_gen;
    int size = chunk2.length();
    sha1_gen.Update((const unsigned char *)chunk2.c_str(), size);
    sha1_gen.Final(fingerprint);
    buf_to_hex(fingerprint, CEPH_CRYPTO_SHA1_DIGESTSIZE, p_str);
    is_intended_refcount_state(cache_ioctx, "foo", ioctx, p_str, 0);
  }
}

TEST_F(LibRadosTwoPoolsPP, TierFlushDuringFlush) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;

  // create a new pool 
  std::string temp_pool_name = get_temp_pool_name() + "-test-flush";
  ASSERT_EQ(0, cluster.pool_create(temp_pool_name.c_str()));

  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_tier", temp_pool_name),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_chunk_algorithm", "fastcdc"),
	inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
	inbl, NULL, NULL));

  // create object
  bufferlist gbl;
  {
    //bufferlist bl;
    generate_buffer(1024*8, &gbl);
    ObjectWriteOperation op;
    op.write_full(gbl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-chunk to set manifest object
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, ioctx, "bar", 0,
		CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
	      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // delete temp pool, so flushing chunk will fail
  ASSERT_EQ(0, s_cluster.pool_delete(temp_pool_name.c_str()));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // flush to check if proper error is returned
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(-ENOENT, completion->get_return_value());
    completion->release();
  }

}

TEST_F(LibRadosTwoPoolsPP, ManifestSnapHasChunk) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
	set_pool_str(pool_name, "fingerprint_algorithm", "sha1"),
	inbl, NULL, NULL));
  cluster.wait_for_latest_osdmap();

  // create object
  {
    bufferlist bl;
    bl.append("there HIHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }

  string er_fp_oid, hi_fp_oid, HI_fp_oid, ai_fp_oid, bi_fp_oid,
	  Er_fp_oid, Hi_fp_oid, SI_fp_oid;

  // get fp_oid
  er_fp_oid = get_fp_oid("er", "sha1");
  hi_fp_oid = get_fp_oid("hi", "sha1");
  HI_fp_oid = get_fp_oid("HI", "sha1");
  ai_fp_oid = get_fp_oid("ai", "sha1");
  bi_fp_oid = get_fp_oid("bi", "sha1");
  Er_fp_oid = get_fp_oid("Er", "sha1");
  Hi_fp_oid = get_fp_oid("Hi", "sha1");
  SI_fp_oid = get_fp_oid("SI", "sha1");

  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("er");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(er_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(hi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("HI");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(HI_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("ai");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(ai_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("bi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(bi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("Er");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(Er_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("Hi");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(Hi_fp_oid, &op));
  }
  // write
  {
    ObjectWriteOperation op;
    bufferlist bl;
    bl.append("SI");
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate(SI_fp_oid, &op));
  }

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, HI_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, HI_fp_oid, "foo");

  // foo head:     [hi] [HI]
  
  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));


  // create a clone
  {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }
  // write
  {
    bufferlist bl;
    bl.append("a");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }
  // write
  {
    bufferlist bl;
    bl.append("S");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 8));
  }

  // foo snap[0]:      [hi] [HI]
  // foo head   : [er] [ai] [SI]

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, er_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, ai_fp_oid, "foo");
  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, SI_fp_oid, "foo");

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  // create a clone
  {
    bufferlist bl;
    bl.append("b");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }
  // write
  {
    bufferlist bl;
    bl.append("b");
    ASSERT_EQ(0, ioctx.write("foo", bl, 1, 6));
  }

  // foo snap[1]:      [HI] [HI]
  // foo snap[0]: [er] [ai] [SI]
  // foo head   : [er] [bi] [SI]

  // set-chunk (dedup)
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, bi_fp_oid, "foo");

  {
    ASSERT_EQ(1, cls_cas_references_chunk(ioctx, "foo", SI_fp_oid));
    ASSERT_EQ(1, cls_cas_references_chunk(ioctx, "foo", er_fp_oid));
    ASSERT_EQ(1, cls_cas_references_chunk(ioctx, "foo", ai_fp_oid));
    ASSERT_EQ(2, cls_cas_references_chunk(ioctx, "foo", HI_fp_oid));
    ASSERT_EQ(-ENOLINK, cls_cas_references_chunk(ioctx, "foo", Hi_fp_oid));
  }
}

TEST_F(LibRadosTwoPoolsPP, ManifestRollback) {
  SKIP_IF_CRIMSON();
  // skip test if not yet pacific
  if (_get_required_osd_release(cluster) < "pacific") {
    cout << "cluster is not yet pacific, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ABere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("EFere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("thABe hiEF");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk3", "foo");
  // foo snap[1]: 
  // foo snap[0]:  
  // foo head   :          [chunk1]                    [chunk3]

  ioctx.snap_set_read(my_snaps[1]);
  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 10, "chunk2", "foo");
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:      
  // foo head   :          [chunk1]                    [chunk3]
 
  // foo snap[1]: [                  chunk2                   ] 
  // foo snap[0]:                             
  // foo head   :           [chunk1]                   [chunk3]
  
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback("foo", my_snaps[0]));

  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('t', bl[0]);
  }

  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback("foo", my_snaps[1]));

  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('C', bl[0]);
  }

}

TEST_F(LibRadosTwoPoolsPP, ManifestRollbackRefcount) {
  SKIP_IF_CRIMSON();
  // skip test if not yet pacific
  if (_get_required_osd_release(cluster) < "pacific") {
    cout << "cluster is not yet pacific, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ABere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("EFere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }
  {
    bufferlist bl;
    bl.append("DDDDD hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk4", &op));
  }
  {
    bufferlist bl;
    bl.append("EEEEE hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk5", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  my_snaps.resize(2);
  my_snaps[1] = my_snaps[0];
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("thABe hiEF");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }

  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk3", "foo");
  // foo snap[1]: 
  // foo snap[0]:  
  // foo head   :          [chunk1]                    [chunk3]

  ioctx.snap_set_read(my_snaps[1]);
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk4", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 6, 2, "chunk5", "foo");
  // foo snap[1]:           [chunk4]          [chunk5]       
  // foo snap[0]:  
  // foo head   :           [chunk1]                   [chunk3]
  
  ioctx.snap_set_read(my_snaps[0]);
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 10, "chunk2", "foo");
  // foo snap[1]:           [chunk4]          [chunk5]       
  // foo snap[0]: [                  chunk2                   ] 
  // foo head   :          [chunk1]                    [chunk3]
  
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback("foo", my_snaps[1]));
  // foo snap[1]:          [chunk4]          [chunk5]       
  // foo snap[0]: [                  chunk2                   ] 
  // foo head   :          [chunk4]          [chunk5] <-- will contain these contents

  sleep(10);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk1", 0);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk3", 0);

  ioctx.selfmanaged_snap_remove(my_snaps[1]);
  sleep(10);
  // foo snap[1]:          
  // foo snap[0]: [                  chunk2                   ] 
  // foo head   :          [chunk4]          [chunk5] 
  ioctx.snap_set_read(librados::SNAP_HEAD);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk4", 1);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk5", 1);

  {
    bufferlist bl;
    bl.append("thABe hiEF");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }
  // foo snap[1]:          
  // foo snap[0]: [                  chunk2                   ] 
  // foo head   :          
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk1", 0);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk3", 0);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk4", 0);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk5", 0);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk2", 1);
}

TEST_F(LibRadosTwoPoolsPP, ManifestEvictRollback) {
  SKIP_IF_CRIMSON();
  // skip test if not yet pacific
  if (_get_required_osd_release(cluster) < "pacific") {
    cout << "cluster is not yet pacific, skipping test" << std::endl;
    return;
  }

  // create object
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("ABere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk1", &op));
  }
  {
    bufferlist bl;
    bl.append("CDere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk2", &op));
  }
  {
    bufferlist bl;
    bl.append("EFere hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("chunk3", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // create a snapshot, clone
  vector<uint64_t> my_snaps(1);
  ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&my_snaps[0]));
  ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(my_snaps[0],
	my_snaps));

  {
    bufferlist bl;
    bl.append("there hiHI");
    ASSERT_EQ(0, ioctx.write("foo", bl, bl.length(), 0));
  }


  // set-chunk 
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 2, 2, "chunk1", "foo");
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 8, 2, "chunk3", "foo");
  // foo snap[0]:  
  // foo head   :          [chunk1]                    [chunk3]

  ioctx.snap_set_read(my_snaps[0]);
  manifest_set_chunk(cluster, cache_ioctx, ioctx, 0, 10, "chunk2", "foo");
  // foo snap[0]: [                  chunk2                   ] 
  // foo head   :          [chunk1]                    [chunk3]
  
  sleep(10);
  ioctx.snap_set_read(librados::SNAP_HEAD);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk1", 1);
  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk3", 1);


  ioctx.snap_set_read(my_snaps[0]);
  // evict--this makes the chunk missing state
  {
    ObjectReadOperation op, stat_op;
    op.tier_evict();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate(
	"foo", completion, &op,
	librados::OPERATION_IGNORE_OVERLAY, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
  }

  // rollback to my_snaps[0]
  ASSERT_EQ(0, ioctx.selfmanaged_snap_rollback("foo", my_snaps[0]));

  ioctx.snap_set_read(librados::SNAP_HEAD);
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('C', bl[0]);
  }

  is_intended_refcount_state(ioctx, "foo", cache_ioctx, "chunk2", 1);
}

class LibRadosTwoPoolsECPP : public RadosTestECPP
{
public:
  LibRadosTwoPoolsECPP() {};
  ~LibRadosTwoPoolsECPP() override {};
protected:
  static void SetUpTestCase() {
    SKIP_IF_CRIMSON();
    pool_name = get_temp_pool_name();
    ASSERT_EQ("", create_one_ec_pool_pp(pool_name, s_cluster));
  }
  static void TearDownTestCase() {
    SKIP_IF_CRIMSON();
    ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, s_cluster));
  }
  static std::string cache_pool_name;

  void SetUp() override {
    SKIP_IF_CRIMSON();
    cache_pool_name = get_temp_pool_name();
    ASSERT_EQ(0, s_cluster.pool_create(cache_pool_name.c_str()));
    RadosTestECPP::SetUp();

    ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
    cache_ioctx.application_enable("rados", true);
    cache_ioctx.set_namespace(nspace);
  }
  void TearDown() override {
    SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
    ASSERT_EQ('b', bl[0]);
  }
}

TEST_F(LibRadosTwoPoolsECPP, Promote) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
      if (r == -EAGAIN ||
	  r == -ENOENT) {  // in case mgr osdmap is a bit stale
	sleep(5);
	continue;
      }
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
  SKIP_IF_CRIMSON();
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

  // read foo snap.  the OSD may or may not realize that this snap has
  // been logically deleted; either response is valid.
  {
    bufferlist bl;
    int r = ioctx.read("foo", bl, 1, 0);
    ASSERT_TRUE(r == 1 || r == -ENOENT);
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsECPP, Whiteout) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EBUSY, completion->get_return_value());
    completion->release();
  }
}

TEST_F(LibRadosTwoPoolsECPP, EvictSnap) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // cleanup
  ioctx.selfmanaged_snap_remove(my_snaps[0]);
}

TEST_F(LibRadosTwoPoolsECPP, TryFlush) {
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
    ASSERT_EQ(-EPERM, completion->get_return_value());
    completion->release();
  }

  // unpin
  {
    ObjectWriteOperation op;
    op.cache_unpin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  SKIP_IF_CRIMSON();
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  cache_ioctx.application_enable("rados", true);
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

      completion->wait_for_complete();
      completion2->wait_for_complete();
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
  SKIP_IF_CRIMSON();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
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

      completion->wait_for_complete();
      completion2->wait_for_complete();
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

    completion->wait_for_complete();
    completion2->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    ASSERT_EQ(0, completion2->get_return_value());
    completion->release();
    completion2->release();
  }
}

TEST_F(LibRadosTwoPoolsECPP, TryFlushReadRace) {
  SKIP_IF_CRIMSON();
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
  test_lock.lock();
  for (int i = 0; i < max_reads; ++i) {
    start_flush_read();
    num_reads++;
  }
  test_lock.unlock();

  // try-flush
  ObjectReadOperation op;
  op.cache_try_flush();
  librados::AioCompletion *completion = cluster.aio_create_completion();
  ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op,
      librados::OPERATION_IGNORE_OVERLAY |
      librados::OPERATION_SKIPRWLOCKS, NULL));

  completion->wait_for_complete();
  ASSERT_EQ(0, completion->get_return_value());
  completion->release();

  // stop reads
  std::unique_lock locker{test_lock};
  max_reads = 0;  
  cond.wait(locker, [] { return num_reads == 0;});
}

TEST_F(LibRadosTierECPP, CallForcesPromote) {
  SKIP_IF_CRIMSON();
  Rados cluster;
  std::string pool_name = get_temp_pool_name();
  std::string cache_pool_name = pool_name + "-cache";
  ASSERT_EQ("", create_one_ec_pool_pp(pool_name, cluster));
  ASSERT_EQ(0, cluster.pool_create(cache_pool_name.c_str()));
  IoCtx cache_ioctx;
  ASSERT_EQ(0, cluster.ioctx_create(cache_pool_name.c_str(), cache_ioctx));
  cache_ioctx.application_enable("rados", true);
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
    completion->wait_for_complete();
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
    completion->wait_for_complete();
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
  ASSERT_EQ(0, destroy_one_ec_pool_pp(pool_name, cluster));
}

TEST_F(LibRadosTierECPP, HitSetNone) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  utime_t start = ceph_clock_now();
  utime_t hard_stop = start + utime_t(600, 0);

  while (true) {
    utime_t now = ceph_clock_now();
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
      auto p = hbl.cbegin();
      HitSet hs;
      decode(hs, p);
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
  ceph_assert(num_pg > 0);

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

    auto p = bl.cbegin();
    decode(hitsets[i], p);

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
  SKIP_IF_CRIMSON();
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
  utime_t start = ceph_clock_now();
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

    utime_t now = ceph_clock_now();
    ASSERT_TRUE(now < hard_stop);

    sleep(1);
  }
  delete[] buf;
}

TEST_F(LibRadosTwoPoolsECPP, PromoteOn2ndRead) {
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
  SKIP_IF_CRIMSON();
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
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  {
    ObjectWriteOperation op;
    op.cache_pin();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("baz", completion, &op));
    completion->wait_for_complete();
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
TEST_F(LibRadosTwoPoolsECPP, SetRedirectRead) {
  SKIP_IF_CRIMSON();
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
    bl.append("there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("bar", &op));
  }

  // configure tier
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  {
    ObjectWriteOperation op;
    op.set_redirect("bar", cache_ioctx, 0);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('t', bl[0]);
  }

  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier remove\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, SetChunkRead) {
  SKIP_IF_CRIMSON();
  // note: require >= mimic

  {
    bufferlist bl;
    bl.append("there hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }

  {
    bufferlist bl;
    bl.append("There hi");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set_chunk
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 0, 4, "bar", "foo");

  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('T', bl[0]);
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, ManifestPromoteRead) {
  SKIP_IF_CRIMSON();
  // note: require >= mimic

  // create object
  {
    bufferlist bl;
    bl.append("hiaa there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("base chunk");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, cache_ioctx.operate("foo-chunk", &op));
  }
  {
    bufferlist bl;
    bl.append("HIaa there");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }
  {
    bufferlist bl;
    bl.append("BASE CHUNK");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar-chunk", &op));
  }

  // set-redirect
  {
    ObjectWriteOperation op;
    op.set_redirect("bar", ioctx, 0);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // set-chunk
  manifest_set_chunk(cluster, ioctx, cache_ioctx, 0, 10, "bar-chunk", "foo-chunk");
  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object (redirect)
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo", bl, 1, 0));
    ASSERT_EQ('H', bl[0]);
  }
  // promote
  {
    ObjectWriteOperation op;
    op.tier_promote();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo-chunk", completion, &op));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }
  // read and verify the object
  {
    bufferlist bl;
    ASSERT_EQ(1, cache_ioctx.read("foo-chunk", bl, 1, 0));
    ASSERT_EQ('B', bl[0]);
  }

  // wait for maps to settle before next test
  cluster.wait_for_latest_osdmap();
}

TEST_F(LibRadosTwoPoolsECPP, TrySetDedupTier) {
  SKIP_IF_CRIMSON();
  // note: require >= mimic
  
  bufferlist inbl;
  ASSERT_EQ(-EOPNOTSUPP, cluster.mon_command(
	set_pool_str(pool_name, "dedup_tier", cache_pool_name),
	inbl, NULL, NULL));
}

TEST_F(LibRadosTwoPoolsPP, PropagateBaseTierError) {
  SKIP_IF_CRIMSON();
  // write object  to base tier
  bufferlist omap_bl;
  encode(static_cast<uint32_t>(0U), omap_bl);

  ObjectWriteOperation op1;
  op1.omap_set({{"somekey", omap_bl}});
  ASSERT_EQ(0, ioctx.operate("propagate-base-tier-error", &op1));

  // configure cache
  bufferlist inbl;
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier add\", \"pool\": \"" + pool_name +
    "\", \"tierpool\": \"" + cache_pool_name +
    "\", \"force_nonempty\": \"--force-nonempty\" }",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier cache-mode\", \"pool\": \"" + cache_pool_name +
    "\", \"mode\": \"writeback\"}",
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    "{\"prefix\": \"osd tier set-overlay\", \"pool\": \"" + pool_name +
    "\", \"overlaypool\": \"" + cache_pool_name + "\"}",
    inbl, NULL, NULL));

  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_type", "bloom"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_count", 1),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "hit_set_period", 600),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "target_max_objects", 250),
    inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // guarded op should fail so expect error to propagate to cache tier
  bufferlist test_omap_bl;
  encode(static_cast<uint32_t>(1U), test_omap_bl);

  ObjectWriteOperation op2;
  op2.omap_cmp({{"somekey", {test_omap_bl, CEPH_OSD_CMPXATTR_OP_EQ}}}, nullptr);
  op2.omap_set({{"somekey", test_omap_bl}});

  ASSERT_EQ(-ECANCELED, ioctx.operate("propagate-base-tier-error", &op2));
}

TEST_F(LibRadosTwoPoolsPP, HelloWriteReturn) {
  SKIP_IF_CRIMSON();
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
			   "10000"),
	      inbl, NULL, NULL));

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // this *will* return data due to the RETURNVEC flag
  {
    bufferlist in, out;
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_return_data", in, &out, &rval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &o,
				   librados::OPERATION_RETURNVEC));
    completion->wait_for_complete();
    ASSERT_EQ(42, completion->get_return_value());
    ASSERT_EQ(42, rval);
    out.hexdump(std::cout);
    ASSERT_EQ("you might see this", std::string(out.c_str(), out.length()));
  }

  // this will overflow because the return data is too big
  {
    bufferlist in, out;
    int rval;
    ObjectWriteOperation o;
    o.exec("hello", "write_too_much_return_data", in, &out, &rval);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, ioctx.aio_operate("foo", completion, &o,
				   librados::OPERATION_RETURNVEC));
    completion->wait_for_complete();
    ASSERT_EQ(-EOVERFLOW, completion->get_return_value());
    ASSERT_EQ(-EOVERFLOW, rval);
    ASSERT_EQ("", std::string(out.c_str(), out.length()));
  }
}

TEST_F(LibRadosTwoPoolsPP, TierFlushDuringUnsetDedupTier) {
  SKIP_IF_CRIMSON();
  // skip test if not yet octopus
  if (_get_required_osd_release(cluster) < "octopus") {
    cout << "cluster is not yet octopus, skipping test" << std::endl;
    return;
  }

  bufferlist inbl;

  // set dedup parameters without dedup_tier
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "fingerprint_algorithm", "sha1"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "dedup_chunk_algorithm", "fastcdc"),
    inbl, NULL, NULL));
  ASSERT_EQ(0, cluster.mon_command(
    set_pool_str(cache_pool_name, "dedup_cdc_chunk_size", 1024),
    inbl, NULL, NULL));

  // create object
  bufferlist gbl;
  {
    generate_buffer(1024*8, &gbl);
    ObjectWriteOperation op;
    op.write_full(gbl);
    ASSERT_EQ(0, cache_ioctx.operate("foo", &op));
  }
  {
    bufferlist bl;
    bl.append("there hiHI");
    ObjectWriteOperation op;
    op.write_full(bl);
    ASSERT_EQ(0, ioctx.operate("bar", &op));
  }

  // wait for maps to settle
  cluster.wait_for_latest_osdmap();

  // set-chunk to set manifest object
  {
    ObjectReadOperation op;
    op.set_chunk(0, 2, ioctx, "bar", 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate("foo", completion, &op,
      librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(0, completion->get_return_value());
    completion->release();
  }

  // flush to check if proper error is returned
  {
    ObjectReadOperation op;
    op.tier_flush();
    librados::AioCompletion *completion = cluster.aio_create_completion();
    ASSERT_EQ(0, cache_ioctx.aio_operate(
      "foo", completion, &op, librados::OPERATION_IGNORE_CACHE, NULL));
    completion->wait_for_complete();
    ASSERT_EQ(-EINVAL, completion->get_return_value());
    completion->release();
  }
}


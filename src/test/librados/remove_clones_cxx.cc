// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

// CEPH_OSD_OP_REMOVE_CLONES forgets an object's history after a client has
// moved it elsewhere: the head becomes a redirect, the clones go, the snapshots
// live on, and a read at a snapid is forwarded.  Used on the wrong object it
// would simply lose snapshot data, so most of this file is about what it must
// refuse.
//
// Each test gets a source pool of the parameter's type and a replicated target
// pool.  Snapshots are allocated in both pools in step, so that a snapid means
// the same in each, as the file system's global snapids do.  The target holds
// the same history in upper case, so a test can tell where a read was served.
//
// The tests named ReplicaLost* mark an OSD down and set cluster-wide flags, so
// they run only with CEPH_TEST_REMOVE_CLONES_DISRUPT=1 on a cluster of exactly
// 3 OSDs with nothing else running.

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "common/ceph_json.h"
#include "common/errno.h"
#include "common/json/OSDStructures.h"
#include "include/rados.h"
#include "include/rados/librados.hpp"
#include "include/scope_guard.h"
#include "include/stringify.h"
#include "test/librados/crimson_utils.h"
#include "test/librados/test_cxx.h"
#include "test/librados/test_pool_types.h"
#include "gtest/gtest.h"

using namespace librados;
using ceph::test::PoolType;

namespace {

constexpr size_t OBJECT_SIZE = 64 * 1024;
constexpr auto STATS_TIMEOUT = std::chrono::seconds(60);
constexpr auto SCRUB_TIMEOUT = std::chrono::seconds(300);

bufferlist to_bl(const std::string& s)
{
  bufferlist bl;
  bl.append(s);
  return bl;
}

std::string upper(std::string s)
{
  std::transform(s.begin(), s.end(), s.begin(), ::toupper);
  return s;
}

bool disruptive_tests_enabled()
{
  return std::getenv("CEPH_TEST_REMOVE_CLONES_DISRUPT") != nullptr;
}

} // anonymous namespace

class RemoveClones : public ::testing::TestWithParam<PoolType> {
protected:
  static Rados rados;

  std::string pool_name;
  std::string target_name;
  IoCtx ioctx;   // the pool the object leaves
  IoCtx target;  // the pool it moved to
  std::vector<snap_t> snaps;  // newest first
  std::string now;            // an object's current contents

  static void SetUpTestSuite() {
    ASSERT_EQ("", connect_cluster_pp(rados));
  }
  static void TearDownTestSuite() {
    rados.shutdown();
  }

  void SetUp() override {
    SKIP_IF_CRIMSON();
    pool_name = get_temp_pool_name("remove_clones_src_");
    target_name = get_temp_pool_name("remove_clones_tgt_");
    ASSERT_EQ("", ceph::test::create_pool_by_type(pool_name, rados, GetParam()));
    if (GetParam() == PoolType::LEGACY_EC) {
      ASSERT_EQ("", set_allow_ec_overwrites_pp(pool_name, rados, true));
    }
    ASSERT_EQ("", create_pool_pp(target_name, rados));
    // A split makes the pool stats estimates until the next scrub, and moves
    // PGs around while a test waits for them.
    for (const auto& name : {pool_name, target_name}) {
      ASSERT_EQ(0, command("{\"prefix\": \"osd pool set\", \"pool\": \"" + name +
                           "\", \"var\": \"pg_autoscale_mode\", \"val\": \"off\"}"));
    }
    ASSERT_EQ(0, rados.ioctx_create(pool_name.c_str(), ioctx));
    ASSERT_EQ(0, rados.ioctx_create(target_name.c_str(), target));
    ioctx.application_enable("rados", true);
    target.application_enable("rados", true);
    rados.wait_for_latest_osdmap();
  }

  void TearDown() override {
    if (pool_name.empty()) {
      return;
    }
    ioctx.close();
    target.close();
    EXPECT_EQ(0, ceph::test::destroy_pool_by_type(pool_name, rados, GetParam()));
    EXPECT_EQ(0, destroy_pool_pp(target_name, rados));
  }

  // --- building a history -----------------------------------------------

  void new_snap() {
    uint64_t s, t;
    ASSERT_EQ(0, ioctx.selfmanaged_snap_create(&s));
    ASSERT_EQ(0, target.selfmanaged_snap_create(&t));
    ASSERT_EQ(s, t) << "the two pools' snapids must stay in step";
    snaps.insert(snaps.begin(), s);
    use_snapc(true);
  }

  void use_snapc(bool live) {
    std::vector<snap_t> none;
    snap_t seq = live && !snaps.empty() ? snaps.front() : 0;
    ASSERT_EQ(0, ioctx.selfmanaged_snap_set_write_ctx(seq, live ? snaps : none));
    ASSERT_EQ(0, target.selfmanaged_snap_set_write_ctx(seq, live ? snaps : none));
  }

  // Write the same range in both pools, upper case in the target.
  void write_both(const std::string& oid, const std::string& data, uint64_t off) {
    bufferlist lower_bl = to_bl(data), upper_bl = to_bl(upper(data));
    ASSERT_EQ(0, ioctx.write(oid, lower_bl, data.size(), off));
    ASSERT_EQ(0, target.write(oid, upper_bl, data.size(), off));
    now.resize(std::max(now.size(), off + data.size()));
    now.replace(off, data.size(), data);
  }

  void write_full_both(const std::string& oid, char c) {
    now.clear();
    write_both(oid, std::string(OBJECT_SIZE, c), 0);
  }

  // Each version overwrites only part of the object, so that the clones share
  // some of their bytes and the pool's byte count depends on clone_overlap.
  // Returns the contents at each snapshot, oldest first.
  std::vector<std::string> make_history(const std::string& oid, int versions) {
    std::vector<std::string> history;
    write_full_both(oid, 'a');
    for (int i = 1; i < versions; ++i) {
      history.push_back(now);
      new_snap();
      write_both(oid, std::string(4096, 'a' + i), i * 8192);
    }
    return history;
  }

  // --- the operation ----------------------------------------------------

  int redirect(const std::string& oid) {
    ObjectWriteOperation op;
    op.set_redirect(oid, target, 0);
    return ioctx.operate(oid, &op, OPERATION_IGNORE_REDIRECT);
  }

  int redirect_and_remove_clones(const std::string& oid) {
    ObjectWriteOperation op;
    op.set_redirect(oid, target, 0);
    op.remove_clones();
    return ioctx.operate(oid, &op, OPERATION_IGNORE_REDIRECT);
  }

  // As the file system does it: no snap context, or the OSD would make a clone.
  int move(const std::string& oid) {
    use_snapc(false);
    int r = redirect_and_remove_clones(oid);
    use_snapc(true);
    return r;
  }

  // --- looking at the result --------------------------------------------

  int list_snaps(const std::string& oid, snap_set_t* out) {
    ObjectReadOperation op;
    int rval = 0;
    op.list_snaps(out, &rval);
    ioctx.snap_set_read(SNAP_DIR);
    // Without the flag this would describe the target object.
    int r = ioctx.operate(oid, &op, nullptr, OPERATION_IGNORE_REDIRECT);
    ioctx.snap_set_read(SNAP_HEAD);
    return r < 0 ? r : rval;
  }

  size_t num_clones(const std::string& oid) {
    snap_set_t ss;
    EXPECT_EQ(0, list_snaps(oid, &ss));
    return std::count_if(ss.clones.begin(), ss.clones.end(),
                         [](const auto& c) { return c.cloneid != SNAP_HEAD; });
  }

  std::string read_at(IoCtx& io, const std::string& oid, snap_t snap) {
    bufferlist bl;
    io.snap_set_read(snap);
    int r = io.read(oid, bl, 4 * OBJECT_SIZE, 0);
    io.snap_set_read(SNAP_HEAD);
    return r < 0 ? "read failed: " + cpp_strerror(r) : bl.to_str();
  }

  void expect_history(const std::string& oid,
                      const std::vector<std::string>& history,
                      bool moved) {
    size_t n = history.size();
    ASSERT_EQ(n, snaps.size());
    for (size_t i = 0; i < n; ++i) {
      // snaps is newest first, history oldest first
      EXPECT_EQ(moved ? upper(history[i]) : history[i],
                read_at(ioctx, oid, snaps[n - 1 - i]))
        << "at snap " << snaps[n - 1 - i];
    }
  }

  int command(const std::string& json, bufferlist* outbl = nullptr) {
    bufferlist in, out;
    std::string outs;
    int r = rados.mon_command(std::string(json), std::move(in),
                              outbl ? outbl : &out, &outs);
    if (r < 0) {
      std::cerr << json << ": " << outs << std::endl;
    }
    return r;
  }

  struct PgInfo {
    std::string state;
    std::string deep_scrub_stamp;
    uint64_t errors = 0;
    uint64_t objects = 0, clones = 0, bytes = 0, omap = 0;
  };

  std::map<std::string, PgInfo> list_pgs(const std::string& pool) {
    std::map<std::string, PgInfo> pgs;
    bufferlist out;
    int r = command("{\"prefix\": \"pg ls-by-pool\", \"poolstr\": \"" + pool +
                    "\", \"format\": \"json\"}", &out);
    EXPECT_EQ(0, r);
    JSONParser p;
    if (r < 0 || !p.parse(out.c_str(), out.length())) {
      ADD_FAILURE() << "cannot parse pg ls-by-pool: " << out.to_str();
      return pgs;
    }
    auto stats = p.find_obj("pg_stats");
    if (!stats) {
      ADD_FAILURE() << "no pg_stats: " << out.to_str();
      return pgs;
    }
    for (const auto& s : stats->get_array_elements()) {
      JSONParser q;
      EXPECT_TRUE(q.parse(s.c_str(), s.size()));
      std::string pgid;
      PgInfo pg;
      JSONDecoder::decode_json("pgid", pgid, &q, true);
      JSONDecoder::decode_json("state", pg.state, &q, true);
      JSONDecoder::decode_json("last_deep_scrub_stamp", pg.deep_scrub_stamp, &q,
                               true);
      auto sum = q.find_obj("stat_sum");
      EXPECT_NE(nullptr, sum);
      if (sum) {
        JSONDecoder::decode_json("num_objects", pg.objects, sum, true);
        JSONDecoder::decode_json("num_object_clones", pg.clones, sum, true);
        JSONDecoder::decode_json("num_bytes", pg.bytes, sum, true);
        JSONDecoder::decode_json("num_objects_omap", pg.omap, sum, true);
        for (const char* key : {"num_scrub_errors", "num_shallow_scrub_errors",
                                "num_deep_scrub_errors"}) {
          uint64_t v = 0;
          JSONDecoder::decode_json(key, v, sum, true);
          pg.errors += v;
        }
      }
      pgs[pgid] = pg;
    }
    return pgs;
  }

  struct PoolCounts {
    uint64_t objects = 0, clones = 0, bytes = 0, omap = 0;
  };

  // The PGs' own counters, as reported to mgr.  They lag, so a test must wait
  // for them, and it must look at them before a scrub: scrub quietly corrects
  // a stats mismatch when it finds nothing else wrong.
  template <typename Pred>
  void wait_for_counts(const std::string& pool, Pred pred, const char* what) {
    auto deadline = std::chrono::steady_clock::now() + STATS_TIMEOUT;
    PoolCounts c;
    for (;;) {
      c = PoolCounts();
      for (const auto& [pgid, pg] : list_pgs(pool)) {
        c.objects += pg.objects;
        c.clones += pg.clones;
        c.bytes += pg.bytes;
        c.omap += pg.omap;
      }
      if (pred(c)) {
        return;
      }
      if (std::chrono::steady_clock::now() > deadline) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    FAIL() << "pool counters never reached: " << what << "; have objects "
           << c.objects << " clones " << c.clones << " bytes " << c.bytes
           << " omap " << c.omap;
  }

  void wait_for_clean(const std::string& pool) {
    // mgr's view lags: don't believe a state from before what just happened.
    std::this_thread::sleep_for(std::chrono::seconds(6));
    wait_for_active_clean(pool);
  }

  void wait_for_active_clean(const std::string& pool) {
    auto deadline = std::chrono::steady_clock::now() + SCRUB_TIMEOUT;
    std::map<std::string, PgInfo> pgs;
    for (;;) {
      pgs = list_pgs(pool);
      bool clean = !pgs.empty();
      for (const auto& [pgid, pg] : pgs) {
        clean = clean && pg.state == "active+clean";
      }
      if (clean) {
        return;
      }
      if (std::chrono::steady_clock::now() > deadline) {
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    std::ostringstream os;
    for (const auto& [pgid, pg] : pgs) {
      os << " " << pgid << "=" << pg.state;
    }
    FAIL() << "pool never went clean:" << os.str();
  }

  // Deep scrub every PG of the pool and insist on no errors.  This compares
  // each head's snapset with the clones that really exist, on every replica.
  // It says nothing about the stats: see wait_for_counts().
  void deep_scrub_clean(const std::string& pool) {
    wait_for_active_clean(pool);
    auto before = list_pgs(pool);
    ASSERT_FALSE(before.empty());
    const std::string cmd = "{\"prefix\": \"osd pool deep-scrub\", \"who\": [\"" +
      pool + "\"]}";
    ASSERT_EQ(0, command(cmd));
    auto deadline = std::chrono::steady_clock::now() + SCRUB_TIMEOUT;
    auto again = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    std::map<std::string, PgInfo> pgs;
    for (;;) {
      pgs = list_pgs(pool);
      std::ostringstream waiting;
      for (const auto& [pgid, pg] : before) {
        if (pgs[pgid].deep_scrub_stamp == pg.deep_scrub_stamp) {
          waiting << " " << pgid << "=" << pgs[pgid].state;
        }
      }
      if (waiting.str().empty()) {
        break;
      }
      ASSERT_LT(std::chrono::steady_clock::now(), deadline)
        << "deep scrub of " << pool << " never finished:" << waiting.str();
      // A request that reaches a PG at the wrong moment is dropped.
      if (std::chrono::steady_clock::now() > again) {
        ASSERT_EQ(0, command(cmd));
        again = std::chrono::steady_clock::now() + std::chrono::seconds(30);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    for (const auto& [pgid, pg] : pgs) {
      EXPECT_EQ(0u, pg.errors) << pgid << " " << pg.state;
      EXPECT_EQ(std::string::npos, pg.state.find("inconsistent"))
        << pgid << " " << pg.state;
    }
  }

  void deep_scrub_both() {
    deep_scrub_clean(pool_name);
    deep_scrub_clean(target_name);
  }

  // --- disruption ------------------------------------------------------

  int osd_flag(const char* verb, const char* key) {
    return command(std::string("{\"prefix\": \"osd ") + verb +
                   "\", \"key\": \"" + key + "\"}");
  }

  std::vector<int> acting(const std::string& oid) {
    ceph::messaging::osd::OSDMapReply reply;
    bufferlist out;
    EXPECT_EQ(0, command("{\"prefix\": \"osd map\", \"pool\": \"" + pool_name +
                         "\", \"object\": \"" + oid +
                         "\", \"format\": \"json\"}", &out));
    JSONParser p;
    EXPECT_TRUE(p.parse(out.c_str(), out.length()));
    reply.decode_json(&p);
    return reply.acting;
  }

  bool in_acting(const std::string& oid, int osd) {
    auto a = acting(oid);
    return std::find(a.begin(), a.end(), osd) != a.end();
  }

  void wait_for_acting(const std::string& oid, int osd, bool want) {
    auto deadline = std::chrono::steady_clock::now() + SCRUB_TIMEOUT;
    while (in_acting(oid, osd) != want) {
      ASSERT_LT(std::chrono::steady_clock::now(), deadline);
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  // An EC pool of k=2, m=1 stops at min_size 3 with one OSD gone.
  void allow_one_lost_shard() {
    if (GetParam() != PoolType::REPLICATED) {
      ASSERT_EQ(0, command("{\"prefix\": \"osd pool set\", \"pool\": \"" +
                           pool_name + "\", \"var\": \"min_size\", \"val\": \"2\"}"));
    }
  }

  // The OSD keeps running, but stays out of the map until noup is cleared.
  void lose_osd(const std::string& oid, int osd) {
    ASSERT_EQ(0, osd_flag("set", "noup"));
    ASSERT_EQ(0, command("{\"prefix\": \"osd down\", \"ids\": [\"" +
                         stringify(osd) + "\"]}"));
    wait_for_acting(oid, osd, false);
  }

  void bring_back_osd(const std::string& oid, int osd) {
    ASSERT_EQ(0, osd_flag("unset", "noup"));
    wait_for_acting(oid, osd, true);
  }
};

Rados RemoveClones::rados;

// --- what it must refuse ---------------------------------------------------

TEST_P(RemoveClones, RefusesPlainObject)
{
  write_full_both("o", 'a');
  ObjectWriteOperation op;
  op.remove_clones();
  EXPECT_EQ(-EINVAL, ioctx.operate("o", &op));
  EXPECT_EQ(now, read_at(ioctx, "o", SNAP_HEAD));
}

TEST_P(RemoveClones, RefusesPlainObjectWithClones)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(2u, num_clones("o"));
  snap_set_t before, after;
  ASSERT_EQ(0, list_snaps("o", &before));

  ObjectWriteOperation op;
  op.remove_clones();
  EXPECT_EQ(-EINVAL, ioctx.operate("o", &op));

  ASSERT_EQ(0, list_snaps("o", &after));
  EXPECT_EQ(before.seq, after.seq);
  EXPECT_EQ(2u, num_clones("o"));
  EXPECT_EQ(now, read_at(ioctx, "o", SNAP_HEAD));
  expect_history("o", history, false);
}

TEST_P(RemoveClones, RefusesMissingObjectAndDoesNotCreateIt)
{
  ObjectWriteOperation op;
  op.remove_clones();
  EXPECT_EQ(-EINVAL, ioctx.operate("nothing", &op));
  uint64_t size;
  time_t mtime;
  EXPECT_EQ(-ENOENT, ioctx.stat("nothing", &size, &mtime));
}

// A manifest, but not a redirect: the chunks' references are the only way back
// to the data, so the has_manifest() half of the check alone would not do.
TEST_P(RemoveClones, RefusesChunkedObject)
{
  if (GetParam() != PoolType::REPLICATED) {
    GTEST_SKIP() << "chunked objects are for replicated pools";
  }
  auto history = make_history("o", 2);
  bufferlist chunk = to_bl("chunk");
  ASSERT_EQ(0, target.write_full("chunk", chunk));
  {
    ObjectReadOperation op;
    op.set_chunk(0, 4096, target, "chunk", 0, CEPH_OSD_OP_FLAG_WITH_REFERENCE);
    ASSERT_EQ(0, ioctx.operate("o", &op, nullptr, OPERATION_IGNORE_REDIRECT));
  }
  ObjectWriteOperation op;
  op.remove_clones();
  EXPECT_EQ(-EINVAL, ioctx.operate("o", &op, OPERATION_IGNORE_REDIRECT));
  EXPECT_EQ(1u, num_clones("o"));
  expect_history("o", history, false);
}

// librados cannot name a clone in a write, because ObjectWriteOperation always
// goes to the head.  The same op in a read operation can, and the OSD treats it
// as the write it is, so this reaches do_op with a snapid.  do_op refuses every
// such write, so the check in the op itself is a second line of defence.
TEST_P(RemoveClones, RefusesClone)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(0, redirect("o"));  // the clones stay
  ASSERT_EQ(2u, num_clones("o"));

  for (snap_t snap : {snaps[0], snaps[1], snap_t(SNAP_DIR)}) {
    ObjectReadOperation op;
    op.remove_clones();
    bufferlist bl;
    ioctx.snap_set_read(snap);
    EXPECT_EQ(-EINVAL, ioctx.operate("o", &op, &bl, OPERATION_IGNORE_REDIRECT))
      << "snap " << snap;
    ioctx.snap_set_read(SNAP_HEAD);
  }
  EXPECT_EQ(2u, num_clones("o"));
  expect_history("o", history, false);
}

TEST_P(RemoveClones, RefusesWhenNotLastAndChangesNothing)
{
  auto history = make_history("o", 3);
  snap_set_t before, after;
  ASSERT_EQ(0, list_snaps("o", &before));
  use_snapc(false);
  {
    ObjectWriteOperation op;
    op.set_redirect("o", target, 0);
    op.remove_clones();
    op.setxattr("user.x", to_bl("y"));
    EXPECT_EQ(-EINVAL, ioctx.operate("o", &op, OPERATION_IGNORE_REDIRECT));
  }
  {
    // Not yet a redirect when it runs, whichever check catches it first.
    ObjectWriteOperation op;
    op.remove_clones();
    op.set_redirect("o", target, 0);
    EXPECT_EQ(-EINVAL, ioctx.operate("o", &op, OPERATION_IGNORE_REDIRECT));
  }
  use_snapc(true);

  // The set-redirect of the failed op must have gone too, or this would be
  // the target's upper case.
  EXPECT_EQ(now, read_at(ioctx, "o", SNAP_HEAD));
  ASSERT_EQ(0, list_snaps("o", &after));
  EXPECT_EQ(before.seq, after.seq);
  EXPECT_EQ(2u, num_clones("o"));
  expect_history("o", history, false);
}

TEST_P(RemoveClones, RefusesWhenNotLastOnARedirect)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(0, redirect("o"));
  use_snapc(false);
  {
    ObjectWriteOperation op;
    op.remove_clones();
    op.setxattr("user.x", to_bl("y"));
    EXPECT_EQ(-EINVAL, ioctx.operate("o", &op, OPERATION_IGNORE_REDIRECT));
  }
  use_snapc(true);
  EXPECT_EQ(2u, num_clones("o"));
  expect_history("o", history, false);
}

// make_writeable() runs after the op has reset the seq, so with a snap context
// it would clone the head for every snap in it.  With a snap newer than the
// head, the op returned 0, scrub was clean, and reads at every snapshot could
// return the head's data instead of their own.
TEST_P(RemoveClones, RefusesNewerSnapContext)
{
  auto history = make_history("o", 3);
  new_snap();
  EXPECT_EQ(-EINVAL, redirect_and_remove_clones("o"));
  EXPECT_EQ(2u, num_clones("o"));
  EXPECT_EQ(history[0], read_at(ioctx, "o", snaps[2]));
}

// With the head's own snap context, which is what a client would normally
// send, the new clone would have the id of one the op has just locked, and the
// OSD asserted in get_write_greedy().  The client resends, so that took down
// every OSD in the PG in turn.
TEST_P(RemoveClones, RefusesCurrentSnapContext)
{
  auto history = make_history("o", 3);
  EXPECT_EQ(-EINVAL, redirect_and_remove_clones("o"));
  EXPECT_EQ(2u, num_clones("o"));
}

// --- what it must do -------------------------------------------------------

TEST_P(RemoveClones, MovesAHistory)
{
  auto history = make_history("o", 4);
  ASSERT_EQ(3u, num_clones("o"));
  expect_history("o", history, false);
  wait_for_counts(pool_name,
                      [](const auto& c) { return c.clones == 3; },
                      "3 clones before the move");

  ASSERT_EQ(0, move("o"));

  snap_set_t ss;
  ASSERT_EQ(0, list_snaps("o", &ss));
  EXPECT_EQ(0u, ss.seq) << "a read at a snapid would look for a clone";
  EXPECT_EQ(0u, num_clones("o"));

  EXPECT_EQ(upper(now), read_at(ioctx, "o", SNAP_HEAD));
  expect_history("o", history, true);

  wait_for_counts(pool_name, [](const auto& c) {
    return c.clones == 0 && c.objects == 1 && c.bytes == 0;
  }, "no clones, one empty head");

  // The target kept all of it.
  EXPECT_EQ(3u, [&] {
    snap_set_t t;
    ObjectReadOperation op;
    int rval;
    op.list_snaps(&t, &rval);
    target.snap_set_read(SNAP_DIR);
    EXPECT_EQ(0, target.operate("o", &op, nullptr));
    target.snap_set_read(SNAP_HEAD);
    size_t n = 0;
    for (const auto& c : t.clones) {
      n += c.cloneid != SNAP_HEAD;
    }
    return n;
  }());

  deep_scrub_both();
}

// The redirect may come from an earlier op, and without IGNORE_REDIRECT the op
// must still stay on the source object and not reach through to the target's
// clones.
TEST_P(RemoveClones, ActsOnTheRedirectNotItsTarget)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(0, redirect("o"));
  ASSERT_EQ(2u, num_clones("o"));
  // Until the clones go, a read at a snapid stops here.
  expect_history("o", history, false);

  use_snapc(false);
  ObjectWriteOperation op;
  op.remove_clones();
  ASSERT_EQ(0, ioctx.operate("o", &op));
  use_snapc(true);

  EXPECT_EQ(0u, num_clones("o"));
  expect_history("o", history, true);
  size_t n = history.size();
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(upper(history[i]), read_at(target, "o", snaps[n - 1 - i]));
  }
  deep_scrub_both();
}

// Every step of the file system's cleanup must be repeatable: a failover can
// run it twice.
TEST_P(RemoveClones, IsRepeatable)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(0, move("o"));
  ASSERT_EQ(0, move("o"));

  use_snapc(false);
  ObjectWriteOperation op;
  op.remove_clones();
  EXPECT_EQ(0, ioctx.operate("o", &op));
  use_snapc(true);

  EXPECT_EQ(upper(now), read_at(ioctx, "o", SNAP_HEAD));
  expect_history("o", history, true);
  deep_scrub_both();
}

// Deleted at head with snapshots alive: only clones are left, and the file
// system must make a head before it can redirect it.
TEST_P(RemoveClones, MovesAHistoryWithNoHead)
{
  auto history = make_history("o", 3);
  ASSERT_EQ(0, ioctx.remove("o"));
  ASSERT_EQ(0, target.remove("o"));
  ASSERT_EQ(2u, num_clones("o"));

  use_snapc(false);
  ObjectWriteOperation op;
  op.create(false);
  op.set_redirect("o", target, 0);
  op.remove_clones();
  ASSERT_EQ(0, ioctx.operate("o", &op, OPERATION_IGNORE_REDIRECT));
  use_snapc(true);

  EXPECT_EQ(0u, num_clones("o"));
  expect_history("o", history, true);
  bufferlist bl;
  EXPECT_EQ(-ENOENT, ioctx.read("o", bl, 1, 0));
  deep_scrub_both();
}

// A clone with omap counts in the pool's stats too.
TEST_P(RemoveClones, CountsOmapOnClones)
{
  bool has_omap = false;
  ASSERT_EQ(0, ceph::test::get_pool_supports_omap(rados, pool_name, &has_omap));
  if (!has_omap) {
    GTEST_SKIP() << "pool does not support omap";
  }
  write_full_both("o", 'a');
  std::map<std::string, bufferlist> kv{{"key", to_bl("value")}};
  {
    ObjectWriteOperation op;
    op.omap_set(kv);
    ASSERT_EQ(0, ioctx.operate("o", &op));
  }
  new_snap();
  write_both("o", std::string(4096, 'b'), 0);
  ASSERT_EQ(1u, num_clones("o"));
  wait_for_counts(pool_name, [](const auto& c) { return c.omap == 2; },
                  "omap on the head and the clone");
  ASSERT_EQ(0, move("o"));
  EXPECT_EQ(0u, num_clones("o"));
  wait_for_counts(pool_name, [](const auto& c) {
    return c.clones == 0 && c.omap == 0 && c.bytes == 0;
  }, "no omap left");
  deep_scrub_both();
}

// The snapshots are deleted later, as they will be: the source has nothing
// left to trim, and must not trip over the clones that it no longer has.
TEST_P(RemoveClones, SnapTrimAfterwards)
{
  make_history("o", 4);
  ASSERT_EQ(0, move("o"));

  use_snapc(false);
  for (snap_t s : snaps) {
    ASSERT_EQ(0, ioctx.selfmanaged_snap_remove(s));
    ASSERT_EQ(0, target.selfmanaged_snap_remove(s));
  }
  snaps.clear();

  wait_for_counts(target_name,
                      [](const auto& c) { return c.clones == 0; },
                      "target trimmed");
  EXPECT_EQ(upper(now), read_at(ioctx, "o", SNAP_HEAD));
  deep_scrub_both();
}

// A write through the redirect goes to the target, under the writer's snap
// context, and leaves the source's reset seq alone.
TEST_P(RemoveClones, LaterWritesGoToTheTarget)
{
  make_history("o", 3);
  ASSERT_EQ(0, move("o"));
  std::string before = upper(now);

  new_snap();
  bufferlist bl = to_bl("NEW!");
  ASSERT_EQ(0, ioctx.write("o", bl, 4, 0));

  snap_set_t ss;
  ASSERT_EQ(0, list_snaps("o", &ss));
  EXPECT_EQ(0u, ss.seq);
  EXPECT_EQ(0u, num_clones("o"));

  std::string want = before;
  want.replace(0, 4, "NEW!");
  EXPECT_EQ(want, read_at(ioctx, "o", SNAP_HEAD));
  EXPECT_EQ(want, read_at(target, "o", SNAP_HEAD));
  EXPECT_EQ(before, read_at(ioctx, "o", snaps[0]));
  deep_scrub_both();
}

// --- replicas going away ---------------------------------------------------

// A cleanup that spans many objects, with an OSD lost half way through.  Some
// PGs lose their primary, some a replica.  The OSD must apply the clones'
// deletions from the log when it comes back, or scrub finds them.
TEST_P(RemoveClones, ReplicaLostMidway)
{
  if (!disruptive_tests_enabled()) {
    GTEST_SKIP() << "needs CEPH_TEST_REMOVE_CLONES_DISRUPT=1";
  }
  allow_one_lost_shard();
  const int n = 16;
  for (int i = 0; i < n; ++i) {
    write_full_both(stringify(i), 'a');
  }
  const std::string v1 = now;
  new_snap();
  for (int i = 0; i < n; ++i) {
    write_both(stringify(i), std::string(4096, 'b'), 8192);
  }
  const std::string v2 = now;

  for (int i = 0; i < n / 2; ++i) {
    ASSERT_EQ(0, move(stringify(i)));
  }

  int gone = acting("0")[1];
  auto sg = make_scope_guard([&] { osd_flag("unset", "noup"); });
  lose_osd("0", gone);

  for (int i = n / 2; i < n; ++i) {
    ASSERT_EQ(0, move(stringify(i)));
  }
  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(upper(v1), read_at(ioctx, stringify(i), snaps[0]));
  }

  bring_back_osd("0", gone);
  wait_for_clean(pool_name);

  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(0u, num_clones(stringify(i)));
    EXPECT_EQ(upper(v2), read_at(ioctx, stringify(i), SNAP_HEAD));
    EXPECT_EQ(upper(v1), read_at(ioctx, stringify(i), snaps[0]));
  }
  wait_for_counts(pool_name, [&](const auto& c) {
    return c.clones == 0 && c.objects == (uint64_t)n && c.bytes == 0;
  }, "no clones left");
  deep_scrub_both();
}

// A clone that a returning replica has not recovered yet must not be removed.
// The op waits for it, like any op on a degraded object: that recovers the one
// object, even with norecover set, and the op goes on by itself.  It used to
// return -EAGAIN, which to execute_ctx() means that an op has queued itself:
// nothing replied, and the client hung.
TEST_P(RemoveClones, ReplicaLostWaitsForDegradedClone)
{
  if (!disruptive_tests_enabled()) {
    GTEST_SKIP() << "needs CEPH_TEST_REMOVE_CLONES_DISRUPT=1";
  }
  allow_one_lost_shard();
  write_full_both("o", 'a');
  std::string v1 = now;
  new_snap();

  int gone = acting("o")[1];
  auto sg = make_scope_guard([&] {
    osd_flag("unset", "noup");
    osd_flag("unset", "norecover");
  });
  lose_osd("o", gone);

  // Makes the clone while that OSD is away.
  write_both("o", std::string(4096, 'b'), 8192);

  ASSERT_EQ(0, osd_flag("set", "norecover"));
  bring_back_osd("o", gone);

  // A write to a degraded head waits for that one object to recover, which
  // norecover does not stop.  That leaves only the clone degraded.
  bufferlist one = to_bl("1");
  ASSERT_EQ(0, ioctx.setxattr("o", "user.kick", one));

  use_snapc(false);
  ObjectWriteOperation op;
  op.set_redirect("o", target, 0);
  op.remove_clones();
  AioCompletion* c = rados.aio_create_completion();
  auto release = make_scope_guard([&] { c->release(); });
  ASSERT_EQ(0, ioctx.aio_operate("o", c, &op, OPERATION_IGNORE_REDIRECT));
  for (int i = 0; i < 60 && !c->is_complete(); ++i) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  ASSERT_TRUE(c->is_complete()) << "the op was lost";
  EXPECT_EQ(0, c->get_return_value());
  ASSERT_EQ(0, osd_flag("unset", "norecover"));
  use_snapc(true);
  wait_for_clean(pool_name);

  EXPECT_EQ(0u, num_clones("o"));
  EXPECT_EQ(upper(v1), read_at(ioctx, "o", snaps[0])) << "forwarded to the target";
  deep_scrub_both();
}

INSTANTIATE_TEST_SUITE_P(
  PoolTypes, RemoveClones,
  ::testing::Values(PoolType::REPLICATED, PoolType::FAST_EC, PoolType::LEGACY_EC),
  [](const ::testing::TestParamInfo<PoolType>& info) {
    return ceph::test::pool_type_name(info.param);
  });

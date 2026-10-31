// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "rgw_gc.h"
#include "rgw_sal_rados.h"
#include "rgw_tools.h"
#include "rgw/rgw_sal_config.h"
#include "rgw/rgw_zone.h"
#include "cls/lock/cls_lock_client.h"
#include "cls/rgw/cls_rgw_client.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/dout.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/scope_guard.h"

#include <chrono>
#include <list>
#include <map>
#include <set>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <unistd.h>

namespace {

const DoutPrefixProvider* dpp = nullptr;
rgw::sal::RadosStore* store = nullptr;
librados::IoCtx gc_ioctx;   // gc log objects
librados::IoCtx tail_ioctx; // raw tail objects of the test chains
std::string tail_pool;      // tail_ioctx's pool as a cls_rgw_obj pool

// rgw_gc_max_objs is 1, so every chain lands here
const std::string gc_oid = "gc.0";
const std::string gc_lock_name = "gc_process";
constexpr const char* refcount_attr = "refcount";

RGWRados* rgw_rados() { return store->getRados(); }

void set_conf(std::string_view key, const std::string& val)
{
  g_conf().set_val_or_die(key, val);
}

struct Chain {
  std::string tag;
  std::vector<std::string> oids;
};

// n chains of m raw objects without a refcount xattr; gc removes each
// through its implicit ref
std::vector<Chain> make_chains(const std::string& prefix, size_t n, size_t m)
{
  std::vector<Chain> chains(n);
  for (size_t i = 0; i < n; ++i) {
    chains[i].tag = fmt::format("{}.{}", prefix, i);
    for (size_t j = 0; j < m; ++j) {
      auto oid = fmt::format("{}.{}", chains[i].tag, j);
      EXPECT_EQ(0, tail_ioctx.create(oid, false));
      chains[i].oids.push_back(std::move(oid));
    }
  }
  return chains;
}

cls_rgw_obj_chain to_obj_chain(const Chain& c)
{
  cls_rgw_obj_chain chain;
  for (const auto& oid : c.oids) {
    chain.push_obj(tail_pool, cls_rgw_obj_key(oid), "");
  }
  return chain;
}

// send_chain_to_gc() expires entries after rgw_gc_obj_min_wait
void enqueue(const std::vector<Chain>& chains, int min_wait)
{
  set_conf("rgw_gc_obj_min_wait", std::to_string(min_wait));
  for (const auto& c : chains) {
    auto chain = to_obj_chain(c);
    auto [r, leftover] = rgw_rados()->send_chain_to_gc(chain, c.tag, null_yield);
    ASSERT_EQ(0, r);
  }
}

// tags of all entries on gc.0, omap and queue, in list order
std::vector<std::string> list_tags()
{
  std::vector<std::string> tags;
  int index = 0;
  std::string marker;
  bool truncated = true;
  bool processing_queue = false;
  while (truncated) {
    std::list<cls_rgw_gc_obj_info> entries;
    int r = rgw_rados()->list_gc_objs(index, marker, 1000, false, entries, truncated,
                                      processing_queue, 0);
    if (r < 0) {
      ADD_FAILURE() << "list_gc_objs returned " << r;
      break;
    }
    for (const auto& e : entries) {
      tags.push_back(e.tag);
    }
  }
  return tags;
}

// entries in the gc.0 queue, read without RGWGC so a processor may be running
size_t queue_length()
{
  size_t n = 0;
  std::string marker;
  bool truncated = true;
  while (truncated) {
    std::list<cls_rgw_gc_obj_info> entries;
    int r = cls_rgw_gc_queue_list_entries(gc_ioctx, gc_oid, marker, 1000, false, entries,
                                          truncated, marker);
    if (r < 0) {
      ADD_FAILURE() << "queue list returned " << r;
      break;
    }
    n += entries.size();
  }
  return n;
}

// this test's raw objects still in the pool
std::set<std::string> objects_left(const std::string& prefix)
{
  std::set<std::string> oids;
  for (auto o = tail_ioctx.nobjects_begin(); o != tail_ioctx.nobjects_end(); ++o) {
    if (o->get_oid().starts_with(prefix + ".")) {
      oids.insert(o->get_oid());
    }
  }
  return oids;
}

bool exists(const std::string& oid)
{
  return tail_ioctx.stat(oid, nullptr, nullptr) == 0;
}

// the queue only loses a finished prefix: what is left is a suffix of the
// enqueued chains, and no trimmed chain has an object left
void expect_trimmed_prefix(const std::vector<Chain>& chains,
                           const std::vector<std::string>& tags,
                           const std::set<std::string>& left)
{
  ASSERT_LE(tags.size(), chains.size());
  const size_t trimmed = chains.size() - tags.size();
  for (size_t i = 0; i < tags.size(); ++i) {
    ASSERT_EQ(chains[trimmed + i].tag, tags[i]);
  }
  for (size_t i = 0; i < trimmed; ++i) {
    for (const auto& oid : chains[i].oids) {
      ASSERT_EQ(0u, left.count(oid)) << "trimmed " << chains[i].tag << " left " << oid;
    }
  }
}

// process gc.0 until nothing is listed
void drain()
{
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(0, rgw_rados()->process_gc(false, 0));
    if (list_tags().empty()) {
      return;
    }
  }
  FAIL() << "gc.0 did not drain";
}

} // anonymous namespace

class RgwGc : public ::testing::Test {
 protected:
  const std::string prefix = fmt::format("{}-{}",
      ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());

  void SetUp() override {
    set_conf("rgw_gc_processor_max_time", "60");
    set_conf("rgw_gc_max_concurrent_io", "10");
    ASSERT_NO_FATAL_FAILURE(drain());
  }

  void TearDown() override {
    ASSERT_NO_FATAL_FAILURE(drain());
    EXPECT_TRUE(objects_left(prefix).empty());
  }
};

TEST_F(RgwGc, timeout_still_trims)
{
  const size_t objs = 40;
  const auto chains = make_chains(prefix, 400, objs);
  ASSERT_NO_FATAL_FAILURE(enqueue(chains, 0));

  set_conf("rgw_gc_processor_max_time", "3");
  set_conf("rgw_gc_max_concurrent_io", "1");
  // a run that hits the deadline still trims every entry it admitted, so only
  // untouched entries stay listed
  auto tags = list_tags();
  size_t runs = 0;
  while (!tags.empty()) {
    ASSERT_EQ(0, rgw_rados()->process_gc(false, 0));
    ++runs;
    auto next = list_tags();
    ASSERT_LT(next.size(), tags.size());
    const auto left = objects_left(prefix);
    ASSERT_NO_FATAL_FAILURE(expect_trimmed_prefix(chains, next, left));
    ASSERT_EQ(next.size() * objs, left.size()) << "a finished entry was left listed";
    tags = std::move(next);
  }
  EXPECT_LT(1u, runs) << "the first run drained gc.0, raise workload";
}

TEST_F(RgwGc, stops_at_first_unexpired)
{
  const auto a = make_chains(prefix + ".a", 1, 2);
  const auto b = make_chains(prefix + ".b", 1, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue(a, 3600));
  ASSERT_NO_FATAL_FAILURE(enqueue(b, 0));

  ASSERT_EQ(0, rgw_rados()->process_gc(true, 0));
  EXPECT_EQ((std::vector<std::string>{a[0].tag, b[0].tag}), list_tags());
  for (const auto& oid : b[0].oids) {
    EXPECT_TRUE(exists(oid)) << oid;
  }
}

TEST_F(RgwGc, failed_entry_blocks_trim)
{
  const auto chains = make_chains(prefix, 3, 2);
  const std::string& poisoned = chains[1].oids[0];
  {
    // an undecodable refcount fails cls_refcount_put with -EIO
    bufferlist bl;
    bl.append("garbage");
    ASSERT_EQ(0, tail_ioctx.setxattr(poisoned, refcount_attr, bl));
    auto unpoison = make_scope_guard([&] { tail_ioctx.rmxattr(poisoned, refcount_attr); });
    ASSERT_NO_FATAL_FAILURE(enqueue(chains, 0));

    ASSERT_EQ(0, rgw_rados()->process_gc(false, 0));
    EXPECT_EQ((std::vector<std::string>{chains[1].tag, chains[2].tag}), list_tags());
    for (const auto& oid : chains[0].oids) {
      EXPECT_FALSE(exists(oid)) << oid;
    }
    EXPECT_TRUE(exists(poisoned));
  }
}

TEST_F(RgwGc, busy_shard_skipped)
{
  const auto chains = make_chains(prefix, 5, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue(chains, 0));
  {
    // another gc processor holds the shard
    rados::cls::lock::Lock l(gc_lock_name);
    l.set_cookie(prefix);
    l.set_duration(utime_t(60, 0));
    librados::ObjectWriteOperation lock_op;
    l.lock_exclusive(&lock_op);
    ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &lock_op));
    auto unlock = make_scope_guard([&] {
        librados::ObjectWriteOperation op;
        l.unlock(&op);
        gc_ioctx.operate(gc_oid, &op);
      });

    ASSERT_EQ(0, rgw_rados()->process_gc(false, 0));
    EXPECT_EQ(chains.size(), list_tags().size());
    EXPECT_EQ(10u, objects_left(prefix).size());
  }
}

TEST_F(RgwGc, lease_broken_mid_run)
{
  const auto chains = make_chains(prefix, 1000, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue(chains, 0));

  // the fixture's 60s lease renews at 30s, after the next trim meets the fence
  set_conf("rgw_gc_max_concurrent_io", "1");
  size_t snapshot = 0;
  {
    // a broken lock aborts the lease; the shard is skipped, so the result
    // isn't checked
    std::jthread worker{[] { rgw_rados()->process_gc(false, 0); }};

    // wait for a trim under the lease
    const auto timeout = std::chrono::steady_clock::now() + std::chrono::seconds(90);
    while (queue_length() == chains.size()) {
      ASSERT_TRUE(std::chrono::steady_clock::now() < timeout) << "no trim landed";
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    std::map<rados::cls::lock::locker_id_t, rados::cls::lock::locker_info_t> lockers;
    {
      librados::ObjectReadOperation op;
      rados::cls::lock::get_lock_info_start(&op, gc_lock_name);
      bufferlist bl;
      ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &op, &bl));
      auto p = bl.cbegin();
      ClsLockType type;
      std::string tag;
      ASSERT_EQ(0, rados::cls::lock::get_lock_info_finish(&p, &lockers, &type, &tag));
    }
    ASSERT_EQ(1u, lockers.size()) << "gc released gc.0 before the break, raise workload";
    const auto& id = lockers.begin()->first;
    librados::ObjectWriteOperation op;
    rados::cls::lock::break_lock(&op, gc_lock_name, id.cookie, id.locker);
    ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &op)) << "gc finished before the break, "
        "raise workload";
    // a trim asserts the lock and the osd orders ops on gc.0, so no trim
    // lands after this read
    snapshot = queue_length();
    worker.join();
  }

  const auto tags = list_tags();
  ASSERT_FALSE(tags.empty()) << "gc drained gc.0 before the break, raise workload";
  ASSERT_EQ(snapshot, tags.size()) << "trimmed after the lock was broken";
  ASSERT_NO_FATAL_FAILURE(expect_trimmed_prefix(chains, tags, objects_left(prefix)));
}

TEST_F(RgwGc, omap_then_queue)
{
  const auto omap = make_chains(prefix + ".omap", 5, 3);
  const auto queue = make_chains(prefix + ".queue", 5, 3);

  // a shard initialized for the queue that still has omap entries
  for (const auto& c : omap) {
    cls_rgw_gc_obj_info info;
    info.tag = c.tag;
    info.chain = to_obj_chain(c);
    librados::ObjectWriteOperation op;
    cls_rgw_gc_set_entry(op, 0, info);
    ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &op));
  }
  // the setup drain moved gc.0 to the queue; make list and the processor read the omap
  rgw_rados()->get_gc()->transitioned_objects_cache[0] = false;
  ASSERT_NO_FATAL_FAILURE(enqueue(queue, 0));
  ASSERT_EQ(omap.size() + queue.size(), list_tags().size());

  ASSERT_EQ(0, rgw_rados()->process_gc(false, 0));
  EXPECT_TRUE(rgw_rados()->get_gc()->transitioned_objects_cache[0]);

  {
    librados::ObjectReadOperation op;
    bufferlist bl;
    cls_rgw_gc_list(op, "", 1, false, bl);
    ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &op, nullptr));
    std::list<cls_rgw_gc_obj_info> entries;
    bool truncated = false;
    std::string next_marker;
    ASSERT_EQ(0, cls_rgw_gc_list_decode(bl, entries, truncated, next_marker));
    EXPECT_TRUE(entries.empty());
  }
  EXPECT_TRUE(list_tags().empty());
  EXPECT_TRUE(objects_left(prefix).empty());
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, const_cast<const char**>(argv));
  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);

  // region -> zonegroup conversion (must happen before common_init_finish)
  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }
  // one gc shard; can't change after startup
  g_conf().set_val_or_die("rgw_gc_max_objs", "1");

  common_init_finish(g_ceph_context);

  static NoDoutPrefix nodpp(g_ceph_context, ceph_subsys_rgw);
  dpp = &nodpp;

  static ceph::async::io_context_pool context_pool{cct->_conf->rgw_thread_pool_size};

  DriverManager::Config cfg = DriverManager::get_config(true, g_ceph_context);

  auto config_store_type = g_conf().get_val<std::string>("rgw_config_store");
  auto cfgstore = DriverManager::create_config_store(dpp, config_store_type);
  if (!cfgstore) {
    std::cerr << "ERROR: failed to create config store" << std::endl;
    return 1;
  }

  rgw::SiteConfig site;
  int r = site.load(dpp, null_yield, cfgstore.get());
  if (r < 0) {
    std::cerr << "ERROR: failed to load site config (r=" << r << ")" << std::endl;
    return 1;
  }

  // no gc thread; the tests drive the processor themselves
  auto driver = DriverManager::get_storage(dpp,
                                           g_ceph_context,
                                           cfg,
                                           context_pool,
                                           site,
                                           false,  // use_gc_thread
                                           false,  // use_lc_thread
                                           false,  // use_restore_thread
                                           false,  // quota_threads
                                           false,  // run_sync_thread
                                           false,  // run_reshard_thread
                                           false,  // run_notification_thread
                                           false,  // run_bucket_logging_thread
                                           false,  // background_tasks
                                           null_yield,
                                           cfgstore.get(),
                                           false); // use_cache
  if (!driver) {
    std::cerr << "ERROR: failed to initialize SAL driver" << std::endl;
    return 1;
  }
  auto close_driver = make_scope_guard([&] {
      gc_ioctx.close();
      tail_ioctx.close();
      driver->shutdown();
      DriverManager::close_storage(driver);
    });

  store = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!store) {
    std::cerr << "ERROR: backend '" << cfg.store_name << "' is not rados" << std::endl;
    return 1;
  }

  const auto& zone = site.get_zone_params();
  r = rgw_init_ioctx(dpp, rgw_rados()->get_rados_handle(), zone.gc_pool, gc_ioctx);
  if (r < 0) {
    std::cerr << "ERROR: failed to open gc pool (r=" << r << ")" << std::endl;
    return 1;
  }
  const rgw_pool pool{zone.log_pool.name, "gc-test"};
  tail_pool = pool.to_str();
  r = rgw_init_ioctx(dpp, rgw_rados()->get_rados_handle(), pool, tail_ioctx);
  if (r < 0) {
    std::cerr << "ERROR: failed to open log pool (r=" << r << ")" << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

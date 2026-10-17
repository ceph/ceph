// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

// RGWGC against a live cluster: the legacy gc.N queue and the gc.fifo.N
// FIFOs.
//
// Uses shard 0 only (rgw_gc_max_objs=1) and drains it, so no RGW on the
// cluster may run GC threads (rgw_enable_gc_threads=false) or it will hold
// the gc.0 lock and process() will silently do nothing.

#include "gtest/gtest.h"

#include "rgw_gc.h"
#include "rgw_sal_rados.h"
#include "rgw_tools.h"
#include "rgw/rgw_sal_config.h"
#include "rgw/rgw_zone.h"
#include "rgw_gc_log.h"
#include "cls/rgw/cls_rgw_ops.h"
#include "cls/rgw_gc/cls_rgw_gc_client.h"
#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "common/common_init.h"
#include "common/dout.h"
#include "global/global_context.h"
#include "include/scope_guard.h"

#include "rgw_common.h"

#include <fmt/format.h>

#include <iostream>
#include <list>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

const DoutPrefixProvider* dpp = nullptr;
rgw::sal::RadosStore* store = nullptr;
librados::IoCtx gc_ioctx;
librados::IoCtx tail_ioctx;
std::string tail_pool;

const std::string gc_oid = "gc.0";

RGWRados* rgw_rados()
{
  return store->getRados();
}

void set_conf(std::string_view key, const std::string& val)
{
  g_conf().set_val_or_die(key, val);
}

struct Chain {
  std::string tag;
  std::vector<std::string> oids;
};

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

void enqueue_fifo(const std::vector<Chain>& chains, int min_wait)
{
  set_conf("rgw_gc_obj_min_wait", std::to_string(min_wait));
  for (const auto& c : chains) {
    auto chain = to_obj_chain(c);
    auto [r, leftover] = rgw_rados()->send_chain_to_gc(chain, c.tag, null_yield);
    ASSERT_EQ(0, r);
    ASSERT_FALSE(leftover);
  }
}

void enqueue_queue(const std::vector<Chain>& chains, uint32_t expiration_secs)
{
  for (const auto& c : chains) {
    cls_rgw_gc_obj_info info;
    info.tag = c.tag;
    info.chain = to_obj_chain(c);
    librados::ObjectWriteOperation op;
    cls_rgw_gc_queue_enqueue(op, expiration_secs, info);
    ASSERT_EQ(0, gc_ioctx.operate(gc_oid, &op));
  }
}

std::list<cls_rgw_gc_obj_info> list_entries(bool expired_only = false)
{
  std::list<cls_rgw_gc_obj_info> all;
  int index = 0;
  std::string marker;
  bool truncated = true;
  bool processing_fifo = false;
  while (truncated) {
    std::list<cls_rgw_gc_obj_info> entries;
    int r = rgw_rados()->list_gc_objs(index, marker, 1000, expired_only, entries,
                                      truncated, processing_fifo, 0);
    if (r < 0) {
      ADD_FAILURE() << "list_gc_objs returned " << r;
      break;
    }
    all.splice(all.end(), entries);
  }
  return all;
}

std::vector<std::string> list_tags(bool expired_only = false)
{
  std::vector<std::string> tags;
  for (const auto& e : list_entries(expired_only)) {
    tags.push_back(e.tag);
  }
  return tags;
}

size_t queue_length()
{
  size_t n = 0;
  std::string marker;
  bool truncated = true;
  while (truncated) {
    std::list<cls_rgw_gc_obj_info> entries;
    int r = cls_rgw_gc_queue_list_entries(gc_ioctx, gc_oid, marker, 1000, false,
                                          entries, truncated, marker);
    if (r < 0) {
      ADD_FAILURE() << "queue list returned " << r;
      break;
    }
    n += entries.size();
  }
  return n;
}

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

void drain()
{
  for (int i = 0; i < 100; ++i) {
    ASSERT_EQ(0, rgw_rados()->process_gc(false, null_yield, 0));
    if (list_tags().empty()) {
      return;
    }
  }
  FAIL() << "gc.0 did not drain";
}

// recreate the legacy queue the way RGWGC::initialize() does
void reset_queue()
{
  gc_ioctx.remove(gc_oid);
  librados::ObjectWriteOperation op;
  op.create(false);
  gc_log_init2(op, g_conf()->rgw_gc_max_queue_size, 0);
  EXPECT_EQ(0, gc_ioctx.operate(gc_oid, &op));
}

} // namespace

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

TEST_F(RgwGc, enqueue_goes_to_fifo_not_queue)
{
  const auto chains = make_chains(prefix, 3, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));

  EXPECT_EQ(0u, queue_length());
  EXPECT_EQ((std::vector<std::string>{chains[0].tag, chains[1].tag, chains[2].tag}),
            list_tags());
}

TEST_F(RgwGc, list_expired_only)
{
  const auto due = make_chains(prefix + ".due", 1, 2);
  const auto later = make_chains(prefix + ".later", 1, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(due, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(later, 3600));

  EXPECT_EQ((std::vector<std::string>{due[0].tag}), list_tags(true));
  EXPECT_EQ((std::vector<std::string>{due[0].tag, later[0].tag}), list_tags(false));
}

// page through one shard an entry at a time and check the cursor after each
// call. the second call is the queue -> fifo hand-off: the page ends exactly
// at the last queue entry, so cls_queue must report no more entries.
TEST_F(RgwGc, list_queue_then_fifo_step_by_step)
{
  const auto q = make_chains(prefix + ".q", 2, 1);
  const auto f = make_chains(prefix + ".f", 2, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue_queue(q, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(f, 0));

  struct Step {
    std::string tag;
    bool processing_fifo;
    bool truncated;
  };
  const std::vector<Step> expected = {
    {q[0].tag, false, true},
    {q[1].tag, true, true},
    {f[0].tag, true, true},
    {f[1].tag, false, false},
  };

  int index = 0;
  std::string marker;
  bool truncated = false;
  bool processing_fifo = false;
  for (const auto& step : expected) {
    SCOPED_TRACE(step.tag);
    std::list<cls_rgw_gc_obj_info> entries;
    ASSERT_EQ(0, rgw_rados()->list_gc_objs(index, marker, 1, false, entries,
                                           truncated, processing_fifo, 0));
    ASSERT_EQ(1u, entries.size());
    EXPECT_EQ(step.tag, entries.front().tag);
    EXPECT_EQ(step.processing_fifo, processing_fifo);
    EXPECT_EQ(step.truncated, truncated);
  }
}

// max is larger than the leftover queue, so one list() call returns the
// last queue entries and the first fifo entries together
TEST_F(RgwGc, list_queue_then_fifo_fills_remain)
{
  const auto q = make_chains(prefix + ".q", 2, 1);
  const auto f = make_chains(prefix + ".f", 2, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue_queue(q, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(f, 0));

  int index = 0;
  std::string marker;
  bool truncated = false;
  bool processing_fifo = false;
  std::list<cls_rgw_gc_obj_info> entries;
  ASSERT_EQ(0, rgw_rados()->list_gc_objs(index, marker, 3, false, entries,
                                         truncated, processing_fifo, 0));
  ASSERT_EQ(3u, entries.size());
  auto it = entries.begin();
  EXPECT_EQ(q[0].tag, it++->tag);
  EXPECT_EQ(q[1].tag, it++->tag);
  EXPECT_EQ(f[0].tag, it->tag);
  EXPECT_TRUE(processing_fifo);
  EXPECT_TRUE(truncated);

  entries.clear();
  ASSERT_EQ(0, rgw_rados()->list_gc_objs(index, marker, 3, false, entries,
                                         truncated, processing_fifo, 0));
  ASSERT_EQ(1u, entries.size());
  EXPECT_EQ(f[1].tag, entries.front().tag);
  EXPECT_FALSE(truncated);
}

TEST_F(RgwGc, process_drains_fifo)
{
  const auto chains = make_chains(prefix, 3, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));

  ASSERT_EQ(0, rgw_rados()->process_gc(false, null_yield, 0));
  EXPECT_TRUE(list_tags().empty());
  EXPECT_TRUE(objects_left(prefix).empty());
}

// process() handles 100 entries per batch; 250 needs three list/trim rounds,
// each resuming from the previous trim
TEST_F(RgwGc, process_drains_more_than_one_batch)
{
  const auto chains = make_chains(prefix, 250, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));
  ASSERT_EQ(250u, list_tags().size());

  ASSERT_EQ(0, rgw_rados()->process_gc(false, null_yield, 0));
  EXPECT_TRUE(list_tags().empty());
  EXPECT_TRUE(objects_left(prefix).empty());
}

TEST_F(RgwGc, process_expired_only_leaves_unexpired_fifo)
{
  const auto due = make_chains(prefix + ".due", 1, 2);
  const auto later = make_chains(prefix + ".later", 1, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(due, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(later, 3600));

  ASSERT_EQ(0, rgw_rados()->process_gc(true, null_yield, 0));
  EXPECT_EQ((std::vector<std::string>{later[0].tag}), list_tags());
  for (const auto& oid : later[0].oids) {
    EXPECT_EQ(0, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
  for (const auto& oid : due[0].oids) {
    EXPECT_EQ(-ENOENT, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
}

// expired-only queue list is empty (unexpired leftover) but fifo still
// has due entries. process() must not stop at the queue
TEST_F(RgwGc, process_expired_only_unexpired_queue_then_due_fifo)
{
  const auto later = make_chains(prefix + ".later", 1, 2);
  const auto due = make_chains(prefix + ".due", 1, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_queue(later, 3600));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(due, 0));

  ASSERT_EQ(0, rgw_rados()->process_gc(true, null_yield, 0));
  EXPECT_EQ(1u, queue_length());
  EXPECT_EQ((std::vector<std::string>{later[0].tag}), list_tags());
  for (const auto& oid : later[0].oids) {
    EXPECT_EQ(0, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
  for (const auto& oid : due[0].oids) {
    EXPECT_EQ(-ENOENT, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
}

TEST_F(RgwGc, process_expired_only_due_queue_leaves_unexpired_fifo)
{
  const auto due = make_chains(prefix + ".due", 1, 2);
  const auto later = make_chains(prefix + ".later", 1, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_queue(due, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(later, 3600));

  ASSERT_EQ(0, rgw_rados()->process_gc(true, null_yield, 0));
  EXPECT_EQ(0u, queue_length());
  EXPECT_EQ((std::vector<std::string>{later[0].tag}), list_tags());
  for (const auto& oid : later[0].oids) {
    EXPECT_EQ(0, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
  for (const auto& oid : due[0].oids) {
    EXPECT_EQ(-ENOENT, tail_ioctx.stat(oid, nullptr, nullptr)) << oid;
  }
}

// a small rgw_max_chunk_size splits one chain into several fifo entries
// under the same tag, and process() still removes every object
TEST_F(RgwGc, split_chain_enqueues_pieces)
{
  const auto chains = make_chains(prefix, 1, 6);
  const auto chain = to_obj_chain(chains[0]);

  cls_rgw_gc_set_entry_op op;
  op.info.tag = chains[0].tag;
  const size_t limit = op.estimate_encoded_size() +
    2 * chain.objs.front().estimate_encoded_size();

  const auto saved = std::to_string(g_conf()->rgw_max_chunk_size);
  auto restore = make_scope_guard([&] {
    set_conf("rgw_max_chunk_size", saved);
  });
  set_conf("rgw_max_chunk_size", std::to_string(limit));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));

  const auto entries = list_entries();
  ASSERT_EQ(3u, entries.size());
  std::vector<std::string> oids;
  for (const auto& e : entries) {
    EXPECT_EQ(chains[0].tag, e.tag);
    EXPECT_EQ(2u, e.chain.objs.size());
    for (const auto& o : e.chain.objs) {
      oids.push_back(o.key.name);
    }
  }
  EXPECT_EQ(chains[0].oids, oids);

  ASSERT_EQ(0, rgw_rados()->process_gc(false, null_yield, 0));
  EXPECT_TRUE(objects_left(prefix).empty());
}

// iterate over a large fifo with a page size that does not divide the count
TEST_F(RgwGc, list_pages_many_fifo_entries)
{
  constexpr size_t n = 500;
  const auto chains = make_chains(prefix, n, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));

  std::vector<std::string> tags;
  int index = 0;
  std::string marker;
  bool truncated = true;
  bool processing_fifo = false;
  while (truncated) {
    std::list<cls_rgw_gc_obj_info> entries;
    ASSERT_EQ(0, rgw_rados()->list_gc_objs(index, marker, 17, false, entries,
                                           truncated, processing_fifo, 0));
    ASSERT_LE(entries.size(), 17u);
    for (const auto& e : entries) {
      tags.push_back(e.tag);
    }
  }
  ASSERT_EQ(n, tags.size());
  for (size_t i = 0; i < n; ++i) {
    EXPECT_EQ(chains[i].tag, tags[i]);
  }
}

// leftover queue and fifo both larger than one process() batch of 100
TEST_F(RgwGc, process_drains_queue_then_fifo_many)
{
  const auto q = make_chains(prefix + ".q", 150, 1);
  const auto f = make_chains(prefix + ".f", 200, 1);
  ASSERT_NO_FATAL_FAILURE(enqueue_queue(q, 0));
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(f, 0));
  ASSERT_EQ(150u, queue_length());
  ASSERT_EQ(350u, list_tags().size());

  ASSERT_NO_FATAL_FAILURE(drain());
  EXPECT_EQ(0u, queue_length());
  EXPECT_TRUE(list_tags().empty());
  EXPECT_TRUE(objects_left(prefix).empty());
}

// the legacy queue is drain-only; an error reading it must not stop
// process() from draining the shard's fifo
TEST_F(RgwGc, broken_legacy_queue_does_not_block_fifo)
{
  const auto chains = make_chains(prefix, 3, 2);
  ASSERT_NO_FATAL_FAILURE(enqueue_fifo(chains, 0));

  auto restore = make_scope_guard([] { reset_queue(); });
  ASSERT_EQ(0, gc_ioctx.remove(gc_oid));
  bufferlist garbage;
  garbage.append("not a queue");
  ASSERT_EQ(0, gc_ioctx.write_full(gc_oid, garbage));

  EXPECT_EQ((std::vector<std::string>{chains[0].tag, chains[1].tag, chains[2].tag}),
            list_tags());
  ASSERT_EQ(0, rgw_rados()->process_gc(false, null_yield, 0));
  EXPECT_TRUE(objects_left(prefix).empty());
}

int main(int argc, char** argv)
{
  auto args = argv_to_vec(argc, const_cast<const char**>(argv));
  auto cct = rgw_global_init(nullptr, args, CEPH_ENTITY_TYPE_CLIENT,
                             CODE_ENVIRONMENT_UTILITY, 0);

  if (!g_conf()->rgw_region.empty() && g_conf()->rgw_zonegroup.empty()) {
    g_conf().set_val_or_die("rgw_zonegroup", g_conf()->rgw_region.c_str());
  }
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

  auto driver = DriverManager::get_storage(dpp,
                                           g_ceph_context,
                                           cfg,
                                           context_pool,
                                           site,
                                           false, // use_gc_thread
                                           false, // use_lc_thread
                                           false, // use_restore_thread
                                           false, // quota_threads
                                           false, // run_sync_thread
                                           false, // run_reshard_thread
                                           false, // run_notification_thread
                                           false, // run_bucket_logging_thread
                                           false, // background_tasks
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
    DriverManager::close_storage(driver);
  });

  store = dynamic_cast<rgw::sal::RadosStore*>(driver);
  if (!store) {
    std::cerr << "ERROR: backend '" << cfg.store_name << "' is not rados" << std::endl;
    return 1;
  }

  r = rgw_init_ioctx(dpp, rgw_rados()->get_rados_handle(),
                     site.get_zone_params().gc_pool, gc_ioctx);
  if (r < 0) {
    std::cerr << "ERROR: failed to open gc pool (r=" << r << ")" << std::endl;
    return 1;
  }

  const rgw_pool pool{site.get_zone_params().log_pool.name, "gc-test"};
  tail_pool = pool.to_str();
  r = rgw_init_ioctx(dpp, rgw_rados()->get_rados_handle(), pool, tail_ioctx);
  if (r < 0) {
    std::cerr << "ERROR: failed to open tail pool (r=" << r << ")" << std::endl;
    return 1;
  }

  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <chrono>
#include <map>
#include <memory>
#include <thread>

#include "gtest/gtest.h"

#include "global/global_context.h"
#include "global/global_init.h"
#include "common/common_init.h"

#include "osd/OSDMap.h"
#include "osd/scheduler/BfqScheduler.h"
#include "osd/scheduler/OpSchedulerItem.h"

using namespace ceph::osd::scheduler;

int main(int argc, char **argv) {
  std::vector<const char*> args(argv, argv+argc);
  auto cct = global_init(nullptr, args, CEPH_ENTITY_TYPE_OSD,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

namespace {

const std::vector<std::string> bfq_conf_keys = {
  "osd_bfq_client_group_weight",
  "osd_bfq_background_group_weight",
  "osd_bfq_client_block_weight",
  "osd_bfq_client_object_weight",
  "osd_bfq_client_object_meta_weight",
  "osd_bfq_client_file_weight",
  "osd_bfq_client_file_meta_weight",
  "osd_bfq_client_other_weight",
  "osd_bfq_background_recovery_weight",
  "osd_bfq_background_best_effort_weight",
  "osd_bfq_max_budget",
  "osd_bfq_min_cost",
  "osd_bfq_cost_per_op",
  "osd_bfq_cost_per_seek",
  "osd_bfq_budget_timeout"
};

}

class BfqSchedulerTest : public testing::Test {
public:
  int whoami = 0;
  uint32_t num_shards = 1;
  int shard_id = 0;
  bool is_rotational = false;
  unsigned cutoff_priority = 12;
  std::unique_ptr<BfqScheduler> q;

  void set_conf(const std::string &key, const std::string &val) {
    ASSERT_EQ(0, g_ceph_context->_conf.set_val(key, val));
  }

  // construct the scheduler after any config overrides so the
  // constructor picks them up
  void create_queue() {
    q = std::make_unique<BfqScheduler>(
      g_ceph_context, whoami, num_shards, shard_id, is_rotational,
      cutoff_priority);
  }

  void TearDown() override {
    q.reset();
    for (const auto &key : bfq_conf_keys) {
      g_ceph_context->_conf.rm_val(key);
    }
  }

  struct MockBfqItem : public PGOpQueueable {
    SchedulerClass scheduler_class;

    MockBfqItem(spg_t pgid, SchedulerClass _scheduler_class)
      : PGOpQueueable(pgid),
	scheduler_class(_scheduler_class) {}

    std::ostream &print(std::ostream &rhs) const final { return rhs; }

    std::string print() const final {
      return std::string();
    }

    std::optional<OpRequestRef> maybe_get_op() const final {
      return std::nullopt;
    }

    SchedulerClass get_scheduler_class() const final {
      return scheduler_class;
    }

    void run(OSD *osd, OSDShard *sdata, PGRef& pg,
	     ThreadPool::TPHandle &handle) final {}
  };
};

namespace {

spg_t make_pgid(int64_t pool) {
  return spg_t(pg_t(0, pool));
}

OpSchedulerItem create_item(
  epoch_t e, uint64_t owner, SchedulerClass klass,
  int64_t pool = 1, int cost = 4096, unsigned priority = 1)
{
  return OpSchedulerItem(
    std::make_unique<BfqSchedulerTest::MockBfqItem>(make_pgid(pool), klass),
    cost, priority, utime_t(), owner, e);
}

OpSchedulerItem create_high_prio_item(
  unsigned priority, epoch_t e, uint64_t owner, SchedulerClass klass)
{
  return OpSchedulerItem(
    std::make_unique<BfqSchedulerTest::MockBfqItem>(make_pgid(1), klass),
    4096, priority, utime_t(), owner, e);
}

OpSchedulerItem get_item(WorkItem item)
{
  // throws (fails the test) if the variant holds monostate or a
  // future time -- bfq must always return a real item
  return std::move(std::get<OpSchedulerItem>(item));
}

using app_md_t = std::map<std::string, std::map<std::string, std::string>>;

int64_t add_pool(OSDMap &map, const std::string &name, app_md_t apps,
		 bool bulk = false, float bias = 1.0f,
		 const std::string &qos_group = "")
{
  OSDMap::Incremental inc(map.get_epoch() + 1);
  inc.fsid = map.get_fsid();
  inc.new_pool_max = map.get_pool_max();
  const int64_t pool_id = ++inc.new_pool_max;
  pg_pool_t empty;
  pg_pool_t *p = inc.get_new_pool(pool_id, &empty);
  p->type = pg_pool_t::TYPE_REPLICATED;
  p->size = 3;
  p->crush_rule = 0;
  p->set_pg_num(8);
  p->set_pgp_num(8);
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  if (bulk) {
    p->set_flag(pg_pool_t::FLAG_BULK);
  }
  if (bias != 1.0f) {
    p->opts.set(pool_opts_t::PG_AUTOSCALE_BIAS, static_cast<double>(bias));
  }
  if (!qos_group.empty()) {
    p->opts.set(pool_opts_t::QOS_GROUP, qos_group);
  }
  p->application_metadata = std::move(apps);
  inc.new_pool_names[pool_id] = name;
  map.apply_incremental(inc);
  return pool_id;
}

void set_map_qos_groups(OSDMap &map,
			const std::map<std::string, uint32_t> &groups)
{
  OSDMap::Incremental inc(map.get_epoch() + 1);
  inc.fsid = map.get_fsid();
  for (const auto &name : map.get_qos_groups()) {
    if (!groups.count(name.first)) {
      inc.old_qos_groups.push_back(name.first);
    }
  }
  for (const auto &[name, weight] : groups) {
    inc.set_qos_group(name, qos_group_t{weight});
  }
  map.apply_incremental(inc);
}

} // anonymous namespace

TEST_F(BfqSchedulerTest, TestEmpty) {
  create_queue();
  ASSERT_TRUE(q->empty());

  for (unsigned i = 100; i < 105; i += 2) {
    q->enqueue(create_item(i, 1, SchedulerClass::client));
  }
  ASSERT_FALSE(q->empty());

  std::list<OpSchedulerItem> reqs;
  reqs.push_back(get_item(q->dequeue()));
  reqs.push_back(get_item(q->dequeue()));
  ASSERT_EQ(2u, reqs.size());
  ASSERT_FALSE(q->empty());

  for (auto &&i : reqs) {
    q->enqueue_front(std::move(i));
  }
  reqs.clear();
  ASSERT_FALSE(q->empty());

  for (int i = 0; i < 3; ++i) {
    ASSERT_FALSE(q->empty());
    get_item(q->dequeue());
  }
  ASSERT_TRUE(q->empty());
}

TEST_F(BfqSchedulerTest, TestSingleStreamFifo) {
  create_queue();
  for (unsigned i = 100; i < 105; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client));
  }
  for (unsigned i = 100; i < 105; ++i) {
    auto r = get_item(q->dequeue());
    ASSERT_EQ(i, r.get_map_epoch());
  }
  ASSERT_TRUE(q->empty());
}

TEST_F(BfqSchedulerTest, TestImmediateAndCutoffOrdering) {
  create_queue();
  ASSERT_TRUE(q->empty());

  // fair-queue items
  for (unsigned i = 100; i < 102; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client));
  }
  // immediate items
  for (unsigned i = 103; i < 105; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::immediate));
  }
  // strict high-priority items (priority >= cutoff of 12)
  for (unsigned i = 200; i < 202; ++i) {
    q->enqueue(create_high_prio_item(i - 180, i, 1, SchedulerClass::client));
  }

  // immediate class first
  ASSERT_EQ(103u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(104u, get_item(q->dequeue()).get_map_epoch());
  // then the strict queue, higher priority first
  ASSERT_EQ(201u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(200u, get_item(q->dequeue()).get_map_epoch());
  // the fair hierarchy drains last
  ASSERT_EQ(100u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(101u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_TRUE(q->empty());
}

TEST_F(BfqSchedulerTest, TestEnqueueFront) {
  create_queue();
  for (unsigned i = 100; i < 104; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client));
  }
  auto r = get_item(q->dequeue());
  ASSERT_EQ(100u, r.get_map_epoch());
  q->enqueue_front(std::move(r));
  // the requeued item bypasses the fair hierarchy and comes back first
  ASSERT_EQ(100u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(101u, get_item(q->dequeue()).get_map_epoch());
}

TEST_F(BfqSchedulerTest, TestHighPriorityBandOrdering) {
  create_queue();
  // two strict-queue items in the same priority band drain FIFO...
  q->enqueue(create_high_prio_item(20, 100, 1, SchedulerClass::client));
  q->enqueue(create_high_prio_item(20, 101, 1, SchedulerClass::client));
  // ...and a front-enqueued item in the same band jumps to its head.
  // Pins the band orientation: dequeue() pops the BACK of each band's
  // list, so enqueue_high() must push_front for FIFO arrivals and
  // push_back for front insertion, an easy pair to invert
  q->enqueue_front(create_high_prio_item(20, 102, 1, SchedulerClass::client));
  ASSERT_EQ(102u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(100u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_EQ(101u, get_item(q->dequeue()).get_map_epoch());
  ASSERT_TRUE(q->empty());
}

TEST_F(BfqSchedulerTest, TestClientStreamWeights) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  set_conf("osd_bfq_client_block_weight", "300");
  set_conf("osd_bfq_client_object_weight", "100");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object}
  });

  constexpr unsigned per_stream = 600;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // both streams stay backlogged over the measurement window
  std::map<uint64_t, unsigned> count;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++count[get_item(q->dequeue()).get_owner()];
  }
  // weights 300:100 -> expect a 3:1 split of the 400 dequeues
  ASSERT_NEAR(300, count[1], 30);
  ASSERT_NEAR(100, count[2], 30);
}

TEST_F(BfqSchedulerTest, TestGroupWeights) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  set_conf("osd_bfq_client_group_weight", "100");
  set_conf("osd_bfq_background_group_weight", "25");
  create_queue();

  constexpr unsigned per_stream = 600;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 5));
    q->enqueue(create_item(i, 2, SchedulerClass::background_recovery, 5));
  }

  std::map<uint64_t, unsigned> count;
  for (unsigned i = 0; i < 500; ++i) {
    ASSERT_FALSE(q->empty());
    ++count[get_item(q->dequeue()).get_owner()];
  }
  // group weights 100:25 -> expect a 4:1 split of the 500 dequeues
  ASSERT_NEAR(400, count[1], 40);
  ASSERT_NEAR(100, count[2], 40);
}

TEST_F(BfqSchedulerTest, TestBudgetRotation) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object}
  });

  constexpr unsigned per_stream = 64;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // with equal weights and 4-item budgets, service alternates in
  // budget-sized runs rather than per-op interleave
  uint64_t prev_owner = 0;
  unsigned switches = 0;
  for (unsigned i = 0; i < 2 * per_stream; ++i) {
    auto owner = get_item(q->dequeue()).get_owner();
    if (prev_owner && owner != prev_owner) {
      ++switches;
    }
    prev_owner = owner;
  }
  ASSERT_TRUE(q->empty());
  // strict alternation would switch 127 times; 4-item rounds yield
  // roughly 2*64/4 = 32 switches (a few more during budget ramp-up)
  ASSERT_GE(switches, 2u);
  ASSERT_LT(switches, 64u);
}

TEST_F(BfqSchedulerTest, TestPoolClassification) {
  create_queue();

  // no pool map yet: every client op classifies as client_other
  auto unmapped = create_item(1, 1, SchedulerClass::client, 42);
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_other), q->classify(unmapped));

  q->set_pool_streams({
    {42, bfq_stream_t::client_block},
    {7, bfq_stream_t::client_file}
  });
  auto block = create_item(1, 1, SchedulerClass::client, 42);
  auto file = create_item(1, 1, SchedulerClass::client, 7);
  auto other = create_item(1, 1, SchedulerClass::client, 9);
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_block), q->classify(block));
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_file), q->classify(file));
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_other), q->classify(other));

  // background classes ignore the pool
  auto rec = create_item(1, 1, SchedulerClass::background_recovery, 42);
  auto be = create_item(1, 1, SchedulerClass::background_best_effort, 42);
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::background_recovery), q->classify(rec));
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::background_best_effort),
	    q->classify(be));
}

TEST_F(BfqSchedulerTest, TestOsdmapDataVsMetaRefinement) {
  create_queue();

  OSDMap map;
  uuid_d fsid;
  map.build_simple(g_ceph_context, 0, fsid, 1);

  const auto rbd = add_pool(map, "rbd", {{"rbd", {}}});
  // rgw pools split on the explicit traffic-class key only
  const auto rgw_plain = add_pool(map, "rgw.buckets.data", {{"rgw", {}}});
  const auto rgw_meta = add_pool(
    map, "rgw.buckets.index",
    {{"rgw", {{"traffic-class", "metadata"}}}});
  const auto rgw_data_class = add_pool(
    map, "rgw.tagged.data", {{"rgw", {{"traffic-class", "data"}}}});
  const auto rgw_bogus_class = add_pool(
    map, "rgw.bogus", {{"rgw", {{"traffic-class", "latency-9000"}}}});
  // cephfs pools carry the mon-stamped "data"/"metadata" markers
  const auto fs_data = add_pool(map, "cephfs_data",
				{{"cephfs", {{"data", "fsname"}}}});
  const auto fs_meta = add_pool(map, "cephfs_metadata",
				{{"cephfs", {{"metadata", "fsname"}}}});
  // ...but an explicit traffic-class beats the native marker...
  const auto fs_override = add_pool(
    map, "cephfs_md_as_data",
    {{"cephfs", {{"metadata", "fsname"}, {"traffic-class", "data"}}}});
  // ...and a bare tag (pre-Luminous upgrade artifact) serves data
  const auto fs_bare = add_pool(map, "cephfs_old", {{"cephfs", {}}});
  // no metadata stream exists for block: the tag is ignored
  const auto rbd_meta_class = add_pool(
    map, "rbd.tagged", {{"rbd", {{"traffic-class", "metadata"}}}});
  // autoscaler hints (bulk, pg_autoscale_bias) are NOT classification
  // signals: absent on pre-created pools, inert under ratio-driven
  // autoscaling
  const auto rgw_biased = add_pool(map, "rgw.biased", {{"rgw", {}}},
				   false, 4.0f);
  const auto rgw_bulk = add_pool(
    map, "rgw.bulk.meta",
    {{"rgw", {{"traffic-class", "metadata"}}}}, true, 1.0f);
  const auto untagged = add_pool(map, "untagged", {});
  const auto ambiguous = add_pool(map, "shared",
				  {{"rbd", {}}, {"rgw", {}}});

  q->update_from_osdmap(map);

  auto leaf_of = [&](int64_t pool) {
    return q->classify(create_item(1, 1, SchedulerClass::client, pool));
  };
  auto leaf = [](bfq_stream_t s) { return bfq_leaf_of(s); };
  ASSERT_EQ(leaf(bfq_stream_t::client_block), leaf_of(rbd));
  ASSERT_EQ(leaf(bfq_stream_t::client_object), leaf_of(rgw_plain));
  ASSERT_EQ(leaf(bfq_stream_t::client_object_meta), leaf_of(rgw_meta));
  ASSERT_EQ(leaf(bfq_stream_t::client_object), leaf_of(rgw_data_class));
  ASSERT_EQ(leaf(bfq_stream_t::client_object), leaf_of(rgw_bogus_class));
  ASSERT_EQ(leaf(bfq_stream_t::client_file), leaf_of(fs_data));
  ASSERT_EQ(leaf(bfq_stream_t::client_file_meta), leaf_of(fs_meta));
  ASSERT_EQ(leaf(bfq_stream_t::client_file), leaf_of(fs_override));
  ASSERT_EQ(leaf(bfq_stream_t::client_file), leaf_of(fs_bare));
  ASSERT_EQ(leaf(bfq_stream_t::client_block), leaf_of(rbd_meta_class));
  ASSERT_EQ(leaf(bfq_stream_t::client_object), leaf_of(rgw_biased));
  ASSERT_EQ(leaf(bfq_stream_t::client_object_meta), leaf_of(rgw_bulk));
  ASSERT_EQ(leaf(bfq_stream_t::client_other), leaf_of(untagged));
  ASSERT_EQ(leaf(bfq_stream_t::client_other), leaf_of(ambiguous));
}

TEST_F(BfqSchedulerTest, TestQosGroupClassification) {
  create_queue();

  OSDMap map;
  uuid_d fsid;
  map.build_simple(g_ceph_context, 0, fsid, 1);
  set_map_qos_groups(map, {
    {"gold", 500},
    {"silver", 100}
  });

  // an explicit qos_group pool option beats the derived stream
  const auto rbd_gold = add_pool(map, "rbd.gold", {{"rbd", {}}},
				 false, 1.0f, "gold");
  // an untagged pool can be steered too
  const auto plain_silver = add_pool(map, "plain.silver", {},
				     false, 1.0f, "silver");
  // a reference to an undefined group falls back to the derived stream
  const auto rbd_dangling = add_pool(map, "rbd.dangling", {{"rbd", {}}},
				     false, 1.0f, "tin");
  const auto rbd_plain = add_pool(map, "rbd", {{"rbd", {}}});

  q->update_from_osdmap(map);

  auto leaf_of = [&](int64_t pool) {
    return q->classify(create_item(1, 1, SchedulerClass::client, pool));
  };
  const auto gold_leaf = leaf_of(rbd_gold);
  const auto silver_leaf = leaf_of(plain_silver);
  ASSERT_GE(gold_leaf, bfq_num_streams);
  ASSERT_GE(silver_leaf, bfq_num_streams);
  ASSERT_NE(gold_leaf, silver_leaf);
  ASSERT_EQ(bfq_group_t::client, bfq_group_of_leaf(gold_leaf));
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_block), leaf_of(rbd_dangling));
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_block), leaf_of(rbd_plain));

  // removing a group reroutes its pools to the derived classification;
  // surviving groups keep their leaves
  set_map_qos_groups(map, {{"silver", 100}});
  q->update_from_osdmap(map);
  ASSERT_EQ(bfq_leaf_of(bfq_stream_t::client_block), leaf_of(rbd_gold));
  ASSERT_EQ(silver_leaf, leaf_of(plain_silver));
}

TEST_F(BfqSchedulerTest, TestQosGroupWeights) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();
  auto leaves = q->set_qos_groups({
    {"gold", 300},
    {"silver", 100}
  });
  q->set_pool_leaves({
    {1, leaves.at("gold")},
    {2, leaves.at("silver")}
  });

  constexpr unsigned per_stream = 800;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // user-defined leaves schedule like built-in streams: weights
  // 300:100 -> a 3:1 split of the 400 dequeues
  std::map<uint64_t, unsigned> count;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++count[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(300, count[1], 30);
  ASSERT_NEAR(100, count[2], 30);

  // swap the weights through a table refresh (the OSDMap path calls
  // the same update): felt when each leaf is next (re)tagged
  q->set_qos_groups({
    {"gold", 100},
    {"silver", 300}
  });
  std::map<uint64_t, unsigned> after;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++after[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(100, after[1], 40);
  ASSERT_NEAR(300, after[2], 40);
}

TEST_F(BfqSchedulerTest, TestQosGroupVsBuiltinWeights) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  set_conf("osd_bfq_client_block_weight", "100");
  create_queue();
  auto leaves = q->set_qos_groups({{"gold", 300}});
  q->set_pool_leaves({
    {1, leaves.at("gold")},
    {2, bfq_leaf_of(bfq_stream_t::client_block)}
  });

  constexpr unsigned per_stream = 600;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // qos group leaves compete as siblings of the built-in streams
  // under the client group: weights 300:100 -> a 3:1 split
  std::map<uint64_t, unsigned> count;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++count[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(300, count[1], 30);
  ASSERT_NEAR(100, count[2], 30);
}

TEST_F(BfqSchedulerTest, TestQosGroupLifecycle) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();
  auto leaves = q->set_qos_groups({{"gold", 300}});
  const auto gold = leaves.at("gold");
  q->set_pool_leaves({{1, gold}});

  for (unsigned i = 0; i < 10; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
  }

  // the group vanishes from the table while its queue is backlogged:
  // the leaf must keep scheduling (with its last weight) until empty
  q->set_qos_groups({});
  q->set_pool_leaves({});
  unsigned drained = 0;
  while (!q->empty()) {
    get_item(q->dequeue());
    ++drained;
  }
  ASSERT_EQ(10u, drained);

  // the drained slot is recycled for the next new group, not leaked
  auto leaves2 = q->set_qos_groups({{"bronze", 50}});
  ASSERT_EQ(gold, leaves2.at("bronze"));

  // and a known group keeps its leaf across table refreshes
  auto leaves3 = q->set_qos_groups({{"bronze", 500}});
  ASSERT_EQ(leaves2.at("bronze"), leaves3.at("bronze"));

  // items enqueued to a recycled leaf drain normally
  q->set_pool_leaves({{2, leaves3.at("bronze")}});
  q->enqueue(create_item(1, 2, SchedulerClass::client, 2));
  ASSERT_FALSE(q->empty());
  get_item(q->dequeue());
  ASSERT_TRUE(q->empty());
}

TEST_F(BfqSchedulerTest, TestQosGroupAdditionKeepsIdleSlots) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();

  // silver is defined alone, serves a burst and goes idle: its slot
  // is drained and off the tree, exactly what a recyclable slot of a
  // removed group looks like...
  auto leaves = q->set_qos_groups({{"silver", 100}});
  const auto silver = leaves.at("silver");
  q->set_pool_leaves({{1, silver}});
  for (unsigned i = 0; i < 8; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
  }
  while (!q->empty()) {
    get_item(q->dequeue());
  }

  // ...but silver is still defined when gold, which sorts before it
  // in the table, is added.  Reconciliation must judge recyclability
  // against the whole incoming table, not against a visited flag:
  // silver keeps its leaf (and with it its adapted budget and
  // virtual-time memory) and gold lands on a fresh one
  auto leaves2 = q->set_qos_groups({{"gold", 300}, {"silver", 100}});
  ASSERT_EQ(silver, leaves2.at("silver"));
  ASSERT_NE(silver, leaves2.at("gold"));
  ASSERT_GE(leaves2.at("gold"), bfq_num_streams);

  // and the same table applied again changes nothing
  auto leaves3 = q->set_qos_groups({{"gold", 300}, {"silver", 100}});
  ASSERT_EQ(leaves2, leaves3);
}

TEST_F(BfqSchedulerTest, TestMetaStreamWeights) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  set_conf("osd_bfq_client_object_meta_weight", "300");
  set_conf("osd_bfq_client_object_weight", "100");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_object_meta},
    {2, bfq_stream_t::client_object}
  });

  constexpr unsigned per_stream = 600;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // the metadata stream competes as a first-class leaf: weights
  // 300:100 -> a 3:1 split of the 400 dequeues
  std::map<uint64_t, unsigned> count;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++count[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(300, count[1], 30);
  ASSERT_NEAR(100, count[2], 30);
}

TEST_F(BfqSchedulerTest, TestAdditiveCostModel) {
  set_conf("osd_bfq_min_cost", "65536");
  create_queue();
  // default model: pure bytes above the min_cost floor
  ASSERT_EQ(65536u, q->calc_scaled_cost(4096));
  ASSERT_EQ(1u << 20, q->calc_scaled_cost(1 << 20));

  // a per-op constant switches to the additive model (fixed
  // overhead + bytes) and the min_cost floor no longer applies
  set_conf("osd_bfq_cost_per_op", "16384");
  create_queue();
  ASSERT_EQ(16384u + 4096u, q->calc_scaled_cost(4096));
  ASSERT_EQ(16384u + 1u, q->calc_scaled_cost(0));

  // the seek term is charged on rotational media only: on flash it is
  // ignored outright -- it neither adds to the charge nor, by itself,
  // switches off the floor -- so one cluster-wide value is safe for a
  // mixed fleet...
  set_conf("osd_bfq_cost_per_seek", "524288");
  create_queue();
  ASSERT_EQ(16384u + 4096u, q->calc_scaled_cost(4096));
  g_ceph_context->_conf.rm_val("osd_bfq_cost_per_op");
  create_queue();
  ASSERT_EQ(65536u, q->calc_scaled_cost(4096));

  // ...while on a rotational device it is added, and is enough on its
  // own to select the additive model
  is_rotational = true;
  create_queue();
  ASSERT_EQ(524288u + 4096u, q->calc_scaled_cost(4096));
  set_conf("osd_bfq_cost_per_op", "16384");
  create_queue();
  ASSERT_EQ(16384u + 524288u + 4096u, q->calc_scaled_cost(4096));
  ASSERT_EQ(16384u + 524288u + 1u, q->calc_scaled_cost(0));
}

TEST_F(BfqSchedulerTest, TestAutoBudgetByMediaType) {
  // osd_bfq_max_budget = 0 (the default) sizes the budget by media
  // type: rotation latency is bandwidth-proportional, so flash gets a
  // smaller budget than a rotational device
  create_queue();
  ASSERT_EQ(1u << 20, q->get_max_budget());

  is_rotational = true;
  create_queue();
  ASSERT_EQ(8u << 20, q->get_max_budget());

  // an explicit value overrides auto sizing on any media
  set_conf("osd_bfq_max_budget", "2097152");
  create_queue();
  ASSERT_EQ(2u << 20, q->get_max_budget());
}

TEST_F(BfqSchedulerTest, TestDrain) {
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object},
    {3, bfq_stream_t::client_file}
  });

  unsigned enqueued = 0;
  for (unsigned i = 0; i < 20; ++i) {
    for (int64_t pool = 1; pool <= 4; ++pool) {
      q->enqueue(create_item(i, pool, SchedulerClass::client, pool));
      ++enqueued;
    }
    q->enqueue(create_item(i, 5, SchedulerClass::background_recovery));
    q->enqueue(create_item(i, 6, SchedulerClass::background_best_effort));
    q->enqueue(create_item(i, 7, SchedulerClass::immediate));
    enqueued += 3;
  }
  q->enqueue(create_high_prio_item(20, 1000, 8, SchedulerClass::client));
  ++enqueued;

  // the fast-shutdown pattern: dequeue until empty must terminate
  unsigned drained = 0;
  while (!q->empty()) {
    get_item(q->dequeue());
    ++drained;
  }
  ASSERT_EQ(enqueued, drained);
}

TEST_F(BfqSchedulerTest, TestOversizedItemRotation) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object}
  });

  // stream A leads with an item 4x the entire budget
  q->enqueue(create_item(100, 1, SchedulerClass::client, 1, 65536));
  for (unsigned i = 101; i < 104; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
  }
  for (unsigned i = 200; i < 204; ++i) {
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // the oversized item must dispatch (budget goes negative, no hang)...
  ASSERT_EQ(100u, get_item(q->dequeue()).get_map_epoch());
  // ...and the overrun charge makes stream A ineligible, rotating to B
  ASSERT_EQ(200u, get_item(q->dequeue()).get_map_epoch());

  unsigned drained = 2;
  while (!q->empty()) {
    get_item(q->dequeue());
    ++drained;
  }
  ASSERT_EQ(8u, drained);
}

TEST_F(BfqSchedulerTest, TestBudgetTimeoutExpiry) {
  // a budget that covers either stream's whole backlog in one round
  // (12 x 4 KiB against an initial budget of max_budget / 2), so
  // that only the timeout can end a round early
  set_conf("osd_bfq_max_budget", "1048576");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "1");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object}
  });

  constexpr unsigned per_stream = 12;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // whichever stream wins the first round, the round outlives its
  // timeout during the sleep; the next fair dequeue must expire it and
  // rotate to the competitor although the first stream is still
  // backlogged and well within budget.  Without timeout handling the
  // first stream would drain all twelve of its items first.
  const uint64_t first = get_item(q->dequeue()).get_owner();
  const uint64_t other = first == 1 ? 2 : 1;
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  ASSERT_EQ(other, get_item(q->dequeue()).get_owner());

  // each stream still drains in FIFO order across the rotations
  std::map<uint64_t, unsigned> next_epoch = {{first, 1}, {other, 1}};
  unsigned drained = 2;
  while (!q->empty()) {
    auto r = get_item(q->dequeue());
    ASSERT_EQ(next_epoch[r.get_owner()]++, r.get_map_epoch());
    ++drained;
  }
  ASSERT_EQ(2 * per_stream, drained);
}

TEST_F(BfqSchedulerTest, TestRuntimeWeightChange) {
  set_conf("osd_bfq_max_budget", "16384");
  set_conf("osd_bfq_min_cost", "4096");
  set_conf("osd_bfq_budget_timeout", "100000");
  create_queue();
  q->set_pool_streams({
    {1, bfq_stream_t::client_block},
    {2, bfq_stream_t::client_object}
  });

  constexpr unsigned per_stream = 800;
  for (unsigned i = 0; i < per_stream; ++i) {
    q->enqueue(create_item(i, 1, SchedulerClass::client, 1));
    q->enqueue(create_item(i, 2, SchedulerClass::client, 2));
  }

  // equal weights: roughly even split
  std::map<uint64_t, unsigned> before;
  for (unsigned i = 0; i < 100; ++i) {
    ++before[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(50, before[1], 15);

  // raise block weight at runtime; the observer marks the config
  // dirty and the next dequeue folds it in at round re-tag time
  set_conf("osd_bfq_client_block_weight", "300");
  g_ceph_context->_conf.apply_changes(nullptr);

  std::map<uint64_t, unsigned> after;
  for (unsigned i = 0; i < 400; ++i) {
    ASSERT_FALSE(q->empty());
    ++after[get_item(q->dequeue()).get_owner()];
  }
  ASSERT_NEAR(300, after[1], 40);
  ASSERT_NEAR(100, after[2], 40);
}

// ---- bfq_detail::BfqServiceTree unit tests ----

using bfq_detail::BfqServiceTree;

TEST(BfqServiceTree, ActivationAndBackShift) {
  BfqServiceTree t(2);
  ASSERT_FALSE(t.has_active());
  ASSERT_EQ(std::nullopt, t.select());

  t.activate(0, 1000, 100);  // S=0, F=10
  t.activate(1, 1000, 100);  // S=0, F=10
  ASSERT_TRUE(t.has_active());
  ASSERT_DOUBLE_EQ(0.0, t.entity(0).start);
  ASSERT_DOUBLE_EQ(10.0, t.entity(0).finish);

  // tie on F: first index wins
  auto sel = t.select();
  ASSERT_TRUE(sel);
  ASSERT_EQ(0u, *sel);

  t.charge(500);  // V = 500/200 = 2.5

  // entity 0 consumed only half its allotment: back-shift F to 5,
  // re-tag S=5, F=5+10=15
  t.expire(0, 500, 1000, 100, true);
  ASSERT_DOUBLE_EQ(5.0, t.entity(0).start);
  ASSERT_DOUBLE_EQ(15.0, t.entity(0).finish);

  // entity 1 (F=10, eligible) now beats entity 0 (F=15)
  sel = t.select();
  ASSERT_TRUE(sel);
  ASSERT_EQ(1u, *sel);
}

TEST(BfqServiceTree, EligibilityGate) {
  BfqServiceTree t(2);
  t.activate(0, 1000, 10);   // S=0, F=100 (light weight)
  t.activate(1, 100, 100);   // S=0, F=1

  auto sel = t.select();
  ASSERT_TRUE(sel);
  ASSERT_EQ(1u, *sel);

  t.charge(100);             // V = 100/110 ~ 0.909
  t.expire(1, 100, 100, 100, true);  // F1=1 -> S1=1, F1=2

  // entity 1 has the smaller F (2 < 100) but S1=1 > V, so it is not
  // eligible; WF2Q+ must pick entity 0 instead of letting 1 run ahead
  sel = t.select();
  ASSERT_TRUE(sel);
  ASSERT_EQ(0u, *sel);

  // when only ineligible entities remain, V jumps forward
  t.expire(0, 1000, 1000, 10, false);  // deactivate 0
  sel = t.select();
  ASSERT_TRUE(sel);
  ASSERT_EQ(1u, *sel);

  // full idle renormalizes the tree
  t.expire(1, 100, 100, 100, false);
  ASSERT_FALSE(t.has_active());
  ASSERT_DOUBLE_EQ(0.0, t.get_vtime());
  ASSERT_DOUBLE_EQ(0.0, t.entity(0).finish);
}

TEST(BfqServiceTree, WeightedShare) {
  BfqServiceTree t(2);
  t.activate(0, 1000, 200);
  t.activate(1, 1000, 100);

  std::array<unsigned, 2> rounds = {0, 0};
  for (unsigned i = 0; i < 300; ++i) {
    auto sel = t.select();
    ASSERT_TRUE(sel);
    ++rounds[*sel];
    t.charge(1000);
    t.expire(*sel, 1000, 1000, *sel == 0 ? 200 : 100, true);
  }
  // weights 200:100 with equal allotments -> 2:1 round share
  ASSERT_NEAR(200, rounds[0], 10);
  ASSERT_NEAR(100, rounds[1], 10);
}

// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <gtest/gtest.h>
#include <gtest/gtest-spi.h>
#include "test/osd/ECPeeringTestFixture.h"
#include "test/osd/TestCommon.h"

using namespace std;

namespace {

/* One step of an op on a single object: truncate to off, or write or zero
 * len bytes at off. Written data is chosen when the op is submitted.
 */
struct ObjOp {
  enum Type { TRUNCATE, WRITE, ZERO } type;
  uint64_t off;
  uint64_t len;
};

ObjOp Truncate(uint64_t size) { return {ObjOp::TRUNCATE, size, 0}; }
ObjOp Write(uint64_t off, uint64_t len) { return {ObjOp::WRITE, off, len}; }
ObjOp Zero(uint64_t off, uint64_t len) { return {ObjOp::ZERO, off, len}; }

/* An object created with size bytes of random data. The ops in committed
 * are then applied one at a time, each committing before the next. Each
 * entry of ops is one op under test; they are all in flight together.
 */
struct ObjectScenario {
  uint64_t size;
  std::vector<std::vector<ObjOp>> committed;
  std::vector<std::vector<ObjOp>> ops;
};

}  // namespace

/**
 * ECTruncateTestBase - EC peering fixture configured from a BackendConfig,
 * with helpers that submit ops mixing truncates, writes and zeros while
 * keeping a model of the expected contents of each object, and that check
 * every shard of an object against its model.
 */
class ECTruncateTestBase : public ECPeeringTestFixture {
public:
  explicit ECTruncateTestBase(const BackendConfig& config) {
    k = config.k;
    m = config.m;
    stripe_unit = config.stripe_unit;
    ec_plugin = config.ec_plugin;
    ec_technique = config.ec_technique;
    pool_flags = config.pool_flags;
    num_zones = config.num_zones;
  }

protected:
  /* Read the head object of every shard directly from the store. */
  std::map<int, bufferlist> read_shards(const std::string& obj_name) {
    const hobject_t hoid = make_test_object(obj_name);
    std::map<int, bufferlist> shards;
    for (int shard = 0; shard < k + m; ++shard) {
      ghobject_t ghoid(hoid, ghobject_t::NO_GEN, shard_id_t(shard));
      TestPG* test_pg = find_test_pg_for_shard(shard);
      ceph_assert(test_pg);
      EXPECT_LE(0, get_osd_fixture(test_pg->pg_whoami.osd)->store->read(
        test_pg->ch, ghoid, 0, 0, shards[shard]))
        << "shard " << shard;
    }
    return shards;
  }

  ECUtil::stripe_info_t get_sinfo() {
    return ECUtil::stripe_info_t(ec_impl, &get_pool(), stripe_unit * k);
  }

  int get_shard(unsigned raw_shard) {
    return int(get_sinfo().get_shard(raw_shard_id_t(raw_shard)));
  }

  /* Queue ops, in order, as a single PGTransaction on an existing object on
   * the primary, and apply them to model, which must hold the contents of the
   * object once every op queued before is applied. The returned result is
   * set when the op completes.
   */
  std::shared_ptr<int> queue_ops(const std::string& obj_name,
                                 const std::vector<ObjOp>& ops,
                                 std::string& model) {
    const uint64_t pre_op_size = model.size();
    std::vector<std::pair<ObjOp, bufferlist>> steps;
    for (const auto& op : ops) {
      bufferlist bl;
      switch (op.type) {
      case ObjOp::TRUNCATE:
        model.resize(op.off, '\0');
        break;
      case ObjOp::WRITE:
        bl = create_random_buffer(op.len);
        if (model.size() < op.off + op.len) {
          model.resize(op.off + op.len, '\0');
        }
        model.replace(op.off, op.len, bl.c_str(), op.len);
        break;
      case ObjOp::ZERO:
        ceph_assert(op.off + op.len <= model.size());
        model.replace(op.off, op.len, op.len, '\0');
        break;
      }
      steps.emplace_back(op, bl);
    }

    const uint64_t new_size = model.size();
    std::shared_ptr<int> result;
    run_primary_op(
      [&](std::shared_ptr<int> r) {
        result = r;
        do_ops_impl(obj_name, steps, pre_op_size, new_size, r);
      }, false);
    ceph_assert(result);
    return result;
  }

  int submit_ops(const std::string& obj_name,
                 const std::vector<ObjOp>& ops,
                 std::string& model) {
    auto result = queue_ops(obj_name, ops, model);
    event_loop->run_until_idle();
    return *result;
  }

  void do_ops_impl(const std::string& obj_name,
                   const std::vector<std::pair<ObjOp, bufferlist>>& steps,
                   uint64_t pre_op_size,
                   uint64_t new_size,
                   std::shared_ptr<int> result) {
    hobject_t hoid = make_test_object(obj_name);
    PGTransactionUPtr pg_t = std::make_unique<PGTransaction>();

    ObjectContextRef obc = get_object_context(hoid, false);
    ceph_assert(obc);
    ceph_assert(obc->obs.oi.size == pre_op_size);
    pg_t->obc_map[hoid] = obc;
    TestPG* test_pg = get_test_pg();
    test_pg->outstanding_writes[hoid]++;

    for (const auto& [op, bl] : steps) {
      switch (op.type) {
      case ObjOp::TRUNCATE:
        pg_t->truncate(hoid, op.off);
        break;
      case ObjOp::WRITE: {
        bufferlist data = bl;
        pg_t->write(hoid, op.off, data.length(), data);
        break;
      }
      case ObjOp::ZERO:
        pg_t->zero(hoid, op.off, op.len);
        break;
      }
    }

    object_stat_sum_t delta_stats;
    delta_stats.num_bytes = int64_t(new_size) - int64_t(pre_op_size);

    eversion_t prior_version = obc->obs.oi.version;
    eversion_t at_version = get_next_version();

    object_info_t new_oi = obc->obs.oi;
    new_oi.version = at_version;
    new_oi.prior_version = prior_version;
    new_oi.size = new_size;
    {
      bufferlist oi_bl;
      new_oi.encode(oi_bl, osdmap->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      pg_t->setattr(hoid, OI_ATTR, oi_bl);
    }
    obc->obs.oi = new_oi;

    std::vector<pg_log_entry_t> log_entries;
    pg_log_entry_t entry;
    entry.op = pg_log_entry_t::MODIFY;
    entry.soid = hoid;
    entry.version = at_version;
    entry.prior_version = prior_version;
    log_entries.push_back(entry);

    auto write_complete = make_write_completion(
      test_pg, hoid, result,
      [obc, prior_version, pre_op_size]() {
        obc->obs.oi.version = prior_version;
        obc->obs.oi.size = pre_op_size;
        obc->attr_cache.clear();
      },
      [this, obj_name, steps, at_version](int) {
        for (const auto& [op, bl] : steps) {
          switch (op.type) {
          case ObjOp::TRUNCATE:
            record_truncate_and_write(obj_name, op.off, {}, at_version);
            break;
          case ObjOp::WRITE:
            record_truncate_and_write(obj_name, std::nullopt,
                                      {{op.off, bl.to_str()}}, at_version);
            break;
          case ObjOp::ZERO:
            record_truncate_and_write(obj_name, std::nullopt,
                                      {{op.off, std::string(op.len, '\0')}},
                                      at_version);
            break;
          }
        }
      });

    do_transaction(
      hoid, std::move(pg_t), delta_stats, at_version, std::move(log_entries),
      write_complete);
  }

  /* Check every shard of an object, other than those in skip, against model,
   * the expected contents of the object. Each shard must be as long as the
   * shard size for the object size. Each data shard must hold its chunks of
   * the object, and zeros past the end of the object. Each parity shard must
   * hold the parity that ec_impl encodes from those data chunks.
   */
  void check_shards(const std::string& obj_name,
                    const std::string& model,
                    const std::set<int>& skip) {
    SCOPED_TRACE("check_shards " + obj_name);
    const ECUtil::stripe_info_t sinfo = get_sinfo();
    const uint64_t size = model.size();
    const uint64_t sw = sinfo.get_stripe_width();
    auto shards = read_shards(obj_name);
    std::set<int> bad_shards;

    for (int shard = 0; shard < k + m; ++shard) {
      if (!skip.contains(shard)) {
        EXPECT_EQ(sinfo.object_size_to_shard_size(size, shard_id_t(shard)),
                  shards.at(shard).length())
          << "length of shard " << shard << " for object size " << size;
      }
    }

    for (uint64_t stripe = 0; stripe * sw < size; ++stripe) {
      shard_id_map<bufferptr> in(k + m);
      shard_id_map<bufferptr> out(k + m);
      for (raw_shard_id_t raw; raw < k + m; ++raw) {
        bufferptr bp = buffer::create_aligned(stripe_unit, EC_ALIGN_SIZE);
        bp.zero();
        if (raw < k) {
          const uint64_t ro_offset = stripe * sw + int(raw) * stripe_unit;
          if (ro_offset < size) {
            bp.copy_in(0, std::min(stripe_unit, size - ro_offset),
                       model.data() + ro_offset);
          }
          in[sinfo.get_shard(raw)] = bp;
        } else {
          out[sinfo.get_shard(raw)] = bp;
        }
      }
      ASSERT_EQ(0, ec_impl->encode_chunks(in, out));

      for (int shard = 0; shard < k + m; ++shard) {
        const shard_id_t id(shard);
        if (skip.contains(shard) || bad_shards.contains(shard)) {
          continue;
        }
        const bufferptr& expected = in.contains(id) ? in.at(id) : out.at(id);
        bufferlist& stored = shards.at(shard);
        const uint64_t offset = stripe * stripe_unit;
        if (offset >= stored.length()) {
          continue;
        }
        const uint64_t length = std::min(stripe_unit, stored.length() - offset);
        const char* actual = stored.c_str() + offset;
        for (uint64_t i = 0; i < length; ++i) {
          if (actual[i] != expected.c_str()[i]) {
            ADD_FAILURE() << (in.contains(id) ? "data" : "parity")
                          << " shard " << shard << " differs from the model"
                          << " at shard offset " << offset + i
                          << " (stripe " << stripe << ", object size "
                          << size << ")";
            bad_shards.insert(shard);
            break;
          }
        }
      }
    }
  }

  std::string object_name(const std::string& name, size_t index) {
    return name + "_" + std::to_string(index);
  }

  /* Create each object and apply its committed ops, returning the models. */
  std::vector<std::string> create_objects(
      const std::string& name,
      const std::vector<ObjectScenario>& objects) {
    std::vector<std::string> models;
    for (size_t i = 0; i < objects.size(); ++i) {
      bufferlist bl = create_random_buffer(objects[i].size);
      models.emplace_back(bl.c_str(), bl.length());
      create_and_write_verify(object_name(name, i), models.back());
      for (const auto& ops : objects[i].committed) {
        EXPECT_EQ(0, submit_ops(object_name(name, i), ops, models.back()));
      }
    }
    return models;
  }

  struct QueuedOp {
    size_t object;
    std::shared_ptr<int> result;
    std::string model;  // The object once this op is applied.
  };

  /* Queue every object's ops, with the i'th op of each object queued after
   * the (i-1)'th op of every object.
   */
  std::vector<QueuedOp> queue_all_ops(
      const std::string& name,
      const std::vector<ObjectScenario>& objects,
      std::vector<std::string>& models) {
    std::vector<QueuedOp> queued;
    for (size_t step = 0; ; ++step) {
      bool any = false;
      for (size_t i = 0; i < objects.size(); ++i) {
        if (step < objects[i].ops.size()) {
          auto result =
            queue_ops(object_name(name, i), objects[i].ops[step], models[i]);
          queued.push_back({i, result, models[i]});
          any = true;
        }
      }
      if (!any) {
        return queued;
      }
    }
  }

  void verify_objects(const std::string& name,
                      const std::vector<std::string>& models,
                      const std::set<int>& down) {
    for (size_t i = 0; i < models.size(); ++i) {
      if (!models[i].empty()) {
        verify_object(object_name(name, i), models[i], 0, models[i].size());
      }
      check_shards(object_name(name, i), models[i], down);
    }
  }

  /* Apply the ops of each object, all in flight together, and check the
   * objects and their shards. Then take down the OSD holding the first data
   * shard, so that reads must decode, and check again.
   */
  void run_forward(const std::string& name,
                   const std::vector<ObjectScenario>& objects) {
    ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
    auto models = create_objects(name, objects);
    const auto queued = queue_all_ops(name, objects, models);
    event_loop->run_until_idle();
    for (const auto& op : queued) {
      EXPECT_EQ(0, *op.result);
    }
    {
      SCOPED_TRACE("after the ops");
      verify_objects(name, models, {});
    }

    const int down = get_shard(0);
    mark_osd_down(down);
    event_loop->run_until_idle();
    ASSERT_TRUE(all_shards_active()) << "All shards should be active after peering";
    SCOPED_TRACE("with shard " + std::to_string(down) + " down");
    verify_objects(name, models, {down});
  }

  /* Submit the ops of each object so that they reach every shard except
   * blocked_shard, then take down the OSD of failing_shard so that peering
   * rolls back those still in flight. An op that does not write to
   * blocked_shard can complete first, and must then survive. Each object
   * must be left as it was after its last completed op, and each surviving
   * shard of an object with no completed op must be restored exactly.
   */
  void run_rollback(const std::string& name,
                    const std::vector<ObjectScenario>& objects,
                    int blocked_shard,
                    int failing_shard) {
    ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
    ASSERT_NE(blocked_shard, failing_shard);
    ASSERT_GE(k + m - 1, k) << "Too few shards would remain up";

    const auto originals = create_objects(name, objects);
    std::vector<std::map<int, bufferlist>> original_shards;
    for (size_t i = 0; i < objects.size(); ++i) {
      original_shards.push_back(read_shards(object_name(name, i)));
    }
    {
      SCOPED_TRACE("before the ops");
      verify_objects(name, originals, {});
    }

    const int primary = get_primary_shard_from_osdmap();
    event_loop->suspend_from_to_osd(primary, blocked_shard);
    auto models = originals;
    const auto queued = queue_all_ops(name, objects, models);
    event_loop->run_until_idle();

    auto expected = originals;
    std::vector<bool> in_flight(objects.size(), false);
    std::vector<bool> completed(objects.size(), false);
    for (const auto& op : queued) {
      if (*op.result == 0) {
        EXPECT_FALSE(in_flight[op.object]) << "ops must complete in order";
        expected[op.object] = op.model;
        completed[op.object] = true;
      } else {
        EXPECT_EQ(-EINPROGRESS, *op.result);
        in_flight[op.object] = true;
      }
    }
    if (!std::ranges::any_of(in_flight, std::identity{})) {
      std::cout << "No op writes to shard " << blocked_shard
                << ", so none is left to roll back" << std::endl;
    }

    mark_osd_down(failing_shard);
    event_loop->unsuspend_from_to_osd(primary, blocked_shard);
    event_loop->run_until_idle();
    ASSERT_TRUE(all_shards_active()) << "All shards should be active after peering";

    SCOPED_TRACE("after rollback with shard " + std::to_string(blocked_shard) +
                 " blocked and shard " + std::to_string(failing_shard) +
                 " failed");
    verify_objects(name, expected, {failing_shard});
    for (size_t i = 0; i < objects.size(); ++i) {
      if (completed[i]) {
        continue;
      }
      const auto shards = read_shards(object_name(name, i));
      for (int shard = 0; shard < k + m; ++shard) {
        if (shard == failing_shard) {
          continue;
        }
        EXPECT_EQ(original_shards[i].at(shard).length(), shards.at(shard).length())
          << "size of shard " << shard << " of " << object_name(name, i);
        EXPECT_TRUE(original_shards[i].at(shard).contents_equal(shards.at(shard)))
          << "contents of shard " << shard << " of " << object_name(name, i);
      }
    }
  }
};

/**
 * TestECFailoverWithPeering - parameterized EC peering and failover tests.
 *
 * This fixture is parameterized over BackendConfig to test multiple EC
 * configurations (different k/m values, stripe units, plugins, and optimizations).
 * Only EC configurations are tested since peering and failover are EC-specific.
 */
class TestECFailoverWithPeering : public ECTruncateTestBase,
                                   public ::testing::WithParamInterface<BackendConfig> {
public:
  TestECFailoverWithPeering() : ECTruncateTestBase(GetParam()) {}

protected:
  /* Truncate an object and apply writes of the given {offset, length} in a
   * single op that reaches every shard except shard 1, then fail the last
   * parity shard so that the op is rolled back.
   */
  void rollback_truncate_and_write(
      const std::string& obj_name,
      uint64_t object_size,
      uint64_t truncate_to,
      const std::vector<std::pair<uint64_t, uint64_t>>& writes) {
    std::vector<ObjOp> ops{Truncate(truncate_to)};
    for (auto [offset, length] : writes) {
      ops.push_back(Write(offset, length));
    }
    run_rollback(obj_name, {{object_size, {}, {ops}}}, 1, get_shard(k + m - 1));
  }
};

TEST_P(TestECFailoverWithPeering, BasicPeeringCycle) {
  pg_t pgid = get_primary_test_pg()->get_peering_state()->get_info().pgid.pgid;
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(pgid, &acting_osds, &acting_primary);
  
  ASSERT_TRUE(get_primary_test_pg()->get_peering_state()->is_clean())
    << "Primary should be clean after peering";
  
  // Verify primary is shard 0
  ASSERT_EQ(get_primary_shard_from_osdmap(), 0) << "Shard 0 should be primary";
  
  for (int i = 1; i < k + m; i++) {
    ASSERT_FALSE(get_test_pg_by_shard(i)->get_peering_listener()->backend_listener->pgb_is_primary())
      << "Shard " << i << " should not be primary";
  }
}

TEST_P(TestECFailoverWithPeering, WriteWithPeering) {

  const std::string obj_name = "test_write_with_peering";
  const std::string test_data = "Data written with full peering support";
  
  create_and_write_verify(obj_name, test_data);

  auto* primary_ps = get_primary_test_pg()->get_peering_state();
  ASSERT_GT(primary_ps->get_pg_log().get_log().log.size(), 0)
    << "Primary should have log entries after write";
}

TEST_P(TestECFailoverWithPeering, OSDFailureWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_osd_failure";
  uint64_t object_size = k * stripe_unit;
  const std::string test_data_full(object_size, 'X');
  const size_t read_length = 2 * stripe_unit;
  const std::string test_data_read(read_length, 'X');
  int failed_osd = 1;  // Fail shard 1 which contains part of the data

  create_and_write_verify(obj_name, test_data_full);
  // Measure the number of reads that occur.
  event_loop->reset_stats();
  bufferlist pre_failover_read;
  read_object(obj_name, 0, read_length, pre_failover_read, object_size);
  ASSERT_EQ(4, event_loop->get_stats_by_type().at(EventLoop::EventType::OSD_MESSAGE));

  // Use fixture helper to mark OSD as down
  mark_osd_down(failed_osd);
  
  // Reset EventLoop stats before post-failover read
  event_loop->reset_stats();
  verify_object(obj_name);
  ASSERT_EQ(k * 2, event_loop->get_stats_by_type().at(EventLoop::EventType::OSD_MESSAGE));
}

TEST_P(TestECFailoverWithPeering, PrimaryFailoverWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_primary_failover";
  const std::string test_data = "Data before primary failover";
  
  create_and_write_verify(obj_name, test_data);
  
  // Mark OSD 0 (the initial primary) as down
  // PeeringState will automatically determine the new primary
  mark_osd_down(0);
  
  // Determine the actual new primary from the OSDMap
  int new_primary_shard = get_primary_shard_from_osdmap();
  ASSERT_GE(new_primary_shard, 0) << "Should have a valid new primary after failover";
  
  // For an optimized EC pool (k=4, m=2), the new primary should be a coding shard (>= k)
  // For a non-optimized pool, it would be shard 1
  const pg_pool_t& pool = get_pool();
  if (pool.allows_ecoptimizations()) {
    ASSERT_GE(new_primary_shard, k)
      << "New primary should be a coding shard (>= k) for optimized pool";
  } else {
    ASSERT_EQ(new_primary_shard, 1)
      << "New primary should be shard 1 for non-optimized pool";
  }
  
  TestPG* new_primary_pg = get_primary_test_pg();
  ASSERT_TRUE(new_primary_pg != nullptr) << "New primary TestPG should exist";
  ASSERT_TRUE(new_primary_pg->has_backend()) << "New primary should have backend";
  EXPECT_TRUE(new_primary_pg->get_backend_listener()->pgb_is_primary())
    << "Shard " << new_primary_shard << " should be new primary";
  
  TestPG* failed_pg = get_first_test_pg_for_osd(0);
  ASSERT_TRUE(failed_pg != nullptr) << "Failed OSD TestPG should still exist";
  ASSERT_TRUE(failed_pg->has_backend()) << "Failed OSD should still have backend";
  EXPECT_FALSE(failed_pg->get_backend_listener()->pgb_is_primary())
    << "Failed shard should not be primary";
  
  std::string state = get_state_name(new_primary_shard);
  ASSERT_TRUE(state.find("Active") != std::string::npos)
    << "New primary should be Active after failover, got: " << state;
  
  // Verify the PG reached Active state
  ASSERT_TRUE(get_primary_test_pg()->get_peering_state()->is_active())
    << "New primary should be in Active state";
  
  // Verify reads work after primary failover (with EC reconstruction)
  verify_object(obj_name);
}

TEST_P(TestECFailoverWithPeering, MultipleOSDFailuresWithPeering) {
  // This test only runs for configurations with m=2 and num_zones=1
  if (m != 2 || num_zones != 1) {
    GTEST_SKIP() << "MultipleOSDFailuresWithPeering only runs for m=2, num_zones=1";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj_name = "test_multiple_failures";
  const std::string test_data = "Data before multiple failures";
  
  create_and_write_verify(obj_name, test_data);
  
  std::vector<int> failed_osds = {1, 2};  // Fail 2 data shards
  ASSERT_EQ(failed_osds.size(), static_cast<size_t>(m))
    << "Should fail exactly m OSDs";
  
  // Use fixture helper to mark multiple OSDs as down
  mark_osds_down(failed_osds);
  
  auto* primary_ps = get_primary_test_pg()->get_peering_state();
  for (int failed_osd : failed_osds) {
    ASSERT_TRUE(primary_ps->get_acting_recovery_backfill().count(
      pg_shard_t(failed_osd, shard_id_t(failed_osd))) == 0)
      << "Failed OSD " << failed_osd << " should not be in acting set";
  }
  
  std::string primary_state = get_state_name(0);
  ASSERT_TRUE(primary_state.find("Peering") != std::string::npos ||
              primary_state.find("Active") != std::string::npos ||
              primary_state.find("Recovery") != std::string::npos)
    << "Primary should be operational, got: " << primary_state;
}

TEST_P(TestECFailoverWithPeering, RecoveryWithPeering) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";
  
  const std::string obj1_name = "test_recovery_obj1";
  const std::string obj1_data = "First object data for recovery test";
  
  const std::string obj2_name = "test_recovery_obj2";
  const std::string obj2_data = "Second object data for recovery test";
  
  int result = create_and_write(obj1_name, obj1_data);
  ASSERT_EQ(result, 0) << "First pre-failure write should complete";
  
  result = create_and_write(obj2_name, obj2_data);
  ASSERT_EQ(result, 0) << "Second pre-failure write should complete";
  
  ASSERT_TRUE(primary_is_clean()) << "Primary should be clean before recovery test";
  
  auto* primary_ps = get_primary_test_pg()->get_peering_state();
  eversion_t pre_failure_log_head = primary_ps->get_pg_log().get_log().head;
  ASSERT_GT(pre_failure_log_head.version, 0u)
    << "Primary should have log entries before failure";
  
  int failed_osd = k - 1;  // Last data shard
  
  // Use fixture helper to mark OSD as down
  mark_osd_down(failed_osd);
  
  std::string state_after_failure = get_state_name(0);
  ASSERT_TRUE(all_shards_active() ||
              state_after_failure.find("Recovery") != std::string::npos ||
              state_after_failure.find("Peering") != std::string::npos)
    << "PG should be active, recovering, or peering after OSD failure, got: "
    << state_after_failure;
  
  // EC can reconstruct data from remaining k shards even with one shard missing
  bufferlist obj1_read;
  int read_result = read_object(obj1_name, 0, obj1_data.length(),
                                obj1_read, obj1_data.length());
  ASSERT_GE(read_result, 0) << "First object should be readable after OSD failure";
  ASSERT_EQ(obj1_read.length(), obj1_data.length())
    << "First object read length should match after failure";
  {
    std::string read_str(obj1_read.c_str(), obj1_read.length());
    ASSERT_EQ(read_str, obj1_data)
      << "First object data should be correct after OSD failure (EC reconstruction)";
  }
  
  bufferlist obj2_read;
  read_result = read_object(obj2_name, 0, obj2_data.length(),
                            obj2_read, obj2_data.length());
  ASSERT_GE(read_result, 0) << "Second object should be readable after OSD failure";
  ASSERT_EQ(obj2_read.length(), obj2_data.length())
    << "Second object read length should match after failure";
  {
    std::string read_str(obj2_read.c_str(), obj2_read.length());
    ASSERT_EQ(read_str, obj2_data)
      << "Second object data should be correct after OSD failure (EC reconstruction)";
  }
  
  const std::string post_recovery_obj = "test_post_recovery";
  const std::string post_recovery_data = "Data written after OSD failure and recovery";
  
  result = create_and_write(post_recovery_obj, post_recovery_data);
  ASSERT_EQ(result, 0) << "Write after OSD failure should complete successfully";
  
  bufferlist post_recovery_read;
  read_result = read_object(post_recovery_obj, 0, post_recovery_data.length(),
                            post_recovery_read, post_recovery_data.length());
  ASSERT_GE(read_result, 0) << "Post-recovery object should be readable";
  ASSERT_EQ(post_recovery_read.length(), post_recovery_data.length())
    << "Post-recovery read length should match";
  {
    std::string read_str(post_recovery_read.c_str(), post_recovery_read.length());
    ASSERT_EQ(read_str, post_recovery_data)
      << "Post-recovery data should match what was written";
  }
  
  eversion_t post_recovery_log_head = primary_ps->get_pg_log().get_log().head;
  ASSERT_GT(post_recovery_log_head.version, pre_failure_log_head.version)
    << "Primary PG log head should advance after post-recovery write";
  
  // Even though the OSD is "down", its PeeringState still holds the log
  // from before it went down.
  auto* failed_ps = get_first_test_pg_for_osd(failed_osd)->get_peering_state();
  ASSERT_TRUE(failed_ps != nullptr) << "Failed OSD's PeeringState should still exist";
  
  size_t primary_log_size = primary_ps->get_pg_log().get_log().log.size();
  size_t failed_log_size = failed_ps->get_pg_log().get_log().log.size();
  ASSERT_LE(failed_log_size, primary_log_size)
    << "Failed OSD's PG log size should not exceed primary's log size";
  // The primary wrote 3 objects (obj1, obj2, post_recovery_obj), so its log must be non-empty.
  ASSERT_GT(primary_log_size, 0u)
    << "Primary PG log should have entries after 3 writes";
  
  auto* listener_ptr = get_primary_test_pg()->get_peering_listener();
  ASSERT_TRUE(listener_ptr != nullptr) << "Peering listener should exist";
  ASSERT_TRUE(listener_ptr->activate_complete_called)
    << "on_activate_complete should have been called during peering";
}
TEST_P(TestECFailoverWithPeering, ZeroSizeObjectWithAttributesRecovery) {
  //  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_primary_failover";
  const std::string test_data;

  create_and_write(obj_name, test_data);

  // Mark OSD 0 (the initial primary) as down
  // PeeringState will automatically determine the new primary
  mark_osd_down(0);

  write_attribute(obj_name, "key", "value", false);

  // Determine the actual new primary from the OSDMap
  int new_primary_shard = get_primary_shard_from_osdmap();
  ASSERT_GE(new_primary_shard, 0) << "Should have a valid new primary after failover";

  // For an optimized EC pool (k=4, m=2), the new primary should be a coding shard (>= k)
  // For a non-optimized pool, it would be shard 1
  const pg_pool_t& pool = get_pool();
  if (pool.allows_ecoptimizations()) {
    ASSERT_GE(new_primary_shard, k)
      << "New primary should be a coding shard (>= k) for optimized pool";
  } else {
    ASSERT_EQ(new_primary_shard, 1)
      << "New primary should be shard 1 for non-optimized pool";
  }

  ASSERT_TRUE(get_primary_test_pg()->get_peering_listener()->backend_listener->pgb_is_primary())
    << "Shard " << new_primary_shard << " should be new primary";

  ASSERT_FALSE(get_first_test_pg_for_osd(0)->get_peering_listener()->backend_listener->pgb_is_primary())
    << "Failed shard should not be primary";

  std::string state = get_state_name(new_primary_shard);
  ASSERT_TRUE(state.find("Active") != std::string::npos)
    << "New primary should be Active after failover, got: " << state;

  // Verify the PG reached Active state
  ASSERT_TRUE(get_primary_test_pg()->get_peering_state()->is_active())
    << "New primary should be in Active state";

  mark_osd_up(0);

  run_recovery(obj_name, true, test_data);
  // Verify that the attribute was recovered on shard 0
  hobject_t hoid = make_test_object(obj_name);
  ghobject_t ghoid = ghobject_t(hoid, ghobject_t::NO_GEN, shard_id_t(0));

  OsdTestFixture* osd_fixture = get_osd_fixture(0);
  ceph_assert(osd_fixture != nullptr && osd_fixture->store);
  TestPG* test_pg = get_test_pg(0, 0);
  ceph_assert(test_pg != nullptr);

  ceph::buffer::ptr attr_value;
  int r = osd_fixture->store->getattr(test_pg->ch, ghoid, "key", attr_value);
  ASSERT_GE(r, 0) << "Attribute 'key' should exist on recovered shard 0";
  ASSERT_EQ(std::string(attr_value.c_str(), attr_value.length()), "value")
    << "Attribute 'key' should have value 'value' after recovery";
}

// ---------------------------------------------------------------------------
// EC backend configurations for parameterized tests
// ---------------------------------------------------------------------------

namespace {

/**
 * EC-only backend configurations for TestECFailoverWithPeering.
 * These configurations test various EC parameters:
 * - Different k/m ratios (2+1, 4+2, 8+3)
 * - Different stripe units (4k, 8k, 16k)
 * - Different plugins (isa, jerasure)
 * - Optimized vs non-optimized EC
 * - Multi-zone configurations
 */
const std::vector<BackendConfig> kECPeeringConfigs = {
  // ISA plugin with optimizations (modern EC)
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_ISA_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_ISA_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_ISA_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_ISA_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "isa", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_ISA_Opt_k8m3_su4k"},

  // Jerasure plugin with optimizations (modern EC)
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  8192,  4, 2, 1, "EC_Jerasure_Opt_k4m2_su8k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  16384, 4, 2, 1, "EC_Jerasure_Opt_k4m2_su16k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  2, 1, 1, "EC_Jerasure_Opt_k2m1_su4k"},
  {PGBackendTestFixture::EC, "jerasure", "reed_sol_van", pg_pool_t::FLAG_EC_OVERWRITES | pg_pool_t::FLAG_EC_OPTIMIZATIONS,  4096,  8, 3, 1, "EC_Jerasure_Opt_k8m3_su4k"},
};

}  // namespace

/**
 * Test OSD failure and recovery with peering.
 *
 * This test simulates the following scenario:
 * 1. Write full stripe with pattern A (committed to all shards)
 * 2. Write full stripe with pattern B (committed to all shards)
 * 3. Mark OSD 5 as down (forcing peering)
 * 4. Trigger peering - PG should remain active/recovering
 * 5. Read data back - should get pattern B (latest write)
 *
 * This verifies that the test infrastructure properly handles OSD failures
 * and peering without leaving OSDs in a suspended state that would block
 * teardown.
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterOSDFailure
) {
  // GTEST_SKIP(); // Temporary
  int failing_shard = k + m - 1;
  int blocked_shard = 1;
  const std::string obj_name = "test";
  const size_t data_size = stripe_unit * k;  // One full stripe.
  std::string pattern_a(data_size, 'A');
  std::string pattern_b(data_size, 'B');
  std::string pattern_c(data_size, 'C');

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  create_and_write_verify(obj_name, pattern_a);
  suspend_primary_to_osd(blocked_shard);
  int result = write(obj_name, 0, pattern_b, data_size);
  ASSERT_EQ(-EINPROGRESS, result);
  result = write(obj_name, 0, pattern_c, data_size);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(failing_shard);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  // Ensure all shards have completed peering and applied rollback transactions
  ASSERT_TRUE(all_shards_active()) << "All shards should be active after peering";

  verify_object(obj_name);

  std::cout << "\n=== RollbackAfterOSDFailure Test Complete ===" << std::endl;
}
/**
 * ECRecoveryTest - Test EC recovery scenario with missing objects
 *
 * This test verifies the EC recovery mechanism by:
 * 1. Writing and verifying an object
 * 2. Removing an OSD from the acting set (simulating OSD failure)
 * 3. Performing an overwrite to the object (creating a version mismatch)
 * 4. Adding the OSD back to the acting set
 * 5. Inspecting the missing list to verify the object is marked as missing
 * 6. Demonstrating that the primary can open a recovery operation
 *
 * The test runs multiple times, once for each OSD to fail:
 * - OSD 1 (always)
 */
TEST_P(TestECFailoverWithPeering, ECRecoveryTest) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Determine which OSDs to test based on num_zones
  std::vector<int> osds_to_test;
  osds_to_test.push_back(1); // Non-primary
  osds_to_test.push_back(0); // Primary
  osds_to_test.push_back(k); // First coding shard

  if (num_zones > 1) {
    osds_to_test.push_back(k + m + 1);  // k + m + 1
    osds_to_test.push_back(2 * k + m);  // 2k + m
  }

  // Run the test for each OSD
  for (int removed_osd : osds_to_test) {
    const std::string obj_name = "test_ec_recovery_osd" + std::to_string(removed_osd);
    const size_t data_size = stripe_unit * k;  // One full stripe.
    std::string pattern_a(data_size, 'A');
    std::string pattern_b(data_size, 'B');

    create_and_write_verify(obj_name, pattern_a);
    mark_osd_down(removed_osd);
    write_verify(obj_name, 0, pattern_b, data_size);
    mark_osd_up(removed_osd);

    // Use the fixture helper to run recovery and verify callbacks
    run_recovery(obj_name, removed_osd == 0, pattern_b);

    std::cout << "=== Recovery test with OSD " << removed_osd << " completed successfully ===" << std::endl;
  }
}

/**
 * ECSequentialOSDFailoverTest - Test sequential OSD failure and recovery
 *
 * This test verifies the EC recovery mechanism by sequentially failing and
 * recovering each OSD in the cluster:
 * 1. Create an object and write initial data
 * 2. For each OSD (0 to (k+m)*num_zones - 1):
 *    a. Fail the OSD
 *    b. Write new data to the object (overwrite)
 *    c. Recover the OSD
 *    d. Verify recovery completes
 * 3. Verify final data is correct
 *
 * Unlike ECRecoveryTest which creates a new object for each OSD failure,
 * this test performs a new write to the same object on each cycle.
 */
TEST_P(TestECFailoverWithPeering, ECSequentialOSDFailoverTest) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_sequential_failover";
  const size_t data_size = stripe_unit * k;  // One full stripe

  // Calculate total number of OSDs to test
  int total_osds = (k + m) * num_zones;

  std::cout << "\n=== Testing sequential OSD failover for " << total_osds
            << " OSDs (k=" << k << ", m=" << m << ", zones=" << num_zones << ") ===" << std::endl;

  // Create object with initial pattern
  std::string initial_pattern(data_size, 'A');
  create_and_write_verify(obj_name, initial_pattern);

  // Cycle through each OSD, failing and recovering it
  for (int osd_to_fail = 0; osd_to_fail < total_osds; osd_to_fail++) {
    char pattern_char = 'B' + (osd_to_fail % 25);  // Cycle through B-Z, then wrap
    std::string cycle_pattern(data_size, pattern_char);
    mark_osd_down(osd_to_fail);
    write_verify(obj_name, 0, cycle_pattern, data_size);
    mark_osd_up(osd_to_fail);
    run_recovery(obj_name, osd_to_fail == 0, cycle_pattern);
  }

  std::cout << "\n=== Sequential OSD failover test completed successfully ===" << std::endl;
}

/**
 * ECZoneRecoveryTest - Test zone-level EC recovery scenario (zone 0 fails first)
 *
 * This test reproduces a bug whereby a full write, following a partial write
 * will rollback to an OI with an incorrect previous version.
 *
 * Recreate https://tracker.ceph.com/issues/76213
 */
TEST_P(TestECFailoverWithPeering, RollbackVersionMismatch) {
  if (k < 3) {
    GTEST_SKIP() << "SnapshotTrimRollbackVersionMismatch requires at least 3 data shards";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_attr_rollback";
  int temp_failing_shard = 2;     // Temporarily fail shard 2 for peering interval change

  create_and_write_verify(obj_name, "initial_data");
  eversion_t v1 = read_shard_object_info(obj_name, 0).version;
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, k).version);

  int result = write_attribute(obj_name, "test_attr", "value1", false);
  ASSERT_EQ(0, result);
  event_loop->run_until_idle();

  eversion_t v2 = read_shard_object_info(obj_name, 0).version;
  ASSERT_GT(v2, v1);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v2, read_shard_object_info(obj_name, k).version);

  suspend_primary_to_osd(k);
  result = write_attribute(obj_name, "test_attr", "value2", true);
  ASSERT_NE(0, result);
  mark_osd_down(temp_failing_shard);
  unsuspend_primary_to_osd(k);
  event_loop->run_until_idle();
  ASSERT_EQ(v2, read_shard_object_info(obj_name, 0).version);
  ASSERT_EQ(v1, read_shard_object_info(obj_name, 1).version);
  ASSERT_EQ(v2, read_shard_object_info(obj_name, k).version);

}

/**
 * TEST: MultiObjectRecoveryReadCrash
 *
 * This test reproduces Bug 75432: Assertion failure in ECCommon::ReadPipeline::do_read_op()
 * when handling multi-object EC reads with partial failures.
 *
 * The bug occurs when:
 * 1. Multiple objects of different sizes are read simultaneously
 * 2. Smaller objects complete successfully (shard_reads cleared)
 * 3. A larger object needs additional reads due to a shard failure (need_resend = true)
 * 4. do_read_op() is called with both completed and incomplete objects
 */
TEST_P(TestECFailoverWithPeering, MultiObjectRecoveryReadCrash) {
  // This test requires k >= 3 and m >= 2
  if (k < 3 || m < 2) {
    GTEST_SKIP() << "Test requires k >= 3 and m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Create objects of different sizes with initial pattern
  const std::string obj1_name = "crash_test_obj1";
  const std::string obj1_pattern_a(stripe_unit, 'A');  // 1 chunk

  const std::string obj2_name = "crash_test_obj2";
  const std::string obj2_pattern_a(2 * stripe_unit, 'A');  // 2 chunks

  const std::string obj3_name = "crash_test_obj3";
  const std::string obj3_pattern_a(3 * stripe_unit, 'A');  // 3 chunks

  // Write initial pattern to all objects
  int result = create_and_write(obj1_name, obj1_pattern_a);
  EXPECT_EQ(result, 0) << "First object write should complete";

  result = create_and_write(obj2_name, obj2_pattern_a);
  EXPECT_EQ(result, 0) << "Second object write should complete";

  result = create_and_write(obj3_name, obj3_pattern_a);
  EXPECT_EQ(result, 0) << "Third object write should complete";

  EXPECT_TRUE(primary_is_clean()) << "Primary should be clean";

  // Mark shard 1 as down - this will require recovery
  int failed_osd = 1;
  mark_osd_down(failed_osd);

  // Write new pattern to all objects while OSD 1 is down
  // This creates objects that need recovery on OSD 1
  const std::string obj1_pattern_b(stripe_unit, 'B');
  const std::string obj2_pattern_b(2 * stripe_unit, 'B');
  const std::string obj3_pattern_b(3 * stripe_unit, 'B');

  result = write(obj1_name, 0, obj1_pattern_b, obj1_pattern_b.length());
  EXPECT_EQ(result, 0) << "First object update should complete";

  result = write(obj2_name, 0, obj2_pattern_b, obj2_pattern_b.length());
  EXPECT_EQ(result, 0) << "Second object update should complete";

  result = write(obj3_name, 0, obj3_pattern_b, obj3_pattern_b.length());
  EXPECT_EQ(result, 0) << "Third object update should complete";

  // Bring OSD back up to trigger peering
  // Peering will detect that OSD 1 has stale data and populate peer_missing
  mark_osd_up(failed_osd);

  // Inject read error on shard 2 for object 3 only
  // This will cause object 3's recovery to fail and need resend
  inject_read_error_for_shard(obj3_name, 2, -EIO);

  // Now trigger recovery for all 3 objects simultaneously
  // This is the key: recovery reads multiple objects in a single operation
  // obj1: 1 chunk - reads shard 0 only -> succeeds -> shard_reads cleared
  // obj2: 2 chunks - reads shards 0, k -> succeeds -> shard_reads cleared
  // obj3: 3 chunks - reads shards 0, 2, k -> shard 2 fails -> needs resend
  // BUG: do_read_op() called with obj1/obj2 having empty shard_reads

  std::cout << "Starting recovery for all 3 objects..." << std::endl;

  run_recovery(obj1_name, false, obj1_pattern_b);
  run_recovery(obj2_name, false, obj2_pattern_b);
  run_recovery(obj3_name, false, obj3_pattern_b);

  // If the bug is present, we'll crash before getting here
  // If the bug is fixed, recovery should complete successfully
  std::cout << "Recovery completed for all objects" << std::endl;

  SUCCEED() << "Multi-object recovery completed without crash";
}

/**
 * TEST: MultiObjectParallelRecoveryCrash
 *
 * This test reproduces Bug 75432 by recovering multiple objects in parallel
 * within a single recovery operation (not sequentially).
 *
 * The bug occurs when:
 * 1. Multiple objects are recovered in a single operation (parallel recovery)
 * 2. Smaller objects complete successfully (shard_reads cleared)
 * 3. A larger object needs additional reads due to a shard failure (need_resend = true)
 * 4. do_read_op() is called with both completed and incomplete objects
 *
 * Recreate for tracker https://tracker.ceph.com/issues/75432
 *
 * Expected behavior WITH fix: Test completes successfully.
 */
TEST_P(TestECFailoverWithPeering, MultiObjectParallelRecoveryCrash) {
  // This test requires k >= 3 and m >= 2
  if (k < 3 || m < 2) {
    GTEST_SKIP() << "Test requires k >= 3 and m >= 2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Create objects of different sizes with initial pattern
  const std::string obj1_name = "crash_test_obj1";
  const std::string obj1_pattern_a(stripe_unit, 'A');  // 1 chunk

  const std::string obj2_name = "crash_test_obj2";
  const std::string obj2_pattern_a(2 * stripe_unit, 'A');  // 2 chunks

  const std::string obj3_name = "crash_test_obj3";
  const std::string obj3_pattern_a(3 * stripe_unit, 'A');  // 3 chunks

  // Write initial pattern to all objects
  int result = create_and_write(obj1_name, obj1_pattern_a);
  EXPECT_EQ(result, 0) << "First object write should complete";

  result = create_and_write(obj2_name, obj2_pattern_a);
  EXPECT_EQ(result, 0) << "Second object write should complete";

  result = create_and_write(obj3_name, obj3_pattern_a);
  EXPECT_EQ(result, 0) << "Third object write should complete";

  EXPECT_TRUE(primary_is_clean()) << "Primary should be clean";

  // Mark shard 1 as down - this will require recovery
  int failed_osd = 1;
  mark_osd_down(failed_osd);

  // Write new pattern to all objects while OSD 1 is down
  // This creates objects that need recovery on OSD 1
  const std::string obj1_pattern_b(stripe_unit, 'B');
  const std::string obj2_pattern_b(2 * stripe_unit, 'B');
  const std::string obj3_pattern_b(3 * stripe_unit, 'B');

  result = write(obj1_name, 0, obj1_pattern_b, obj1_pattern_b.length());
  EXPECT_EQ(result, 0) << "First object update should complete";

  result = write(obj2_name, 0, obj2_pattern_b, obj2_pattern_b.length());
  EXPECT_EQ(result, 0) << "Second object update should complete";

  result = write(obj3_name, 0, obj3_pattern_b, obj3_pattern_b.length());
  EXPECT_EQ(result, 0) << "Third object update should complete";

  // Bring OSD back up to trigger peering
  // Peering will detect that OSD 1 has stale data and populate peer_missing
  mark_osd_up(failed_osd);

  // Inject read error on shard 2 for object 3 only
  // This will cause object 3's recovery to fail and need resend
  inject_read_error_for_shard(obj3_name, 2, -EIO);

  // Now trigger recovery for all 3 objects in parallel (single operation)
  // This is the key difference from the sequential test
  std::cout << "Starting parallel recovery for all 3 objects..." << std::endl;

  std::vector<std::string> obj_names = {obj1_name, obj2_name, obj3_name};
  std::vector<std::string> expected_data = {obj1_pattern_b, obj2_pattern_b, obj3_pattern_b};
  run_parallel_recovery(obj_names, false, expected_data);

  // If the bug is present, we'll crash before getting here
  // If the bug is fixed, recovery should complete successfully
  std::cout << "Parallel recovery completed for all objects" << std::endl;

  SUCCEED() << "Multi-object parallel recovery completed without crash";
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * Recreate for tracker https://tracker.ceph.com/issues/75211
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }

  // Set osd_async_recovery_min_cost to 0 to ensure even single-object
  // recovery uses async recovery. This is necessary because the test
  // harness doesn't block writes during synchronous recovery, which
  // would cause writes to missing objects to crash.
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  const std::string pattern_p2(full_stripe_size, 'B');

  // Trigger an async recovery on shard 1.
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);

  // Create a dummy object. This is purely here to be the first write in a
  // new interval, which has some special behavior.
  create_and_write_verify("dummy", pattern_p1);

  // This has the effect of preventing ops from completing.
  suspend_primary_to_osd(blocked_shard);

  // Force next partial write to go to all shards (including non-primary)
  // This uses a side effect of call_write_ordered() which causes the next op
  // to be sent to all shards, even if it is a partial write.
  ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(get_primary_backend());
  ASSERT_NE(nullptr, ec_switch) << "Primary backend must be ECSwitch";
  ec_switch->call_write_ordered([] {});

  // This is a partial write that will be sent to all shards due to the above
  // above mechanism. NOTE: This is different to the force_all_shards boolean
  // below, which generates a full write, rather than a partial write sent to
  // all shards!
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);

  // Add a full write. In the defect, the diverge log "merge" code ended up
  // using this version in the missing list - which is wrong.
  result = write(obj_name, 0, pattern_p2, full_stripe_size);
  ASSERT_EQ(-EINPROGRESS, result);

  // Mark an otherwise-uninvolved shard as down to trigger the rollback of
  // above
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  // Now run the recovery - the target shard asserts it is being written with
  // the object version it is expecting. In the defect, this assert failed.
  run_recovery(obj_name, false, pattern_p1);

  // Undo our config change!
  set_config("osd_async_recovery_min_cost", "100");
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * This is a similar scenario to the previous test, but we force the shard
 * to do a sync, rather than async recovery at the end.
 * Recreate for tracker https://tracker.ceph.com/issues/75211
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure2
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }

  // Set osd_async_recovery_min_cost to 0 to ensure even single-object
  // recovery uses async recovery. This is necessary because the test
  // harness doesn't block writes during synchronous recovery, which
  // would cause writes to missing objects to crash.
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  const std::string pattern_p2(full_stripe_size, 'B');

  // Trigger an async recovery on shard 1.
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);

  // Create a dummy object. This is purely here to be the first write in a
  // new interval, which has some special behavior.
  create_and_write_verify("dummy", pattern_p1);

  // This has the effect of preventing ops from completing.
  suspend_primary_to_osd(blocked_shard);

  // Force next partial write to go to all shards (including non-primary)
  // This uses a side effect of call_write_ordered() which causes the next op
  // to be sent to all shards, even if it is a partial write.
  ECSwitch* ec_switch = dynamic_cast<ECSwitch*>(get_primary_backend());
  ASSERT_NE(nullptr, ec_switch) << "Primary backend must be ECSwitch";
  ec_switch->call_write_ordered([] {});

  // This is a partial write that will be sent to all shards due to the above
  // above mechanism. NOTE: This is different to the force_all_shards boolean
  // below, which generates a full write, rather than a partial write sent to
  // all shards!
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);

  // Add a full write. In the defect, the diverge log "merge" code ended up
  // using this version in the missing list - which is wrong.
  result = write(obj_name, 0, pattern_p2, full_stripe_size);
  ASSERT_EQ(-EINPROGRESS, result);

  set_config("osd_async_recovery_min_cost", "100");

  // Mark an otherwise-uninvolved shard as down to trigger the rollback of
  // above
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  // Now run the recovery - the target shard asserts it is being written with
  // the object version it is expecting. In the defect, this assert failed.
  run_recovery(obj_name, false, pattern_p1);
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 */
TEST_P(TestECFailoverWithPeering, ScrubPartialWrite) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_scrub_partial_write";

  uint64_t partial_size = stripe_unit / 2;

  std::cout << "Creating partial write object with size " << partial_size
            << " bytes (stripe_unit=" << stripe_unit << ", full stripe would be "
            << (k * stripe_unit) << " bytes)" << std::endl;

  bufferlist bl = create_random_buffer(partial_size);
  std::string test_data(bl.c_str(), bl.length());

  std::cout << "Writing partial object (" << partial_size << " bytes)" << std::endl;
  create_and_write_verify(obj_name, test_data);

  write(obj_name, 0, test_data, test_data.size());

  // NOTE: Partial writes may expose scrub issues with EC pools
  std::cout << "Scrubbing partial write object to test scrub behavior" << std::endl;
  bool corruption_detected = scrub_object(obj_name);

  std::cout << "Scrub result for partial write: "
            << (corruption_detected ? "corruption detected" : "no corruption detected")
            << std::endl;

  EXPECT_FALSE(corruption_detected)
    << "scrub_object() should NOT detect corruption on valid partial write";

  std::cout << "=== ScrubPartialWrite test completed ===" << std::endl;
}

/**
 * Test rollback after a sequence of blocked full-stripe and chunk writes.
 * This is a similar scenario to the previous test, but we force the shard
 * to do a sync, rather than async recovery at the end.
 * Recreate for tracker https://tracker.ceph.com/issues/75962
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackAfterMixedBlockedWritesWithOSDFailure3
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackAfterMixedBlockedWritesWithOSDFailure requires m >= 2";
  }
  set_config("osd_async_recovery_min_cost", "0");

  const int blocked_shard = k + 1;
  const int recovery_target_shard = 1;
  const std::string obj_name = "test_mixed_blocked_writes";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_p1(full_stripe_size, 'A');
  mark_osd_down(recovery_target_shard);
  create_and_write_verify(obj_name, pattern_p1);
  mark_osd_up(recovery_target_shard);
  create_and_write_verify("dummy", pattern_p1);
  suspend_primary_to_osd(blocked_shard);
  int result = write_attribute(obj_name, "test_attr", "value2", false);
  ASSERT_EQ(-EINPROGRESS, result);
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked_shard);
  event_loop->run_until_idle();

  run_recovery(obj_name, false, pattern_p1);
}

/**
 * Test rollback of blocked WRITE operations.
 *
 * This test demonstrates rollback behavior when a write is blocked to one shard
 * and another shard fails, triggering a peering interval change and rollback.
 *
 * Test sequence, run independently for each combination of blocked_shard X
 * and failed_shard Y:
 * 1. Create an object with initial data
 * 2. Block communication to shard X
 * 3. Perform a write (should return -EINPROGRESS)
 * 4. Mark shard Y as down (triggers rollback)
 * 5. Release communication block
 * 6. Verify object has original data (rollback succeeded), then scrub for
 *    attribute consistency across shards
 *
 * This test requires m >= 2 to have multiple shards to test.
 */
TEST_P(
  TestECFailoverWithPeering,
  RollbackBlockedWrite
) {
  if (m < 2) {
    GTEST_SKIP() << "RollbackBlockedWrite requires m >= 2";
  }

  const std::string obj_name = "test_rollback";
  const size_t full_stripe_size = stripe_unit * k;
  const std::string pattern_initial(full_stripe_size, 'A');
  const std::string pattern_blocked(full_stripe_size, 'B');
  const std::string pattern_after(full_stripe_size, 'C');

  // Test all combinations of blocked shard X and failed shard Y
  // We test shards 0, 1, and k (first data, second data, first parity)
  std::vector<int> test_shards;

  for (int zone = 0; zone < num_zones; ++zone)
  {
    int zone_base = zone * (k + m);
    test_shards.push_back(zone_base + 0);
    test_shards.push_back(zone_base + 1);
    test_shards.push_back(zone_base + k);
  }

  for (int blocked_shard : test_shards) {
    for (int failed_shard : test_shards) {
      if (blocked_shard == failed_shard) {
        continue;  // Skip same shard
      }

      int primary = get_primary_shard_from_osdmap();
      if (blocked_shard == primary || failed_shard == primary)
      {
        continue;
      }

      std::cout << "\n=== Testing blocked_shard=" << blocked_shard
                << " failed_shard=" << failed_shard << " ===" << std::endl;

      // Step 1: Create object with initial data
      create_and_write_verify(obj_name, pattern_initial);

      // Step 2: Block communication to shard X
      suspend_primary_to_osd(blocked_shard);

      // Step 3: Perform a write (should return -EINPROGRESS)
      int result = write(obj_name, 0, pattern_blocked, full_stripe_size);
      ASSERT_EQ(-EINPROGRESS, result)
        << "Write should be blocked for shard " << blocked_shard;

      // Step 3a: Perform an attribute write (may or may not complete)
      // This tests whether attribute writes are also rolled back
      int attr_result = write_attribute(obj_name, "test_attr", "blocked_value", false);
      // Don't assert on the result - it may be -EINPROGRESS or succeed
      std::cout << "Attribute write result: " << attr_result << std::endl;

      // Step 4: Mark shard Y as down (triggers rollback)
      mark_osd_down(failed_shard);

      // Step 4a: Release communication block
      unsuspend_primary_to_osd(blocked_shard);
      mark_osd_up(failed_shard);

      // Step 5: Verify object has original data (rollback succeeded)
      verify_object(obj_name);

      // Step 5a: Scrub to detect attribute inconsistencies across all shards
      // This will catch if zone 1 shards failed to rollback attributes
      bool corrupted = scrub_object(obj_name);
      EXPECT_FALSE(corrupted)
        << "Object '" << obj_name << "' has inconsistent attributes after rollback "
        << "(blocked_shard=" << blocked_shard << ", failed_shard=" << failed_shard << ")";

      // Clean up for next iteration
      delete_object(obj_name);
    }
  }
}

TEST_P(TestECFailoverWithPeering, ScrubClean) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_scrub_corruption";
  uint64_t object_size = k * stripe_unit;

  bufferlist bl = create_random_buffer(object_size);
  std::string test_data(bl.c_str(), bl.length());

  std::cout << "Writing full-stripe object (" << object_size << " bytes of random data)" << std::endl;
  create_and_write_verify(obj_name, test_data);

  std::cout << "Scrubbing object to verify data integrity" << std::endl;
  bool corruption_detected = scrub_object(obj_name);

  ASSERT_FALSE(corruption_detected)
    << "scrub_object() should NOT detect corruption when data is valid";

  std::cout << "=== ScrubDetectsCorruption test completed successfully ===" << std::endl;
}

TEST_P(TestECFailoverWithPeering, ScrubDetectsCorruption) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const uint64_t object_size = k * stripe_unit;
  const std::vector<int> shard_offsets = {/*0, 1, */k};
  const bool supports_crc = ec_plugin == "isa";

  for (int zone = 0; zone < 1; ++zone) {
    for (int shard_offset : shard_offsets) {
      const int absolute_shard = shard_offset;
      const std::string obj_name =
        "test_obj_zone_" + std::to_string(zone) +
        "_shard_" + std::to_string(shard_offset);

      bufferlist bl = create_random_buffer(object_size);
      std::string test_data(bl.c_str(), bl.length());

      std::cout << "\n=== ScrubDetectsCorruption: testing zone " << zone
                << ", shard offset " << shard_offset
                << " (absolute shard " << absolute_shard << ") ===" << std::endl;

      std::cout << "Writing object " << obj_name << " (" << object_size
                << " bytes of random data)" << std::endl;
      create_and_write_verify(obj_name, test_data);

      std::cout << "Corrupting object " << obj_name
                << " for zone iteration " << zone
                << " on relative shard " << shard_offset
                << " using absolute shard " << absolute_shard << std::endl;
      hobject_t hoid = make_test_object(obj_name);
      corrupt_shard_data(hoid,
                         pg_shard_t(absolute_shard, shard_id_t(absolute_shard)));

      std::cout << "Scrubbing object " << obj_name
                << " to verify corruption detection for zone iteration " << zone
                << ", shard offset " << shard_offset << std::endl;
      // skip_verify=true: verify_object() would read through the corrupted shard
      // and produce wrong data, causing ASSERT_EQ to fire.  We skip it here
      // because the point of this test is scrub-based detection, not read-back.
      bool corruption_detected = scrub_object(obj_name, /*skip_verify=*/true);

      std::cout << "Zone iteration " << zone
                << " corruption result for shard offset " << shard_offset
                << ": " << (corruption_detected ? "detected" : "not detected")
                << " (absolute shard " << absolute_shard
                << ", supports_crc=" << (supports_crc ? "true" : "false")
                << ")" << std::endl;

      if (supports_crc) {
        EXPECT_TRUE(corruption_detected)
          << "scrub_object() should detect corruption for object " << obj_name
          << " during zone iteration " << zone
          << ", shard offset " << shard_offset
          << " (absolute shard " << absolute_shard << ")";
      } else {
        EXPECT_FALSE(corruption_detected)
            << "scrub_object() should not report corruption for object "
            << obj_name << " when CRC-based detection is unsupported"
            << " during zone iteration " << zone << ", shard offset "
            << shard_offset << " (absolute shard " << absolute_shard << ")";
      }

      // Delete the corrupted object so teardown's scrub_all_objects() (which
      // skips objects ObjectTracker reports as deleted) doesn't try to scrub
      // it and report a spurious consistency failure. Deleting the object we
      // just used, rather than marking its shard's OSD down, avoids changing
      // cluster state as a side effect purely to satisfy teardown.
      std::cout << "Deleting corrupted object " << obj_name << std::endl;
      delete_object(obj_name);
    }
  }

  std::cout << "=== ScrubDetectsCorruption test completed successfully ===" << std::endl;
}
/**
 * Test that ObjectTracker verification detects corruption during scrub.
 * This proves that the scrub integration with ObjectTracker is working.
 */
TEST_P(TestECFailoverWithPeering, ObjectTrackerDetectsCorruption) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // Enable object tracking for this test
  enable_object_tracking();
  ASSERT_TRUE(get_object_tracker() != nullptr) << "ObjectTracker should be enabled";

  const std::string obj_name = "test_tracker_corruption";
  const uint64_t object_size = k * stripe_unit;

  // Create test data
  bufferlist bl = create_random_buffer(object_size);
  std::string test_data(bl.c_str(), bl.length());

  std::cout << "Writing object '" << obj_name << "' (" << object_size << " bytes)" << std::endl;
  create_and_write_verify(obj_name, test_data);

  // Verify tracker has recorded the object
  ASSERT_TRUE(get_object_tracker()->object_exists(obj_name))
    << "ObjectTracker should have recorded the object";

  // Verify the object matches tracker expectations before corruption
  std::cout << "Verifying object before corruption" << std::endl;
  bool corruption_before = scrub_object(obj_name);
  EXPECT_FALSE(corruption_before)
    << "Scrub should not detect corruption before we corrupt the object";

  // Now corrupt the object on the primary shard
  std::cout << "Corrupting object '" << obj_name << "' on primary shard" << std::endl;
  hobject_t hoid = make_test_object(obj_name);

  // Get primary shard
  MockPGBackendListener* primary_listener = get_primary_listener();
  ASSERT_TRUE(primary_listener != nullptr) << "Should have primary listener";
  pg_shard_t primary_shard = primary_listener->pg_whoami;

  // Corrupt the data
  corrupt_shard_data(hoid, primary_shard);

  static TestECFailoverWithPeering* self;
  self = this;
  EXPECT_FATAL_FAILURE(self->verify_object("test_tracker_corruption"), "Data mismatch");

  std::cout << "Scrubbing corrupted object - should detect corruption" << std::endl;
  bool corruption_after = scrub_object(obj_name, /*skip_verify=*/true);

  const bool supports_crc = (ec_plugin == "isa");
  if (supports_crc) {
    EXPECT_TRUE(corruption_after)
      << "Scrub should detect corruption after we corrupted the object";
  } else {
    EXPECT_FALSE(corruption_after)
      << "Scrub should not report corruption for jerasure (no per-shard CRCs)";
  }

  std::cout << "Test completed: ObjectTracker "
            << (corruption_after ? "successfully detected" : "FAILED to detect")
            << " corruption during scrub" << std::endl;

  std::cout << "Deleting corrupted object " << obj_name << std::endl;
  delete_object(obj_name);
}
TEST_P(TestECFailoverWithPeering, OSD0DownAddNewOSDRecovery) {
  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  const std::string obj_name = "test_osd0_down_new_osd_recovery";
  const std::string test_data = "Data before OSD 0 failure and new OSD addition";

  // Write data to an object
  create_and_write_verify(obj_name, test_data);
  EXPECT_TRUE(primary_is_clean()) << "Primary should be clean before OSD failure";

  // Mark OSD 0 (shard 0, the primary) as down
  mark_osd_down(0);
  ASSERT_TRUE(all_shards_active()) << "PG should be active after OSD 0 failure";

  // Add a new OSD (k+m) to the cluster and assign it to shard 0
  int new_osd_id = (k + m) * num_zones;
  auto new_osdmap = std::make_shared<OSDMap>();
  new_osdmap->deepish_copy_from(*osdmap);
  OSDMapTestHelpers::new_osd_up(*new_osdmap, new_osd_id, pgid, 0);
  update_osdmap_with_peering(new_osdmap);

  // Verify peering completed with the new OSD
  ASSERT_TRUE(all_shards_active()) << "PG should be active after adding new OSD";

  // Verify the new OSD is in the acting set at shard 0
  std::vector<int> acting_osds;
  int acting_primary = -1;
  osdmap->pg_to_acting_osds(pgid, &acting_osds, &acting_primary);
  EXPECT_EQ(acting_osds[0], new_osd_id)
    << "New OSD should be at shard 0 position in acting set";

  // Perform recovery to the new OSD
  run_recovery(obj_name, true, test_data);

  // Verify the object can be read after recovery
  verify_object(obj_name);
  EXPECT_TRUE(primary_is_clean()) << "Primary should be clean after recovery";
}

/**
 * DivergentLogRewindThenSplit
 *
 * Organic reproduction of https://tracker.ceph.com/issues/68649.
 * See debug_clone_issue/BUG_68649_TIMELINE.md for the incident log trace.
 *
 *  1. Trim the pg log to a high tail T (as teuthology's low osd_*_pg_log_entries
 *     causes in the field).
 *  2. Target misses a write and rejoins as an async-recovery target
 *     (is_acting()==false, last_backfill==MAX) — the role in which append_log
 *     rolls entries forward.  (The real incident used a post-backfill target,
 *     which is equally !is_acting; async-recovery is the harness equivalent.)
 *  3. Blocked (uncommitted, partial) writes to obj_head/obj_clone.  The
 *     !is_acting target rolls them forward (crt→head), making them
 *     non-rollbackable.
 *  4. Recover the pre-existing missing (trigger) so last_complete climbs to
 *     the head through the rolled-forward entries.
 *  5. Interval change: proc_replica_log finds obj_head/obj_clone divergent with
 *     prior_version ≤ log_tail, rewinds target to empty log + missing(2) with
 *     last_complete == last_update == T.
 *  6. PG split (pg_num 1→2): child inherits the empty log + missing(2).
 *  7. Child's pg_notify carries last_complete == last_update; GetMissing's
 *     identical-log fast-path clears peer_missing[target], hiding the missing
 *     set.  A subsequent clone is forwarded to the target, which lacks the
 *     object, and crashes with ENOENT in BlueStore.
 *
 * The test asserts the primary retains peer_missing[target].is_missing for
 * obj_head/obj_clone after the split.  Fails without the reset_complete_to fix,
 * passes with it.
 */
TEST_P(TestECFailoverWithPeering, DivergentLogRewindThenSplit) {
  // Needs a coding shard (blocked) and an uninvolved data shard (failed) for
  // the superseding interval; requires k>=3 and m>=2.
  if (m < 2 || k < 3) {
    GTEST_SKIP() << "DivergentLogRewindThenSplit requires k>=3 and m>=2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // ScopedConfig guards restore these to their original values on test exit,
  // even if the test aborts early via ASSERT_*.
  ScopedConfig cfg_async_recovery("osd_async_recovery_min_cost", "0");
  ScopedConfig cfg_trim_min("osd_pg_log_trim_min", "1");
  ScopedConfig cfg_trim_max("osd_pg_log_trim_max", "1000");
  osdmap->set_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT);

  const int target = 1;
  const pg_shard_t target_shard(target, shard_id_t(target));
  const size_t data_size = stripe_unit * k;
  const std::string pa(data_size, 'A'), pb(data_size, 'B');

  // hash=1 routes obj_head/obj_clone into the child PG (seed 1) after the split.
  set_object_hash("obj_head", 1);
  set_object_hash("obj_clone", 1);

  // Phase 1: commit the objects, then build a high log tail T.
  create_and_write_verify("obj_head", pa);
  create_and_write_verify("obj_clone", pa);
  create_and_write_verify("trigger", pa);
  enable_log_trimming = true;
  set_target_pg_log_entries(1);
  for (int i = 0; i < 10; ++i) {
    write_verify("obj_head", 0, pa, data_size);
    write_verify("obj_clone", 0, pa, data_size);
  }

  // Phase 2: target misses one "trigger" write; rejoins as async-recovery
  // (is_acting()==false, last_backfill==MAX).
  mark_osd_down(target);
  write_verify("trigger", 0, pb, data_size);
  mark_osd_up(target);
  ASSERT_FALSE(get_peering_state(0)->is_acting(target_shard))
    << "Target should rejoin as an async-recovery target (!is_acting)";
  ASSERT_FALSE(get_peering_state(target)->get_info().is_incomplete())
    << "Async-recovery target should be complete (last_backfill==MAX)";

  // Phase 3: blocked writes to obj_head/obj_clone; target rolls them forward
  // (crt→head), making them non-rollbackable.
  const int blocked = k + 1;  // a coding shard
  suspend_primary_to_osd(blocked);
  ASSERT_EQ(-EINPROGRESS, write("obj_head", 0, pb, data_size));
  ASSERT_EQ(-EINPROGRESS, write("obj_clone", 0, pb, data_size));
  {
    auto* t = get_peering_state(target);
    ASSERT_EQ(t->get_pg_log().get_can_rollback_to(), t->get_pg_log().get_log().head)
      << "Target must have rolled the partial writes forward (crt==head), "
         "so they are non-rollbackable";
  }

  // Phase 4: recover "trigger" so last_complete climbs to the head through the
  // rolled-forward entries.
  run_recovery("trigger", target == 0, pb);

  // Stall reservation grants: the harness drives recovery directly, not through
  // the reservation path.  Without stalling, a grant delivered across the split's
  // interval change would hit a PeeringState in Reset and abort.
  set_stall_recovery_reservations(true);

  // Phase 5: interval change rewinds the target.  proc_replica_log finds
  // obj_head/obj_clone divergent with prior_version <= log_tail and adds them
  // to missing; the target ends up with an empty log and missing(2).
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked);
  event_loop->run_until_idle();

  {
    auto* t = get_peering_state(target);
    EXPECT_TRUE(t->get_pg_log().get_log().log.empty())
      << "Target log should be rewound to empty";
    EXPECT_EQ(t->get_pg_log().get_missing().num_missing(), 2u)
      << "Target should be missing obj_head and obj_clone";
    // reset_complete_to() on an empty-but-missing log must lower last_complete
    // below last_update; this is the direct fix assertion (tracker 68649).
    EXPECT_LT(t->get_info().last_complete, t->get_info().last_update)
      << "bug 68649: reset_complete_to must lower last_complete on empty log";
  }

  // Raise the async-recovery cost so the child peers with the target as a full
  // acting member; avoids pg_temp churn and matches the real incident (osd.11).
  g_ceph_context->_conf.set_val("osd_async_recovery_min_cost", "100");
  g_ceph_context->_conf.apply_changes(nullptr);

  // Phase 6: PG split — child inherits the empty log + missing(2).
  split_pg();

  auto* child_target = get_child_peering_state(target);
  EXPECT_TRUE(child_target->get_pg_log().get_log().log.empty())
    << "Child target log should be empty after split";
  EXPECT_EQ(child_target->get_pg_log().get_missing().num_missing(), 2u)
    << "Child target should inherit missing(2)";

  // Phase 7: assert that the primary retains peer_missing[target] entries for
  // the two affected objects.  This is the shared precondition consulted by
  // both is_degraded_or_backfilling_object() (which blocks the op for full
  // acting peers) and should_send_op() (which ships an empty op to
  // async-recovery targets).  Without the fix, GetMissing's identical-log
  // fast-path clears peer_missing[target] and neither gate fires, causing a
  // clone write to reach a shard that is missing the object → ENOENT.
  auto* child_primary = get_child_peering_state(0);
  auto primary_has_peer_missing_entry = [&](const hobject_t& soid) -> bool {
    if (child_primary->get_pg_log().get_missing().get_items().count(soid)) {
      return true;
    }
    for (const auto& peer : child_primary->get_acting_recovery_backfill()) {
      if (peer == child_primary->get_primary()) {
        continue;
      }
      auto pm = child_primary->get_peer_missing().find(peer);
      if (pm != child_primary->get_peer_missing().end() &&
          pm->second.is_missing(soid)) {
        return true;
      }
    }
    return false;
  };

  const hobject_t head = make_test_object("obj_head");
  const hobject_t clone = make_test_object("obj_clone");
  ASSERT_EQ(child_target->get_pg_log().get_missing().num_missing(), 2u);
  EXPECT_TRUE(primary_has_peer_missing_entry(head))
    << "bug 68649: peer_missing[target] cleared by GetMissing identical-log "
       "fast-path; primary would forward a write to obj_head to a shard that "
       "is missing the object -> ENOENT in BlueStore";
  EXPECT_TRUE(primary_has_peer_missing_entry(clone))
    << "bug 68649: peer_missing[target] cleared by GetMissing identical-log "
       "fast-path; primary would forward a write to obj_clone to a shard that "
       "is missing the object -> ENOENT in BlueStore";

  // cfg_async_recovery, cfg_trim_min, and cfg_trim_max restore automatically
  // via ScopedConfig destructors at function exit.
}

/**
 * DivergentLogRewindThenNewInterval
 *
 * Companion to DivergentLogRewindThenSplit that confirms the bug is not
 * specific to a PG split: any subsequent interval where the corrupt target
 * re-advertises last_complete == last_update triggers the same GetMissing
 * identical-log fast-path.
 */
TEST_P(TestECFailoverWithPeering, DivergentLogRewindThenNewInterval) {
  if (m < 2 || k < 3) {
    GTEST_SKIP() << "DivergentLogRewindThenNewInterval requires k>=3 and m>=2";
  }

  ASSERT_TRUE(all_shards_active()) << "Initial peering must complete";

  // ScopedConfig guards restore these to their original values on test exit,
  // even if the test aborts early via ASSERT_*.
  ScopedConfig cfg_async_recovery("osd_async_recovery_min_cost", "0");
  ScopedConfig cfg_trim_min("osd_pg_log_trim_min", "1");
  ScopedConfig cfg_trim_max("osd_pg_log_trim_max", "1000");
  osdmap->set_flag(CEPH_OSDMAP_PGLOG_HARDLIMIT);

  const int target = 1;
  const pg_shard_t target_shard(target, shard_id_t(target));
  const size_t data_size = stripe_unit * k;
  const std::string pa(data_size, 'A'), pb(data_size, 'B');

  // Phase 1: commit the objects, then build a high log tail T.
  create_and_write_verify("obj_head", pa);
  create_and_write_verify("obj_clone", pa);
  create_and_write_verify("trigger", pa);
  enable_log_trimming = true;
  set_target_pg_log_entries(1);
  for (int i = 0; i < 10; ++i) {
    write_verify("obj_head", 0, pa, data_size);
    write_verify("obj_clone", 0, pa, data_size);
  }

  // Phase 2: target misses a trigger write; rejoins as async-recovery (!is_acting).
  mark_osd_down(target);
  write_verify("trigger", 0, pb, data_size);
  mark_osd_up(target);
  ASSERT_FALSE(get_peering_state(0)->is_acting(target_shard));

  // Phase 3: blocked writes; target rolls them forward (crt→head), non-rollbackable.
  const int blocked = k + 1;
  suspend_primary_to_osd(blocked);
  ASSERT_EQ(-EINPROGRESS, write("obj_head", 0, pb, data_size));
  ASSERT_EQ(-EINPROGRESS, write("obj_clone", 0, pb, data_size));

  // Phase 4: recover the pre-existing missing so last_complete reaches head.
  run_recovery("trigger", target == 0, pb);
  set_stall_recovery_reservations(true);

  // Phase 5: interval change rewinds the target to an empty log with missing(2).
  mark_osd_down(2);
  unsuspend_primary_to_osd(blocked);
  event_loop->run_until_idle();
  {
    auto* t = get_peering_state(target);
    ASSERT_TRUE(t->get_pg_log().get_log().log.empty());
    ASSERT_EQ(t->get_pg_log().get_missing().num_missing(), 2u);
  }

  // Phase 6: plain new interval (no split).
  g_ceph_context->_conf.set_val("osd_async_recovery_min_cost", "100");
  g_ceph_context->_conf.apply_changes(nullptr);
  advance_epoch();

  // Phase 7: same peer_missing gate as DivergentLogRewindThenSplit.
  // After raising osd_async_recovery_min_cost to 100 above, the target is
  // a full acting peer (not async-recovery) in this interval, so
  // is_degraded_or_backfilling_object() is what guards the write.  The
  // underlying invariant in both cases is identical: peer_missing[target]
  // must contain the soid.
  auto* primary = get_peering_state(0);
  auto primary_has_peer_missing_entry = [&](const hobject_t& soid) -> bool {
    if (primary->get_pg_log().get_missing().get_items().count(soid)) {
      return true;
    }
    for (const auto& peer : primary->get_acting_recovery_backfill()) {
      if (peer == primary->get_primary()) {
        continue;
      }
      auto pm = primary->get_peer_missing().find(peer);
      if (pm != primary->get_peer_missing().end() &&
          pm->second.is_missing(soid)) {
        return true;
      }
    }
    return false;
  };

  const hobject_t head = make_test_object("obj_head");
  const hobject_t clone = make_test_object("obj_clone");
  EXPECT_TRUE(primary_has_peer_missing_entry(head))
    << "bug 68649: peer_missing[target] cleared by GetMissing identical-log "
       "fast-path; primary would forward a write to obj_head to a shard that "
       "is missing the object -> ENOENT in BlueStore";
  EXPECT_TRUE(primary_has_peer_missing_entry(clone))
    << "bug 68649: peer_missing[target] cleared by GetMissing identical-log "
       "fast-path; primary would forward a write to obj_clone to a shard that "
       "is missing the object -> ENOENT in BlueStore";

  // cfg_async_recovery, cfg_trim_min, and cfg_trim_max restore automatically
  // via ScopedConfig destructors at function exit.
}

/**
 * RollbackTruncate*
 *
 * Roll back a divergent op that truncates an object to a size that ends part
 * way through a page of the first shard, optionally writing to the object in
 * the same op. Rollback must restore the data that the truncate removed,
 * including the rest of the page holding the new end, which the truncate
 * zeroes. The truncate leaves a partial stripe at the new end, so the op also
 * rewrites the parity, which must be restored too.
 */
TEST_P(TestECFailoverWithPeering, RollbackTruncate) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t truncate_to = sw + stripe_unit / 2 + 1;
  rollback_truncate_and_write("rollback_truncate",
                              8 * sw + 3 * stripe_unit, truncate_to, {});
}

// Write below the new end, ending at it.
TEST_P(TestECFailoverWithPeering, RollbackTruncateAndWriteBelowNewEnd) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t truncate_to = sw + stripe_unit / 2 + 1;
  rollback_truncate_and_write("rollback_truncate_below",
                              8 * sw + 3 * stripe_unit, truncate_to,
                              {{truncate_to - stripe_unit / 4, stripe_unit / 4}});
}

// Write straddling the new end.
TEST_P(TestECFailoverWithPeering, RollbackTruncateAndWriteAcrossNewEnd) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t truncate_to = sw + stripe_unit / 2 + 1;
  rollback_truncate_and_write("rollback_truncate_across",
                              8 * sw + 3 * stripe_unit, truncate_to,
                              {{truncate_to - stripe_unit / 4, stripe_unit / 2}});
}

// Write starting just above the new end, in the page that holds it.
TEST_P(TestECFailoverWithPeering, RollbackTruncateAndWriteAboveNewEnd) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t truncate_to = sw + stripe_unit / 2 + 1;
  rollback_truncate_and_write("rollback_truncate_above",
                              8 * sw + 3 * stripe_unit, truncate_to,
                              {{truncate_to + 7, stripe_unit}});
}

// Write inside the truncated range, stripes above the new end.
TEST_P(TestECFailoverWithPeering, RollbackTruncateAndWriteInTruncatedRange) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t truncate_to = sw + stripe_unit / 2 + 1;
  rollback_truncate_and_write("rollback_truncate_in_range",
                              8 * sw + 3 * stripe_unit, truncate_to,
                              {{4 * sw + 7, stripe_unit}});
}

/**
 * RollbackTruncateUp
 *
 * Roll back a divergent op that truncates an object to a larger size. The
 * rollback truncates each shard back to its old size, which differs between
 * shards.
 */
TEST_P(TestECFailoverWithPeering, RollbackTruncateUp) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t object_size = 5 * sw + stripe_unit + 123;
  run_rollback("rollback_truncate_up",
               {{object_size, {}, {{Truncate(object_size + sw + 77)}}}},
               1, get_shard(k + m - 1));
}

/**
 * TruncateAndWriteInPage, WritesInPageAboveEnd
 *
 * An op that leaves a gap between two extents of a shard within a page, here
 * between the part of a page that a truncate keeps and a write further into
 * that page, or between two writes to a page above the end of the object,
 * must keep the data on either side of the gap and zero the gap.
 */
TEST_P(TestECFailoverWithPeering, TruncateAndWriteInPage) {
  const uint64_t sw = k * stripe_unit;
  run_forward("truncate_and_write_in_page",
              {{5 * sw + stripe_unit + 123, {}, {{Truncate(1), Write(8, 50)}}}});
}

TEST_P(TestECFailoverWithPeering, WritesInPageAboveEnd) {
  const uint64_t sw = k * stripe_unit;
  const uint64_t object_size = 5 * sw + stripe_unit + 123;
  const uint64_t page = ECUtil::align_next(object_size);
  run_forward("writes_in_page_above_end",
              {{object_size, {}, {{Write(page + 10, 10), Write(page + 100, 10)}}}});
}

// ---------------------------------------------------------------------------
// Instantiate TestECFailoverWithPeering with EC configurations
// ---------------------------------------------------------------------------

INSTANTIATE_TEST_SUITE_P(
  ECConfigs,
  TestECFailoverWithPeering,
  ::testing::ValuesIn(kECPeeringConfigs),
  [](const ::testing::TestParamInfo<BackendConfig>& info) {
    return info.param.label;
  }
);

/**
 * TestECTruncateMatrix
 *
 * Ops mixing truncates, writes and zeros on optimized EC objects, drawn from
 * a table of cases with offsets computed from k and the stripe unit. Each
 * case is run forward, where the objects and their shards must match a model
 * of the expected contents, including after a data shard goes down, and
 * rolled back after an OSD fails, where the objects and their shards must be
 * restored exactly. Rollback is run with the op blocked to shard 1 and the
 * last parity shard, a data shard other than the first two or the primary
 * failing, and with the op blocked to the first parity shard, which every op
 * writes, and a data shard or the primary failing.
 */
namespace {

/* Rollback modes block the op to shard 1, which it may not write, or to the
 * first parity shard, which it always writes, and fail the named shard. */
enum class TruncateMode {
  Forward,
  RollbackParity,
  RollbackData,
  RollbackPrimary,
  BlockParityRollbackData,
  BlockParityRollbackPrimary,
};

struct TruncateCase {
  std::string name;
  /* The objects of the case for k and stripe unit su, or nullopt if the case
   * means nothing for them. */
  std::function<std::optional<std::vector<ObjectScenario>>(uint64_t k,
                                                           uint64_t su)> make;
};

constexpr uint64_t PAGE = EC_ALIGN_SIZE;

enum class SizeKind { ChunkAligned, PageAligned, Unaligned, SubStripe };

const std::vector<std::pair<SizeKind, std::string>> kSizeKinds = {
  {SizeKind::ChunkAligned, "ChunkAligned"},
  {SizeKind::PageAligned, "PageAligned"},
  {SizeKind::Unaligned, "Unaligned"},
  {SizeKind::SubStripe, "SubStripe"},
};

uint64_t object_size(SizeKind kind, uint64_t k, uint64_t su) {
  const uint64_t sw = k * su;
  switch (kind) {
  case SizeKind::ChunkAligned: return 8 * sw + 3 * su;
  case SizeKind::PageAligned: return 5 * sw + su + PAGE;
  case SizeKind::Unaligned: return 5 * sw + su + 123;
  case SizeKind::SubStripe: return sw / 2 + 1234;
  }
  ceph_abort();
}

enum class TargetKind {
  Chunk0MidPage,
  Chunk0Page,
  ChunkBoundary,
  MiddleChunkMidPage,
  LastChunk,
  StripeBoundary,
  LastPage,
  One,
  Zero,
  Equal,
  Grow,
};

const std::vector<std::pair<TargetKind, std::string>> kTargetKinds = {
  {TargetKind::Chunk0MidPage, "Chunk0MidPage"},
  {TargetKind::Chunk0Page, "Chunk0Page"},
  {TargetKind::ChunkBoundary, "ChunkBoundary"},
  {TargetKind::MiddleChunkMidPage, "MiddleChunkMidPage"},
  {TargetKind::LastChunk, "LastChunk"},
  {TargetKind::StripeBoundary, "StripeBoundary"},
  {TargetKind::LastPage, "LastPage"},
  {TargetKind::One, "One"},
  {TargetKind::Zero, "Zero"},
  {TargetKind::Equal, "Equal"},
  {TargetKind::Grow, "Grow"},
};

/* The size to truncate an object of size s to, or nullopt if the target
 * means nothing for this geometry. Targets inside chunks are in the second
 * stripe, if the object has more than two stripes, so that the stripe below
 * is left intact. */
std::optional<uint64_t> truncate_target(TargetKind kind, uint64_t s,
                                        uint64_t k, uint64_t su) {
  const uint64_t sw = k * su;
  const uint64_t base = s >= 2 * sw ? sw : 0;
  std::optional<uint64_t> target;
  switch (kind) {
  case TargetKind::Chunk0MidPage:
    target = base + su / 2 + 1;
    break;
  case TargetKind::Chunk0Page:
    if (su > PAGE) {
      target = base + PAGE;
    }
    break;
  case TargetKind::ChunkBoundary:
    target = base + su;
    break;
  case TargetKind::MiddleChunkMidPage:
    if (k > 2) {
      target = base + (k / 2) * su + su / 2 + 3;
    }
    break;
  case TargetKind::LastChunk:
    target = base + (k - 1) * su + su / 4 + 5;
    break;
  case TargetKind::StripeBoundary:
    target = base + sw;
    break;
  case TargetKind::LastPage: {
    const uint64_t page_start = (s - 1) / PAGE * PAGE;
    target = page_start + (s - page_start) / 2;
    break;
  }
  case TargetKind::One:
    target = 1;
    break;
  case TargetKind::Zero:
    target = 0;
    break;
  case TargetKind::Equal:
    return s;
  case TargetKind::Grow:
    return s + sw + 77;
  }
  if (target && *target >= s) {
    return std::nullopt;
  }
  return target;
}

enum class WriteKind {
  None,
  Below,
  EndingAt,
  Straddling,
  AboveInPage,
  InTruncatedRange,
  PastEnd,
  Several,
  ZeroInside,
};

const std::vector<std::pair<WriteKind, std::string>> kWriteKinds = {
  {WriteKind::None, "NoWrite"},
  {WriteKind::Below, "WriteBelow"},
  {WriteKind::EndingAt, "WriteEndingAt"},
  {WriteKind::Straddling, "WriteStraddling"},
  {WriteKind::AboveInPage, "WriteAboveInPage"},
  {WriteKind::InTruncatedRange, "WriteInTruncatedRange"},
  {WriteKind::PastEnd, "WritePastEnd"},
  {WriteKind::Several, "SeveralWrites"},
  {WriteKind::ZeroInside, "ZeroInside"},
};

/* The ops that follow a truncate from size s to t in the same op, or nullopt
 * if the kind means nothing for these sizes. */
std::optional<std::vector<ObjOp>> writes_after_truncate(
    WriteKind kind, uint64_t s, uint64_t t, uint64_t k, uint64_t su) {
  const uint64_t sw = k * su;
  const uint64_t end = std::max(s, t);
  switch (kind) {
  case WriteKind::None:
    return std::vector<ObjOp>{};
  case WriteKind::Below:
    if (t < 8) {
      return std::nullopt;
    }
    return std::vector<ObjOp>{Write(t / 3, std::min<uint64_t>(t / 3, 3000))};
  case WriteKind::EndingAt:
    if (t == 0) {
      return std::nullopt;
    }
    return std::vector<ObjOp>{Write(t - std::min<uint64_t>(t, 777),
                                    std::min<uint64_t>(t, 777))};
  case WriteKind::Straddling:
    return std::vector<ObjOp>{Write(t - std::min<uint64_t>(t, 500),
                                    std::min<uint64_t>(t, 500) + 700)};
  case WriteKind::AboveInPage:
    return std::vector<ObjOp>{Write(t + 7, 50)};
  case WriteKind::InTruncatedRange:
    if (t + sw + 13 + 1000 >= s) {
      return std::nullopt;
    }
    return std::vector<ObjOp>{Write(t + sw + 13, 1000)};
  case WriteKind::PastEnd:
    return std::vector<ObjOp>{Write(end + su + 5, 999)};
  case WriteKind::Several: {
    std::vector<ObjOp> ops;
    if (t >= 3) {
      ops.push_back(Write(t / 3, std::min<uint64_t>(t / 3, 100)));
    }
    ops.push_back(Write(t - std::min<uint64_t>(t, 40),
                        std::min<uint64_t>(t, 40) + 60));
    ops.push_back(Write(end + 100, 300));
    return ops;
  }
  case WriteKind::ZeroInside:
    if (t < 4) {
      return std::nullopt;
    }
    return std::vector<ObjOp>{Zero(t / 4, std::min<uint64_t>(t / 2, su))};
  }
  ceph_abort();
}

std::vector<TruncateCase> make_truncate_cases() {
  std::vector<TruncateCase> cases;

  /* Every target with every kind of write for the chunk aligned and
   * unaligned sizes, and with a few kinds for the others. */
  for (const auto& [size_kind, size_name] : kSizeKinds) {
    const bool all_writes = size_kind == SizeKind::ChunkAligned ||
                            size_kind == SizeKind::Unaligned;
    for (const auto& [target_kind, target_name] : kTargetKinds) {
      for (const auto& [write_kind, write_name] : kWriteKinds) {
        if (!all_writes && write_kind != WriteKind::None &&
            write_kind != WriteKind::Straddling &&
            write_kind != WriteKind::AboveInPage) {
          continue;
        }
        cases.push_back({
          size_name + "_Truncate" + target_name + "_" + write_name,
          [size_kind, target_kind, write_kind](uint64_t k, uint64_t su)
              -> std::optional<std::vector<ObjectScenario>> {
            const uint64_t s = object_size(size_kind, k, su);
            const auto t = truncate_target(target_kind, s, k, su);
            if (!t) {
              return std::nullopt;
            }
            auto writes = writes_after_truncate(write_kind, s, *t, k, su);
            if (!writes) {
              return std::nullopt;
            }
            std::vector<ObjOp> ops{Truncate(*t)};
            ops.insert(ops.end(), writes->begin(), writes->end());
            return std::vector<ObjectScenario>{{s, {}, {ops}}};
          }});
      }
    }
  }

  /* Cases on an unaligned object with a low target t1 in the first chunk of
   * the second stripe and a higher target t2 in the fourth stripe. */
  auto add = [&cases](const std::string& name,
                      std::function<std::vector<ObjectScenario>(
                        uint64_t s, uint64_t t1, uint64_t t2,
                        uint64_t sw, uint64_t su)> make) {
    cases.push_back({name, [make](uint64_t k, uint64_t su)
        -> std::optional<std::vector<ObjectScenario>> {
      const uint64_t sw = k * su;
      const uint64_t s = object_size(SizeKind::Unaligned, k, su);
      return make(s, sw + su / 2 + 1, 3 * sw + su + 555, sw, su);
    }});
  };

  // Several truncates in one op.
  add("TruncateDownThenUp", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Truncate(t1), Truncate(t2)}}}};
  });
  add("TruncateDownThenUpThenWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1), Truncate(t2), Write(t1 - 100, 400)}}}};
  });
  add("TruncateDownWriteUp", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1), Write(t1 + 300, 2000), Truncate(t2)}}}};
  });
  add("TruncateUpThenDown", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Truncate(s + sw), Truncate(t1)}}}};
  });
  add("TruncateUpWriteDown", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(s + sw), Write(s + 100, 500), Truncate(t1)}}}};
  });
  add("WriteThenTruncateCuttingIt", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Write(t1 - 300, 1000), Truncate(t1)}}}};
  });
  add("WriteThenTruncateUp", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Write(t1 - 300, 1000), Truncate(s + su + 3)}}}};
  });
  add("TruncateWriteTruncateCuttingIt", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t2), Write(t1 - 50, 3000), Truncate(t1 + 1000)}}}};
  });

  // Several ops in flight on one object.
  add("ChainTruncateThenWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1)}, {Write(t1 - 100, 300)}}}};
  });
  add("ChainWriteThenTruncate", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Write(t1 - 100, 300)}, {Truncate(t1 - 50)}}}};
  });
  add("ChainTruncateThenTruncate", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Truncate(t2)}, {Truncate(t1)}}}};
  });
  add("ChainTruncateThenAppend", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Truncate(t1)}, {Write(t1, 5000)}}}};
  });
  add("ChainTruncateWriteTruncate", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t2)}, {Write(t1 - 10, 100)}, {Truncate(t1)}}}};
  });
  add("ChainTruncateThenGrowingWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1)}, {Write(s + 100, 5000)}}}};
  });
  add("ChainTruncateToZeroThenWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {}, {{Truncate(0)}, {Write(0, 3000)}}}};
  });
  add("ChainWriteThenTruncateThenWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Write(t1 - 5, su)}, {Truncate(t1)}, {Write(t1 + 50, 100)}}}};
  });

  // A committed op followed by ops in flight.
  add("CommittedTruncateThenWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {{Truncate(t1)}}, {{Write(t1 - 100, 3000)}}}};
  });
  add("CommittedTruncateThenTruncate", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{{s, {{Truncate(t2)}}, {{Truncate(t1)}}}};
  });
  add("CommittedWriteThenTruncateAndWrite", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {{Write(t1 - 5, 10)}}, {{Truncate(t1), Write(t1 - 20, 40)}}}};
  });

  // Two objects with ops in flight together.
  add("TwoObjects", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1), Write(t1 - 100, 300)}}},
      {8 * sw + 3 * su, {}, {{Write(t1 - 50, 200)}, {Truncate(t1 + su)}}}};
  });
  add("TwoObjectsSameOps", [](auto s, auto t1, auto t2, auto sw, auto su) {
    return std::vector<ObjectScenario>{
      {s, {}, {{Truncate(t1)}, {Write(t1 - 100, 300)}}},
      {s, {}, {{Truncate(t1)}, {Write(t1 - 100, 300)}}}};
  });

  return cases;
}

const std::vector<TruncateCase> kTruncateCases = make_truncate_cases();

}  // namespace

class TestECTruncateMatrix
  : public ECTruncateTestBase,
    public ::testing::WithParamInterface<
      std::tuple<BackendConfig, TruncateCase, TruncateMode>> {
public:
  TestECTruncateMatrix() : ECTruncateTestBase(std::get<0>(GetParam())) {}
};

TEST_P(TestECTruncateMatrix, Run) {
  const auto& [config, truncate_case, mode] = GetParam();
  const auto objects = truncate_case.make(k, stripe_unit);
  if (!objects) {
    GTEST_SKIP() << truncate_case.name << " does not apply to this geometry";
  }
  SCOPED_TRACE(config.label + " " + truncate_case.name);

  switch (mode) {
  case TruncateMode::Forward:
    run_forward(truncate_case.name, *objects);
    break;
  case TruncateMode::RollbackParity:
    run_rollback(truncate_case.name, *objects, 1, get_shard(k + m - 1));
    break;
  case TruncateMode::RollbackData:
    if (k <= 2) {
      GTEST_SKIP() << "needs a data shard other than the first two";
    }
    run_rollback(truncate_case.name, *objects, 1, get_shard(2));
    break;
  case TruncateMode::RollbackPrimary:
    run_rollback(truncate_case.name, *objects, 1,
                 get_primary_shard_from_osdmap());
    break;
  case TruncateMode::BlockParityRollbackData:
    run_rollback(truncate_case.name, *objects, get_shard(k),
                 get_shard(k > 2 ? 2 : 1));
    break;
  case TruncateMode::BlockParityRollbackPrimary:
    run_rollback(truncate_case.name, *objects, get_shard(k),
                 get_primary_shard_from_osdmap());
    break;
  }
}

std::string truncate_matrix_name(
    const ::testing::TestParamInfo<TestECTruncateMatrix::ParamType>& info) {
  static const std::map<TruncateMode, std::string> mode_names = {
    {TruncateMode::Forward, "Forward"},
    {TruncateMode::RollbackParity, "RollbackParity"},
    {TruncateMode::RollbackData, "RollbackData"},
    {TruncateMode::RollbackPrimary, "RollbackPrimary"},
    {TruncateMode::BlockParityRollbackData, "BlockParityRollbackData"},
    {TruncateMode::BlockParityRollbackPrimary, "BlockParityRollbackPrimary"},
  };
  return std::get<0>(info.param).label + "_" + std::get<1>(info.param).name +
         "_" + mode_names.at(std::get<2>(info.param));
}

INSTANTIATE_TEST_SUITE_P(
  ECConfigs,
  TestECTruncateMatrix,
  ::testing::Combine(
    ::testing::ValuesIn(kECPeeringConfigs),
    ::testing::ValuesIn(kTruncateCases),
    ::testing::Values(TruncateMode::Forward,
                      TruncateMode::RollbackParity,
                      TruncateMode::RollbackData,
                      TruncateMode::RollbackPrimary,
                      TruncateMode::BlockParityRollbackData,
                      TruncateMode::BlockParityRollbackPrimary)),
  truncate_matrix_name
);

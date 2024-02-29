// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct cache_test_t : public seastar_test_suite_t {
  segment_manager::EphemeralSegmentManagerRef segment_manager;
  ExtentPlacementManagerRef epm;
  CacheRef cache;
  paddr_t current;
  journal_seq_t seq = JOURNAL_SEQ_MIN;

  cache_test_t() = default;

  seastar::future<paddr_t> submit_transaction(
    TransactionRef t) {
    t->for_each_fresh_block_pre_submit([](auto &extent) {
      if (!extent->is_logical() ||
	  !extent->get_last_committed_crc()) {
	extent->update_checksum();
      }
      assert(extent->calc_crc32c() == extent->get_last_committed_crc());
    });
    auto record = cache->prepare_record(*t, JOURNAL_SEQ_NULL, JOURNAL_SEQ_NULL);

    bufferlist bl;
    for (auto &&block : record.extents) {
      bl.append(block.bl);
    }

    ceph_assert((segment_off_t)bl.length() <
		segment_manager->get_segment_size());
    if (current.as_seg_paddr().get_segment_off() + (segment_off_t)bl.length() >
	segment_manager->get_segment_size())
      current = paddr_t::make_seg_paddr(
	segment_id_t(
	  current.as_seg_paddr().get_segment_id().device_id(),
	  current.as_seg_paddr().get_segment_id().device_segment_id() + 1),
	0);

    auto prev = current;
    current.as_seg_paddr().set_segment_off(
      current.as_seg_paddr().get_segment_off()
      + bl.length());
    return segment_manager->segment_write(
      prev,
      std::move(bl),
      true
    ).safe_then(
      [this, prev, t=std::move(t)]() mutable {
	cache->complete_commit(*t, prev, seq /* TODO */);
        return prev;
      },
      crimson::ct_error::all_same_way([](auto e) {
	ASSERT_FALSE("failed to submit");
      })
     );
  }

  auto get_transaction() {
    return cache->create_transaction(
        Transaction::src_t::MUTATE, "test_cache", false);
  }

  template <typename T, typename... Args>
  auto get_extent(Transaction &t, Args&&... args) {
    return with_trans_intr(
      t,
      [this](auto &&... args) {
	return cache->get_extent<T>(args...);
      },
      std::forward<Args>(args)...);
  }

  seastar::future<> set_up_fut() final {
    segment_manager = segment_manager::create_test_ephemeral();
    return segment_manager->init(
    ).safe_then([this] {
      return segment_manager->mkfs(
        segment_manager::get_ephemeral_device_config(0, 1, 0));
    }).safe_then([this] {
      epm.reset(new ExtentPlacementManager());
      cache.reset(new Cache(*epm));
      current = paddr_t::make_seg_paddr(segment_id_t(segment_manager->get_device_id(), 0), 0);
      epm->test_init_no_background(segment_manager.get());
      return seastar::do_with(
          get_transaction(),
          [this](auto &ref_t) {
        cache->init();
        return with_trans_intr(*ref_t, [&](auto &t) {
          return cache->mkfs(t);
        }).safe_then([this, &ref_t] {
          return submit_transaction(std::move(ref_t)
          ).then([](auto p) {});
        });
      });
    }).handle_error(
      crimson::ct_error::all_same_way([](auto e) {
        ASSERT_FALSE("failed to submit");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return cache->close(
    ).safe_then([this] {
      segment_manager.reset();
      epm.reset();
      cache.reset();
    }).handle_error(
      Cache::close_ertr::assert_all{}
    );
  }
};

TEST_F(cache_test_t, test_addr_fixup)
{
  run_async([this] {
    paddr_t addr;
    int csum = 0;
    {
      auto t = get_transaction();
      auto extent = cache->alloc_new_non_data_extent<TestBlockPhysical>(
	*t,
	TestBlockPhysical::SIZE,
	placement_hint_t::HOT,
	0);
      extent->set_contents('c');
      csum = extent->calc_crc32c();
      submit_transaction(std::move(t)).get0();
      addr = extent->get_paddr();
    }
    {
      auto t = get_transaction();
      auto extent = get_extent<TestBlockPhysical>(
	*t,
	addr,
	TestBlockPhysical::SIZE).unsafe_get0();
      ASSERT_EQ(extent->get_paddr(), addr);
      ASSERT_EQ(extent->calc_crc32c(), csum);
    }
  });
}

TEST_F(cache_test_t, test_dirty_extent)
{
  run_async([this] {
    paddr_t addr;
    int csum = 0;
    int csum2 = 0;
    {
      // write out initial test block
      auto t = get_transaction();
      auto extent = cache->alloc_new_non_data_extent<TestBlockPhysical>(
	*t,
	TestBlockPhysical::SIZE,
	placement_hint_t::HOT,
	0);
      extent->set_contents('c');
      csum = extent->calc_crc32c();
      auto reladdr = extent->get_paddr();
      ASSERT_TRUE(reladdr.is_relative());
      {
	// test that read with same transaction sees new block though
	// uncommitted
	auto extent = get_extent<TestBlockPhysical>(
	  *t,
	  reladdr,
	  TestBlockPhysical::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_clean());
	ASSERT_TRUE(extent->is_pending());
	ASSERT_TRUE(extent->get_paddr().is_relative());
	ASSERT_EQ(extent->get_version(), 0);
	ASSERT_EQ(csum, extent->calc_crc32c());
      }
      submit_transaction(std::move(t)).get0();
      addr = extent->get_paddr();
    }
    {
      // test that consecutive reads on the same extent get the same ref
      auto t = get_transaction();
      auto extent = get_extent<TestBlockPhysical>(
	*t,
	addr,
	TestBlockPhysical::SIZE).unsafe_get0();
      auto t2 = get_transaction();
      auto extent2 = get_extent<TestBlockPhysical>(
	*t2,
	addr,
	TestBlockPhysical::SIZE).unsafe_get0();
      ASSERT_EQ(&*extent, &*extent2);
    }
    {
      // read back test block
      auto t = get_transaction();
      auto extent = get_extent<TestBlockPhysical>(
	*t,
	addr,
	TestBlockPhysical::SIZE).unsafe_get0();
      // duplicate and reset contents
      extent = cache->duplicate_for_write(*t, extent)->cast<TestBlockPhysical>();
      extent->set_contents('c');
      csum2 = extent->calc_crc32c();
      ASSERT_EQ(extent->get_paddr(), addr);
      {
	// test that concurrent read with fresh transaction sees old
        // block
	auto t2 = get_transaction();
	auto extent = get_extent<TestBlockPhysical>(
	  *t2,
	  addr,
	  TestBlockPhysical::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_clean());
	ASSERT_FALSE(extent->is_pending());
	ASSERT_EQ(addr, extent->get_paddr());
	ASSERT_EQ(extent->get_version(), 0);
	ASSERT_EQ(csum, extent->calc_crc32c());
      }
      {
	// test that read with same transaction sees new block
	auto extent = get_extent<TestBlockPhysical>(
	  *t,
	  addr,
	  TestBlockPhysical::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_dirty());
	ASSERT_TRUE(extent->is_pending());
	ASSERT_EQ(addr, extent->get_paddr());
	ASSERT_EQ(extent->get_version(), 1);
	ASSERT_EQ(csum2, extent->calc_crc32c());
      }
      // submit transaction
      submit_transaction(std::move(t)).get0();
      ASSERT_TRUE(extent->is_dirty());
      ASSERT_EQ(addr, extent->get_paddr());
      ASSERT_EQ(extent->get_version(), 1);
      ASSERT_EQ(extent->calc_crc32c(), csum2);
    }
    {
      // test that fresh transaction now sees newly dirty block
      auto t = get_transaction();
      auto extent = get_extent<TestBlockPhysical>(
	*t,
	addr,
	TestBlockPhysical::SIZE).unsafe_get0();
      ASSERT_TRUE(extent->is_dirty());
      ASSERT_EQ(addr, extent->get_paddr());
      ASSERT_EQ(extent->get_version(), 1);
      ASSERT_EQ(csum2, extent->calc_crc32c());
    }
  });
}

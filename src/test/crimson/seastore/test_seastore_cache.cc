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
  segment_manager::EphemeralSegmentManager segment_manager;
  Cache cache;
  paddr_t current{0, 0};

  cache_test_t()
    : segment_manager(segment_manager::DEFAULT_TEST_EPHEMERAL),
      cache(segment_manager) {}

  seastar::future<std::optional<paddr_t>> submit_transaction(
    TransactionRef t) {
    auto record = cache.try_construct_record(*t);
    if (!record) {
      return seastar::make_ready_future<std::optional<paddr_t>>(
	std::nullopt);
    }

    bufferlist bl;
    for (auto &&block : record->extents) {
      bl.append(block.bl);
    }

    ceph_assert((segment_off_t)bl.length() <
		segment_manager.get_segment_size());
    if (current.offset + (segment_off_t)bl.length() >
	segment_manager.get_segment_size())
      current = paddr_t{current.segment + 1, 0};

    auto prev = current;
    current.offset += bl.length();
    return segment_manager.segment_write(
      prev,
      std::move(bl),
      true
    ).safe_then(
      [this, prev, t=std::move(t)] {
	cache.complete_commit(*t, prev);
	return seastar::make_ready_future<std::optional<paddr_t>>(prev);
      },
      crimson::ct_error::all_same_way([](auto e) {
	ASSERT_FALSE("failed to submit");
      })
     );
  }

  auto get_transaction() {
    return TransactionRef(new Transaction);
  }

  seastar::future<> set_up_fut() final {
    return segment_manager.init().safe_then(
      [this] {
	return seastar::do_with(
	  TransactionRef(new Transaction()),
	  [this](auto &transaction) {
	    cache.init();
	    return cache.mkfs(*transaction).safe_then(
	      [this, &transaction] {
		return submit_transaction(std::move(transaction)).then(
		  [](auto p) {
		    ASSERT_TRUE(p);
		  });
	      });
	  });
      }).handle_error(
	crimson::ct_error::all_same_way([](auto e) {
	  ASSERT_FALSE("failed to submit");
	})
      );
  }

  seastar::future<> tear_down_fut() final {
    return cache.close().handle_error(
      Cache::close_ertr::assert_all{});
  }
};

TEST_F(cache_test_t, test_addr_fixup)
{
  run_async([this] {
    paddr_t addr;
    int csum = 0;
    {
      auto t = get_transaction();
      auto extent = cache.alloc_new_extent<TestBlock>(
	*t,
	TestBlock::SIZE);
      extent->set_contents('c');
      csum = extent->checksum();
      auto ret = submit_transaction(std::move(t)).get0();
      ASSERT_TRUE(ret);
      addr = extent->get_paddr();
    }
    {
      auto t = get_transaction();
      auto extent = cache.get_extent<TestBlock>(
	*t,
	addr,
	TestBlock::SIZE).unsafe_get0();
      ASSERT_EQ(extent->get_paddr(), addr);
      ASSERT_EQ(extent->checksum(), csum);
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
      auto extent = cache.alloc_new_extent<TestBlock>(
	*t,
	TestBlock::SIZE);
      extent->set_contents('c');
      csum = extent->checksum();
      auto reladdr = extent->get_paddr();
      ASSERT_TRUE(reladdr.is_relative());
      {
	// test that read with same transaction sees new block though
	// uncommitted
	auto extent = cache.get_extent<TestBlock>(
	  *t,
	  reladdr,
	  TestBlock::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_clean());
	ASSERT_TRUE(extent->is_pending());
	ASSERT_TRUE(extent->get_paddr().is_relative());
	ASSERT_EQ(extent->get_version(), 0);
	ASSERT_EQ(csum, extent->checksum());
      }
      auto ret = submit_transaction(std::move(t)).get0();
      ASSERT_TRUE(ret);
      addr = extent->get_paddr();
    }
    {
      // test that consecutive reads on the same extent get the same ref
      auto t = get_transaction();
      auto extent = cache.get_extent<TestBlock>(
	*t,
	addr,
	TestBlock::SIZE).unsafe_get0();
      auto t2 = get_transaction();
      auto extent2 = cache.get_extent<TestBlock>(
	*t2,
	addr,
	TestBlock::SIZE).unsafe_get0();
      ASSERT_EQ(&*extent, &*extent2);
    }
    {
      // read back test block
      auto t = get_transaction();
      auto extent = cache.get_extent<TestBlock>(
	*t,
	addr,
	TestBlock::SIZE).unsafe_get0();
      // duplicate and reset contents
      extent = cache.duplicate_for_write(*t, extent)->cast<TestBlock>();
      extent->set_contents('c');
      csum2 = extent->checksum();
      ASSERT_EQ(extent->get_paddr(), addr);
      {
	// test that concurrent read with fresh transaction sees old
        // block
	auto t2 = get_transaction();
	auto extent = cache.get_extent<TestBlock>(
	  *t2,
	  addr,
	  TestBlock::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_clean());
	ASSERT_FALSE(extent->is_pending());
	ASSERT_EQ(addr, extent->get_paddr());
	ASSERT_EQ(extent->get_version(), 0);
	ASSERT_EQ(csum, extent->checksum());
      }
      {
	// test that read with same transaction sees new block
	auto extent = cache.get_extent<TestBlock>(
	  *t,
	  addr,
	  TestBlock::SIZE).unsafe_get0();
	ASSERT_TRUE(extent->is_dirty());
	ASSERT_TRUE(extent->is_pending());
	ASSERT_EQ(addr, extent->get_paddr());
	ASSERT_EQ(extent->get_version(), 1);
	ASSERT_EQ(csum2, extent->checksum());
      }
      // submit transaction
      auto ret = submit_transaction(std::move(t)).get0();
      ASSERT_TRUE(ret);
      ASSERT_TRUE(extent->is_dirty());
      ASSERT_EQ(addr, extent->get_paddr());
      ASSERT_EQ(extent->get_version(), 1);
      ASSERT_EQ(extent->checksum(), csum2);
    }
    {
      // test that fresh transaction now sees newly dirty block
      auto t = get_transaction();
      auto extent = cache.get_extent<TestBlock>(
	*t,
	addr,
	TestBlock::SIZE).unsafe_get0();
      ASSERT_TRUE(extent->is_dirty());
      ASSERT_EQ(addr, extent->get_paddr());
      ASSERT_EQ(extent->get_version(), 1);
      ASSERT_EQ(csum2, extent->checksum());
    }
  });
}

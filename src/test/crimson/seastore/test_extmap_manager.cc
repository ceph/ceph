// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/extentmap_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}


struct extentmap_manager_test_t : public seastar_test_suite_t {
  std::unique_ptr<SegmentManager> segment_manager;
  Journal journal;
  Cache cache;
  LBAManagerRef lba_manager;
  TransactionManager tm;
  ExtentMapManagerRef extmap_manager;

  extentmap_manager_test_t()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(
	lba_manager::create_lba_manager(*segment_manager, cache)),
      tm(*segment_manager, journal, cache, *lba_manager),
      extmap_manager(
        extentmap_manager::create_extentmap_manager(tm)) {}

  seastar::future<> set_up_fut() final {
    return segment_manager->init().safe_then([this] {
      return tm.mkfs();
    }).safe_then([this] {
      return tm.mount();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to mount");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return tm.close().handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }

  using test_extmap_t = std::map<uint32_t, lext_map_val_t>;
  test_extmap_t test_ext_mappings;

  extent_mapping_t insert_extent(
    extmap_root_t &extmap_root,
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val) {
    auto extent = extmap_manager->add_lextent(extmap_root, t, lo, val).unsafe_get0();
    EXPECT_EQ(lo, extent.logical_offset);
    EXPECT_EQ(val.laddr, extent.laddr);
    EXPECT_EQ(val.length, extent.length);
    test_ext_mappings.emplace(std::make_pair(extent.logical_offset,
      lext_map_val_t{extent.laddr, extent.length}));
    return extent;
  }

  extent_map_list_t find_extent(
    extmap_root_t &extmap_root,
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->find_lextent(extmap_root, t, lo, len).unsafe_get0();
    EXPECT_EQ(lo, extent.front().logical_offset);
    EXPECT_EQ(len, extent.front().length);
    return extent;
  }

  extent_map_list_t findno_extent(
    extmap_root_t &extmap_root,
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->find_lextent(extmap_root, t, lo, len).unsafe_get0();
    EXPECT_EQ(extent.empty(), true);
    return extent;
  }

  bool rm_extent(
    extmap_root_t &extmap_root,
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val ) {
    auto ret = extmap_manager->rm_lextent(extmap_root, t, lo, val).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_ext_mappings.erase(test_ext_mappings.find(lo));
    return ret;
  }

  void check_mappings(extmap_root_t &extmap_root, Transaction &t) {
    for (auto &&i: test_ext_mappings){
      auto ret_list = find_extent(extmap_root, t, i.first, i.second.length);
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.laddr, ret.laddr);
      EXPECT_EQ(i.second.length, ret.length);
    }
  }

  void check_mappings(extmap_root_t &extmap_root) {
    auto t = tm.create_transaction();
    check_mappings(extmap_root, *t);
  }

};

TEST_F(extentmap_manager_test_t, basic)
{
  run_async([this] {
    extmap_root_t extmap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      extmap_root = extmap_manager->initialize_extmap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }

    uint32_t len = 4096;
    uint32_t lo = 0x1 * len;
    {
      auto t = tm.create_transaction();
      logger().debug("first transaction");
      [[maybe_unused]] auto addref = insert_extent(extmap_root, *t, lo, {lo, len});
      [[maybe_unused]] auto seekref = find_extent(extmap_root, *t, lo, len);
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm.create_transaction();
      logger().debug("second transaction");
      [[maybe_unused]] auto seekref = find_extent(extmap_root, *t, lo, len);
      [[maybe_unused]] auto rmret = rm_extent(extmap_root, *t, lo, {seekref.front().laddr, len});
      [[maybe_unused]] auto seekref2 = findno_extent(extmap_root, *t, lo, len);
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    {
      auto t = tm.create_transaction();
      logger().debug("third transaction");
      [[maybe_unused]] auto seekref = findno_extent(extmap_root, *t, lo, len);
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
  });
}

TEST_F(extentmap_manager_test_t, force_split)
{
  run_async([this] {
    extmap_root_t extmap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      extmap_root = extmap_manager->initialize_extmap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        [[maybe_unused]] auto addref = insert_extent(extmap_root, *t, lo, {lo, len});
        lo += len;
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(extmap_root, *t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm.submit_transaction(std::move(t)).unsafe_get();
      check_mappings(extmap_root);
    }
  });

}

TEST_F(extentmap_manager_test_t, force_split_merge)
{
  run_async([this] {
    extmap_root_t extmap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      extmap_root = extmap_manager->initialize_extmap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 80; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        [[maybe_unused]] auto addref = insert_extent(extmap_root, *t, lo, {lo, len});
        lo += len;
	if ((i % 10 == 0) && (j == 3)) {
	  check_mappings(extmap_root, *t);
	}
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
	check_mappings(extmap_root);
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i % 3 != 0) {
	[[maybe_unused]] auto rmref= rm_extent(extmap_root, *t, e.first, e.second);
      }

      if (i % 10 == 0) {
        logger().debug("submitting transaction i= {}", i);
	tm.submit_transaction(std::move(t)).unsafe_get();
	t = tm.create_transaction();
      }
      if (i % 100 == 0) {
        logger().debug("check_mappings  i= {}", i);
	check_mappings(extmap_root, *t);
	check_mappings(extmap_root);
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
  });
}

TEST_F(extentmap_manager_test_t, force_split_balanced)
{
  run_async([this] {
    extmap_root_t extmap_root(0, L_ADDR_NULL);
    {
      auto t = tm.create_transaction();
      extmap_root = extmap_manager->initialize_extmap(*t).unsafe_get0();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        [[maybe_unused]] auto addref = insert_extent(extmap_root, *t, lo, {lo, len});
        lo += len;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(extmap_root, *t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings(extmap_root);
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i < 100) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_extent(extmap_root, *t, e.first, e.second);
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(extmap_root, *t);
        check_mappings(extmap_root);
      }
      i++;
      if (i == 100)
	break;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings(extmap_root);
  });
}

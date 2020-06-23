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
        extentmap_manager::create_extentmap_manager(&tm)) {}

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
    return tm.close(
    ).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }

  using test_extmap_t = std::map<uint32_t, lext_map_val_t>;
  test_extmap_t test_ext_mappings;
  test_extmap_t temp_mappings;

  TestBlockRef alloc_extent(
    Transaction &t,
    extent_len_t len,
    char contents) {
    auto extent = tm.alloc_extent<TestBlock>(
      t,
      L_ADDR_MIN,
      len).unsafe_get0();
    extent->set_contents(contents);
    EXPECT_EQ(len, extent->get_length());
    return extent;
  }

  ExtentRef insert_extent(
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val) {
    auto extent = extmap_manager->add_lextent(t, lo, val).unsafe_get0();
    EXPECT_EQ(lo, extent->logical_offset);
    EXPECT_EQ(val.laddr, extent->laddr);
    EXPECT_EQ(val.lextent_offset, extent->lextent_offset);
    EXPECT_EQ(val.length, extent->length);
    test_ext_mappings.emplace(std::make_pair(extent->logical_offset,
      lext_map_val_t{extent->laddr, extent->lextent_offset, extent->length}));
    return extent;
  }

  extent_map_list_t find_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(lo, extent.front()->logical_offset);
    EXPECT_EQ(len, extent.front()->length);
    return extent;
  }
  extent_map_list_t findno_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(extent.empty(), true);
    return extent;
  }
  extent_map_list_t find_any(
    Transaction &t,
    uint32_t lo,
    uint32_t len) {
    auto extent = extmap_manager->seek_lextent(t, lo, len).unsafe_get0();
    EXPECT_EQ(extent.size(), 1);
    return extent;
  }

  bool rm_extent(
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val ) {
    auto ret = extmap_manager->rm_lextent(t, lo, val).unsafe_get0();
    EXPECT_EQ(ret, true);
    test_ext_mappings.erase(test_ext_mappings.find(lo));
    return ret;
  }

  extent_map_list_t punch_extent(
    Transaction &t,
    uint32_t lo,
    uint32_t offset,
    uint32_t flag,
    uint32_t len) {
    auto extent = extmap_manager->punch_lextent(t, lo + offset, len).unsafe_get0();
    if (extent.empty())
      return extent;

    auto it = test_ext_mappings.find(lo);
    auto key = it->first;
    auto val = it->second;
    if (it != test_ext_mappings.end())
      test_ext_mappings.erase(it);
    if (flag == 0) {
      test_ext_mappings.insert(std::make_pair(lo +offset +len,
			       lext_map_val_t{val.laddr, len, val.length-len}));
      EXPECT_EQ(lo + offset, extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    if (flag == 1){
      test_ext_mappings.insert(std::make_pair(key, lext_map_val_t{val.laddr, 0, offset}));
      test_ext_mappings.insert(std::make_pair(lo + offset + len,
		       lext_map_val_t{val.laddr, offset + len, val.length-offset-len}));
      EXPECT_EQ(lo + offset, extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    if (flag == 2){
      test_ext_mappings.insert(std::make_pair(key,
			       lext_map_val_t{val.laddr, 0, val.length-len}));
      EXPECT_EQ(lo + offset , extent.front()->logical_offset);
      EXPECT_EQ(len, extent.front()->length);
    }
    return extent;
  }
  ExtentRef set_extent(
    Transaction &t,
    uint32_t lo,
    lext_map_val_t val,
    ExtentRef &&olde) {
    auto extent = extmap_manager->set_lextent(t, lo, val).unsafe_get0();
    EXPECT_EQ(lo, extent->logical_offset);
    EXPECT_EQ(val.length, extent->length);

    auto it = test_ext_mappings.find(olde->logical_offset);
    if (it != test_ext_mappings.end())
      test_ext_mappings.erase(it);
    if (lo > olde->logical_offset && (lo+val.length) < (olde->logical_offset + olde->length) ){
      test_ext_mappings.insert(std::make_pair(olde->logical_offset,
	lext_map_val_t{olde->laddr, olde->lextent_offset,
                       val.lextent_offset - olde->lextent_offset}));
      test_ext_mappings.insert(std::make_pair(lo + val.length,
        lext_map_val_t{olde->laddr, val.lextent_offset+val.length,
        olde->length-val.length-(val.lextent_offset - olde->lextent_offset)}));

    }
    return extent;
}
  void check_mappings(Transaction &t) {
    for (auto &&i: test_ext_mappings){
      auto ret_list = find_extent(t, i.first, i.second.length);
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.laddr, ret->laddr);
      EXPECT_EQ(i.second.lextent_offset, ret->lextent_offset);
      EXPECT_EQ(i.second.length, ret->length);
    }
  }

  void check_mappings() {
    auto t = tm.create_transaction();
    check_mappings(*t);
  }

};

TEST_F(extentmap_manager_test_t, basic)
{
  run_async([this] {
    uint32_t len = 4096;
    uint32_t lo = 0x1 * len;
    {
      auto t = tm.create_transaction();
      logger().debug("first transaction");
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      [[maybe_unused]] auto extent = alloc_extent(*t, len, 'a');
      [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
      [[maybe_unused]] auto seekref = find_extent(*t, lo, len);
      tm.submit_transaction(std::move(t)).unsafe_get();

      auto t2 = tm.create_transaction();
      logger().debug("second transaction");
      [[maybe_unused]] auto seekref2 = find_extent(*t2, lo, len);
      [[maybe_unused]] auto seekref5 = find_any(*t2, 0, len);
      [[maybe_unused]] auto rmret = rm_extent(*t2, lo, {extent->get_laddr(), 0, len});
      [[maybe_unused]] auto decref = tm.dec_ref(*t2, extent->get_laddr()).unsafe_get();
      [[maybe_unused]] auto seekref3 = findno_extent(*t2, lo, len);
      tm.submit_transaction(std::move(t2)).unsafe_get();

      auto t3 = tm.create_transaction();
      logger().debug("third transaction");
      [[maybe_unused]] auto seekref4 = findno_extent(*t3, lo, len);
      tm.submit_transaction(std::move(t3)).unsafe_get();

    }
  });
}

TEST_F(extentmap_manager_test_t, force_split)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 40; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 10; ++j) {
        auto extent = alloc_extent(*t, len, 'a');
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        lo += len;
        if ((i % 20 == 0) && (j == 5)) {
          check_mappings(*t);
        }
      }
      logger().debug("force split submit transaction i = {}", i);
      tm.submit_transaction(std::move(t)).unsafe_get();
      check_mappings();
    }
  });

}

TEST_F(extentmap_manager_test_t, force_split_merge)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 80; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        auto extent = alloc_extent(*t, len, 'a');
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        lo += len;
	if ((i % 10 == 0) && (j == 3)) {
	  check_mappings(*t);
	}
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
	check_mappings();
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i % 3 != 0) {
	auto val = e;
	[[maybe_unused]] auto rmref= rm_extent(*t, e.first, e.second);
        [[maybe_unused]] auto decref = tm.dec_ref(*t, val.second.laddr).unsafe_get();
      }

      if (i % 10 == 0) {
        logger().debug("submitting transaction i= {}", i);
	tm.submit_transaction(std::move(t)).unsafe_get();
	t = tm.create_transaction();
      }
      if (i % 100 == 0) {
        logger().debug("check_mappings  i= {}", i);
	check_mappings(*t);
	check_mappings();
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
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        auto extent = alloc_extent(*t, len, 'a');
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        lo += len;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(*t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 50 == 0) {
        check_mappings();
      }
    }
    auto t = tm.create_transaction();
    int i = 0;
    for (auto &e: test_ext_mappings) {
      if (i < 100) {
        auto val = e;
        [[maybe_unused]] auto rmref= rm_extent(*t, e.first, e.second);
        [[maybe_unused]] auto decref = tm.dec_ref(*t, val.second.laddr).unsafe_get();
      }

      if (i % 10 == 0) {
      logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      if (i % 50 == 0) {
      logger().debug("check_mappings  i= {}", i);
        check_mappings(*t);
        check_mappings();
      }
      i++;
      if (i == 100)
	break;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings();
  });
}

TEST_F(extentmap_manager_test_t, force_split_punch)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
    }
    uint32_t len = 4096;
    uint32_t lo = 0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split_merge transaction");
      for (unsigned j = 0; j < 5; ++j) {
        auto extent = alloc_extent(*t, 3*len, 'a');
        [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), 0, 3*len});
        lo += 3*len;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(*t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 10 == 0) {
        check_mappings();
      }
    }
    temp_mappings.insert(test_ext_mappings.begin(), test_ext_mappings.end());

    auto t = tm.create_transaction();
    int i = 1;
    for (auto &e: temp_mappings) {
      if (i < 50 && i % 3 == 0) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, 0, 0, len);
        i++;
	continue;
      }

      if (i < 100 && i %3 == 0) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, len, 1, len);
        i++;
	continue;
      }

      if (i < 150 && i % 3 == 0) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, 2*len, 2, len);
        i++;
	continue;
      }

      if(i < 200 && i % 3  == 0) {
        [[maybe_unused]] auto punchref= punch_extent(*t, e.first, 0, 3, 3*len);
        i++;
        continue;
      }

      if (i % 50 == 0) {
        logger().debug("submitting transaction i= {}", i);
        tm.submit_transaction(std::move(t)).unsafe_get();
        t = tm.create_transaction();
      }
      i++;
    }
    logger().debug("finally submitting transaction ");
    tm.submit_transaction(std::move(t)).unsafe_get();
    check_mappings();
  });
}

TEST_F(extentmap_manager_test_t, withoffset_and_punch)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
     }

     uint32_t len = 0x800;
     uint32_t lo = 0x600;
     auto t = tm.create_transaction();
     auto lextent_offset = lo % PAGE_SIZE;
     auto logical_len = lextent_offset + len;
     auto alloc_len = (logical_len +PAGE_SIZE -1) &~(PAGE_SIZE - 1);
     auto extent = alloc_extent(*t, alloc_len, 'a');
     [[maybe_unused]] auto addref = insert_extent(*t, lo, {extent->get_laddr(), lextent_offset, len});
     tm.submit_transaction(std::move(t)).unsafe_get();

     len = 0x200;
     lo = 0x700;
     lextent_offset = lo % PAGE_SIZE;
     t = tm.create_transaction();
     auto extlist = find_any(*t, lo, len);
     auto &extref = *extlist.begin();
     EXPECT_EQ(lo, extref->logical_offset+0x100);
     EXPECT_EQ(extref->length, 0x800);
     auto extentnew = alloc_extent(*t, PAGE_SIZE, 'a');
     [[maybe_unused]] auto setref = set_extent(*t, lo, {extentnew->get_laddr(),lextent_offset,len}, std::move(extref));
     tm.submit_transaction(std::move(t)).unsafe_get();
     check_mappings();

   });
}

TEST_F(extentmap_manager_test_t, split_and_findhole)
{
  run_async([this] {
    {
      auto t = tm.create_transaction();
      [[maybe_unused]] auto ret = extmap_manager->alloc_extmap_root(*t).unsafe_get();
      tm.submit_transaction(std::move(t)).unsafe_get();
     }

    uint32_t len = 0x200;
    uint32_t lo = 0x0;
    uint32_t off = 0x0;
    for (unsigned i = 0; i < 50; i++) {
      auto t = tm.create_transaction();
      logger().debug("opened split transaction");
      for (unsigned j = 0; j < 6; ++j) {
        auto extent = alloc_extent(*t, PAGE_SIZE, 'a');
	if (j % 3 == 0)
          [[maybe_unused]] auto addref0 = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
        if (j % 3 == 1)
          [[maybe_unused]] auto addref1 = insert_extent(*t, lo + 0x700, {extent->get_laddr(), 0x700, len});
        if (j %3 == 2) {
          [[maybe_unused]] auto addref2 = insert_extent(*t, lo, {extent->get_laddr(), 0, len});
          [[maybe_unused]] auto addref3 = insert_extent(*t, lo + 0x700, {extent->get_laddr(), 0x700, len});
        }

	lo += PAGE_SIZE;
        if ((i % 10 == 0) && (j == 3)) {
          check_mappings(*t);
        }
      }
      logger().debug("submitting transaction");
      tm.submit_transaction(std::move(t)).unsafe_get();
      if (i % 40 == 0) {
        check_mappings();
      }
    }

    int i = 0;
    lo = 0x0;
    bool skip = false;
    auto t = tm.create_transaction();
    for (auto &e: test_ext_mappings) {
      if (skip) {
        skip = false;
        continue;
      }
      if (i % 3 == 0) off = 0x200;
      if (i % 3 == 1) off = 0x500;
      if (i %3 == 2) off = 0x300;
      auto extref = extmap_manager->find_hole(*t, lo + off, len).unsafe_get0();
      EXPECT_EQ(extref->laddr, e.second.laddr);
      if (i % 3 == 0){
	EXPECT_EQ(extref->logical_offset, e.first + off);
      }
      if (i % 3 == 1){
	EXPECT_EQ(extref->logical_offset, e.first -e.second.lextent_offset + off);
      }
      if (i %3 == 2){
	skip = true;
        EXPECT_EQ(extref->logical_offset, e.first + off);
      }
      lo += PAGE_SIZE;
      i++;
    }
  });
}

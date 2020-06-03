// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;
using namespace crimson::os::seastore::lba_manager;
using namespace crimson::os::seastore::lba_manager::btree;

struct btree_lba_manager_test :
  public seastar_test_suite_t, JournalSegmentProvider {
  SegmentManagerRef segment_manager;
  Journal journal;
  Cache cache;
  BtreeLBAManagerRef lba_manager;

  const size_t block_size;

  btree_lba_manager_test()
    : segment_manager(create_ephemeral(segment_manager::DEFAULT_TEST_EPHEMERAL)),
      journal(*segment_manager),
      cache(*segment_manager),
      lba_manager(new BtreeLBAManager(*segment_manager, cache)),
      block_size(segment_manager->get_block_size())
  {
    journal.set_segment_provider(this);
  }

  segment_id_t next = 0;
  get_segment_ret get_segment() final {
    return get_segment_ret(
      get_segment_ertr::ready_future_marker{},
      next++);
  }

  void put_segment(segment_id_t segment) final {
    return;
  }

  auto submit_transaction(TransactionRef t)
  {
    auto record = cache.try_construct_record(*t);
    if (!record) {
      ceph_assert(0 == "cannot fail");
    }

    return journal.submit_record(std::move(*record)).safe_then(
      [this, t=std::move(t)](paddr_t addr) {
	cache.complete_commit(*t, addr);
      },
      crimson::ct_error::all_same_way([](auto e) {
	ceph_assert(0 == "Hit error submitting to journal");
      }));
  }

  seastar::future<> set_up_fut() final {
    return segment_manager->init(
    ).safe_then([this] {
      return journal.open_for_write();
    }).safe_then([this] {
      return seastar::do_with(
	lba_manager->create_transaction(),
	[this](auto &transaction) {
	  return cache.mkfs(*transaction
	  ).safe_then([this, &transaction] {
	    return lba_manager->mkfs(*transaction);
	  }).safe_then([this, &transaction] {
	    return submit_transaction(std::move(transaction));
	  });
	});
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ceph_assert(0 == "error");
      })
    );
  }

  seastar::future<> tear_down_fut() final {
    return cache.close(
    ).safe_then([this] {
      return journal.close();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }


  struct test_extent_t {
    paddr_t addr;
    size_t len = 0;
    unsigned refcount = 0;
  };
  using test_lba_mapping_t = std::map<laddr_t, test_extent_t>;
  test_lba_mapping_t test_lba_mappings;
  struct test_transaction_t {
    TransactionRef t;
    test_lba_mapping_t mappings;
  };

  auto create_transaction() {
    return test_transaction_t{
      lba_manager->create_transaction(),
      test_lba_mappings
    };
  }

  void submit_test_transaction(test_transaction_t t) {
    submit_transaction(std::move(t.t)).get0();
    test_lba_mappings.swap(t.mappings);
  }

  auto get_overlap(test_transaction_t &t, laddr_t addr, size_t len) {
    auto bottom = t.mappings.upper_bound(addr);
    if (bottom != t.mappings.begin())
      --bottom;
    if (bottom != t.mappings.end() &&
	bottom->first + bottom->second.len <= addr)
      ++bottom;

    auto top = t.mappings.upper_bound(addr + len);
    return std::make_pair(
      bottom,
      top
    );
  }

  auto alloc_mapping(
    test_transaction_t &t,
    laddr_t hint,
    size_t len,
    paddr_t paddr) {
    auto ret = lba_manager->alloc_extent(*t.t, hint, len, paddr).unsafe_get0();
    logger().debug("alloc'd: {}", *ret);
    EXPECT_EQ(len, ret->get_length());
    auto [b, e] = get_overlap(t, ret->get_laddr(), len);
    EXPECT_EQ(b, e);
    t.mappings.emplace(
      std::make_pair(
	ret->get_laddr(),
	test_extent_t{
	  ret->get_paddr(),
	  ret->get_length(),
	  1
        }
      ));
    return ret;
  }

  auto set_mapping(
    test_transaction_t &t,
    laddr_t addr,
    size_t len,
    paddr_t paddr) {
    auto [b, e] = get_overlap(t, addr, len);
    EXPECT_EQ(b, e);

    auto ret = lba_manager->set_extent(*t.t, addr, len, paddr).unsafe_get0();
    EXPECT_EQ(addr, ret->get_laddr());
    EXPECT_EQ(len, ret->get_length());
    EXPECT_EQ(paddr, ret->get_paddr());
    t.mappings.emplace(
      std::make_pair(
	ret->get_laddr(),
	test_extent_t{
	  ret->get_paddr(),
	  ret->get_length(),
	  1
        }
      ));
    return ret;
  }

  auto decref_mapping(
    test_transaction_t &t,
    laddr_t addr) {
    return decref_mapping(t, t.mappings.find(addr));
  }

  void decref_mapping(
    test_transaction_t &t,
    test_lba_mapping_t::iterator target) {
    ceph_assert(target != t.mappings.end());
    ceph_assert(target->second.refcount > 0);
    target->second.refcount--;

    auto refcnt = lba_manager->decref_extent(
      *t.t,
      target->first).unsafe_get0();
    EXPECT_EQ(refcnt, target->second.refcount);
    if (target->second.refcount == 0) {
      t.mappings.erase(target);
    }
  }

  void incref_mapping(
    test_transaction_t &t,
    test_lba_mapping_t::iterator target) {
    ceph_assert(target->second.refcount > 0);
    target->second.refcount++;
    auto refcnt = lba_manager->incref_extent(
      *t.t,
      target->first).unsafe_get0();
    EXPECT_EQ(refcnt, target->second.refcount);
  }

  std::vector<laddr_t> get_mapped_addresses() {
    std::vector<laddr_t> addresses;
    addresses.reserve(test_lba_mappings.size());
    for (auto &i: test_lba_mappings) {
      addresses.push_back(i.first);
    }
    return addresses;
  }

  void check_mappings() {
    auto t = create_transaction();
    check_mappings(t);
  }

  void check_mappings(test_transaction_t &t) {
    for (auto &&i: t.mappings) {
      auto ret_list = lba_manager->get_mapping(
	*t.t, i.first, i.second.len
      ).unsafe_get0();
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.addr, ret->get_paddr());
      EXPECT_EQ(i.first, ret->get_laddr());
      EXPECT_EQ(i.second.len, ret->get_length());
    }
  }
};

TEST_F(btree_lba_manager_test, basic)
{
  run_async([this] {
    laddr_t laddr = 0x12345678 * block_size;
    paddr_t paddr = { 1, static_cast<segment_off_t>(block_size * 10) };
    {
      // write initial mapping
      auto t = create_transaction();
      check_mappings(t);  // check in progress transaction sees mapping
      check_mappings();   // check concurrent does not
      auto ret = alloc_mapping(t, laddr, block_size, paddr);
      submit_test_transaction(std::move(t));
    }
    check_mappings();     // check new transaction post commit sees it
  });
}

TEST_F(btree_lba_manager_test, force_split)
{
  run_async([this] {
    for (unsigned i = 0; i < 40; ++i) {
      auto t = create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 5; ++j) {
	auto ret = alloc_mapping(t, 0, block_size, P_ADDR_MIN);
	if ((i % 10 == 0) && (j == 3)) {
	  check_mappings(t);
	  check_mappings();
	}
      }
      logger().debug("submitting transaction");
      submit_test_transaction(std::move(t));
      check_mappings();
    }
  });
}

TEST_F(btree_lba_manager_test, force_split_merge)
{
  run_async([this] {
    for (unsigned i = 0; i < 80; ++i) {
      auto t = create_transaction();
      logger().debug("opened transaction");
      for (unsigned j = 0; j < 5; ++j) {
	auto ret = alloc_mapping(t, 0, block_size, P_ADDR_MIN);
	// just to speed things up a bit
	if ((i % 100 == 0) && (j == 3)) {
	  check_mappings(t);
	  check_mappings();
	}
      }
      logger().debug("submitting transaction");
      submit_test_transaction(std::move(t));
      if (i % 50 == 0) {
	check_mappings();
      }
    }
    auto addresses = get_mapped_addresses();
    auto t = create_transaction();
    for (unsigned i = 0; i != addresses.size(); ++i) {
      if (i % 2 == 0) {
	decref_mapping(t, addresses[i]);
      }
      logger().debug("submitting transaction");
      if (i % 7 == 0) {
	submit_test_transaction(std::move(t));
	t = create_transaction();
      }
      if (i % 13 == 0) {
	check_mappings();
	check_mappings(t);
      }
    }
    submit_test_transaction(std::move(t));
  });
}

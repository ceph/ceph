// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "test/crimson/gtest_seastar.h"

#include "crimson/common/log.h"

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/lba_manager/btree/btree_lba_manager.h"

#include "test/crimson/seastore/test_block.h"

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

struct btree_test_base :
  public seastar_test_suite_t, SegmentProvider {

  segment_manager::EphemeralSegmentManagerRef segment_manager;
  ExtentReaderRef scanner;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  CacheRef cache;

  size_t block_size;

  WritePipeline pipeline;

  segment_id_t next;

  btree_test_base() = default;

  seastar::lowres_system_clock::time_point get_last_modified(
    segment_id_t id) const final {
    return seastar::lowres_system_clock::time_point();
  }

  seastar::lowres_system_clock::time_point get_last_rewritten(
    segment_id_t id) const final {
    return seastar::lowres_system_clock::time_point();
  }
  void update_segment_avail_bytes(paddr_t offset) final {}

  segment_id_t get_segment(device_id_t id, segment_seq_t seq) final {
    auto ret = next;
    next = segment_id_t{
      next.device_id(),
      next.device_segment_id() + 1};
    return ret;
  }

  journal_seq_t get_journal_tail_target() const final { return journal_seq_t{}; }
  void update_journal_tail_committed(journal_seq_t committed) final {}

  virtual void complete_commit(Transaction &t) {}
  seastar::future<> submit_transaction(TransactionRef t)
  {
    auto record = cache->prepare_record(*t);
    return journal->submit_record(std::move(record), t->get_handle()).safe_then(
      [this, t=std::move(t)](auto submit_result) mutable {
	cache->complete_commit(
            *t,
            submit_result.record_block_base,
            submit_result.write_result.start_seq);
	complete_commit(*t);
      }).handle_error(crimson::ct_error::assert_all{});
  }

  virtual LBAManager::mkfs_ret test_structure_setup(Transaction &t) = 0;
  seastar::future<> set_up_fut() final {
    segment_manager = segment_manager::create_test_ephemeral();
    scanner.reset(new ExtentReader());
    auto& scanner_ref = *scanner.get();
    journal = journal::make_segmented(
      *segment_manager, scanner_ref, *this);
    epm.reset(new ExtentPlacementManager());
    cache.reset(new Cache(scanner_ref, *epm));

    block_size = segment_manager->get_block_size();
    next = segment_id_t{segment_manager->get_device_id(), 0};
    scanner_ref.add_segment_manager(segment_manager.get());
    journal->set_write_pipeline(&pipeline);

    return segment_manager->init(
    ).safe_then([this] {
      return journal->open_for_write().discard_result();
    }).safe_then([this] {
      return epm->open();
    }).safe_then([this] {
      return seastar::do_with(
	cache->create_transaction(
            Transaction::src_t::MUTATE, "test_set_up_fut", false),
	[this](auto &ref_t) {
	  return with_trans_intr(*ref_t, [&](auto &t) {
	    cache->init();
	    return cache->mkfs(t
	    ).si_then([this, &t] {
	      return test_structure_setup(t);
	    });
	  }).safe_then([this, &ref_t] {
	    return submit_transaction(std::move(ref_t));
	  });
	});
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ceph_assert(0 == "error");
      })
    );
  }

  virtual void test_structure_reset() {}
  seastar::future<> tear_down_fut() final {
    return cache->close(
    ).safe_then([this] {
      return journal->close();
    }).safe_then([this] {
      return epm->close();
    }).safe_then([this] {
      test_structure_reset();
      segment_manager.reset();
      scanner.reset();
      journal.reset();
      epm.reset();
      cache.reset();
    }).handle_error(
      crimson::ct_error::all_same_way([] {
	ASSERT_FALSE("Unable to close");
      })
    );
  }
};

struct lba_btree_test : btree_test_base {
  std::map<laddr_t, lba_map_val_t> check;

  auto get_op_context(Transaction &t) {
    return op_context_t<laddr_t>{*cache, t};
  }

  LBAManager::mkfs_ret test_structure_setup(Transaction &t) final {
    return cache->get_root(
      t
    ).si_then([this, &t](RootBlockRef croot) {
      auto mut_croot = cache->duplicate_for_write(
	t, croot
      )->cast<RootBlock>();
      mut_croot->root.lba_root = LBABtree::mkfs(get_op_context(t));
    });
  }

  void update_if_dirty(Transaction &t, LBABtree &btree, RootBlockRef croot) {
    if (btree.is_root_dirty()) {
      auto mut_croot = cache->duplicate_for_write(
	t, croot
      )->cast<RootBlock>();
      mut_croot->root.lba_root = btree.get_root_undirty();
    }
  }

  template <typename F>
  auto lba_btree_update(F &&f) {
    auto tref = cache->create_transaction(
        Transaction::src_t::MUTATE, "test_btree_update", false);
    auto &t = *tref;
    with_trans_intr(
      t,
      [this, tref=std::move(tref), f=std::forward<F>(f)](auto &t) mutable {
	return cache->get_root(
	  t
	).si_then([this, f=std::move(f), &t](RootBlockRef croot) {
	  return seastar::do_with(
	    LBABtree(croot->root.lba_root),
	    [this, croot, f=std::move(f), &t](auto &btree) mutable {
	      return std::invoke(
		std::move(f), btree, t
	      ).si_then([this, croot, &t, &btree] {
		update_if_dirty(t, btree, croot);
		return seastar::now();
	      });
	    });
	}).si_then([this, tref=std::move(tref)]() mutable {
	  return submit_transaction(std::move(tref));
	});
      }).unsafe_get0();
  }

  template <typename F>
  auto lba_btree_read(F &&f) {
    auto t = cache->create_transaction(
        Transaction::src_t::READ, "test_btree_read", false);
    return with_trans_intr(
      *t,
      [this, f=std::forward<F>(f)](auto &t) mutable {
	return cache->get_root(
	  t
	).si_then([f=std::move(f), &t](RootBlockRef croot) mutable {
	  return seastar::do_with(
	    LBABtree(croot->root.lba_root),
	    [f=std::move(f), &t](auto &btree) mutable {
	      return std::invoke(
		std::move(f), btree, t
	      );
	    });
	});
      }).unsafe_get0();
  }

  static auto get_map_val(extent_len_t len) {
    return lba_map_val_t{0, P_ADDR_NULL, len, 0};
  }

  void insert(laddr_t addr, extent_len_t len) {
    ceph_assert(check.count(addr) == 0);
    check.emplace(addr, get_map_val(len));
    lba_btree_update([=](auto &btree, auto &t) {
      return btree.insert(
	get_op_context(t), addr, get_map_val(len)
      ).si_then([](auto){});
    });
  }

  void remove(laddr_t addr) {
    auto iter = check.find(addr);
    ceph_assert(iter != check.end());
    auto len = iter->second.len;
    check.erase(iter++);
    lba_btree_update([=](auto &btree, auto &t) {
      return btree.lower_bound(
	get_op_context(t), addr
      ).si_then([this, len, addr, &btree, &t](auto iter) {
	EXPECT_FALSE(iter.is_end());
	EXPECT_TRUE(iter.get_key() == addr);
	EXPECT_TRUE(iter.get_val().len == len);
	return btree.remove(
	  get_op_context(t), iter 
	);
      });
    });
  }

  void check_lower_bound(laddr_t addr) {
    auto iter = check.lower_bound(addr);
    auto result = lba_btree_read([=](auto &btree, auto &t) {
      return btree.lower_bound(
	get_op_context(t), addr
      ).si_then([](auto iter)
		-> std::optional<std::pair<const laddr_t, const lba_map_val_t>> {
	if (iter.is_end()) {
	  return std::nullopt;
	} else {
	  return std::make_optional(
	    std::make_pair(iter.get_key(), iter.get_val()));
	}
      });
    });
    if (iter == check.end()) {
      EXPECT_FALSE(result);
    } else {
      EXPECT_TRUE(result);
      decltype(result) to_check = *iter;
      EXPECT_EQ(to_check, *result);
    }
  }
};

TEST_F(lba_btree_test, basic)
{
  run_async([this] {
    constexpr unsigned total = 16<<10;
    for (unsigned i = 0; i < total; i += 16) {
      insert(i, 8);
    }

    for (unsigned i = 0; i < total; i += 16) {
      check_lower_bound(i);
      check_lower_bound(i + 4);
      check_lower_bound(i + 8);
      check_lower_bound(i + 12);
    }
  });
}

struct btree_lba_manager_test : btree_test_base {
  BtreeLBAManagerRef lba_manager;

  btree_lba_manager_test() = default;

  void complete_commit(Transaction &t) final {
    lba_manager->complete_transaction(t);
  }

  LBAManager::mkfs_ret test_structure_setup(Transaction &t) final {
    lba_manager.reset(new BtreeLBAManager(*segment_manager, *cache));
    return lba_manager->mkfs(t);
  }

  void test_structure_reset() final {
    lba_manager.reset();
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

  auto create_transaction(bool create_fake_extent=true) {
    auto t = test_transaction_t{
      cache->create_transaction(
          Transaction::src_t::MUTATE, "test_mutate_lba", false),
      test_lba_mappings
    };
    if (create_fake_extent) {
      cache->alloc_new_extent<TestBlockPhysical>(*t.t, TestBlockPhysical::SIZE);
    };
    return t;
  }

  auto create_weak_transaction() {
    auto t = test_transaction_t{
      cache->create_transaction(
          Transaction::src_t::READ, "test_read_weak", true),
      test_lba_mappings
    };
    return t;
  }

  void submit_test_transaction(test_transaction_t t) {
    submit_transaction(std::move(t.t)).get();
    test_lba_mappings.swap(t.mappings);
  }

  auto get_overlap(test_transaction_t &t, laddr_t addr, size_t len) {
    auto bottom = t.mappings.upper_bound(addr);
    if (bottom != t.mappings.begin())
      --bottom;
    if (bottom != t.mappings.end() &&
	bottom->first + bottom->second.len <= addr)
      ++bottom;

    auto top = t.mappings.lower_bound(addr + len);
    return std::make_pair(
      bottom,
      top
    );
  }

  seastore_off_t next_off = 0;
  paddr_t get_paddr() {
    next_off += block_size;
    return make_fake_paddr(next_off);
  }

  auto alloc_mapping(
    test_transaction_t &t,
    laddr_t hint,
    size_t len,
    paddr_t paddr) {
    auto ret = with_trans_intr(
      *t.t,
      [=](auto &t) {
	return lba_manager->alloc_extent(t, hint, len, paddr);
      }).unsafe_get0();
    logger().debug("alloc'd: {}", *ret);
    EXPECT_EQ(len, ret->get_length());
    auto [b, e] = get_overlap(t, ret->get_key(), len);
    EXPECT_EQ(b, e);
    t.mappings.emplace(
      std::make_pair(
	ret->get_key(),
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

    auto refcnt = with_trans_intr(
      *t.t,
      [=](auto &t) {
	return lba_manager->decref_extent(
	  t,
	  target->first);
      }).unsafe_get0().refcount;
    EXPECT_EQ(refcnt, target->second.refcount);
    if (target->second.refcount == 0) {
      t.mappings.erase(target);
    }
  }

  auto incref_mapping(
    test_transaction_t &t,
    laddr_t addr) {
    return incref_mapping(t, t.mappings.find(addr));
  }

  void incref_mapping(
    test_transaction_t &t,
    test_lba_mapping_t::iterator target) {
    ceph_assert(target->second.refcount > 0);
    target->second.refcount++;
    auto refcnt = with_trans_intr(
      *t.t,
      [=](auto &t) {
	return lba_manager->incref_extent(
	  t,
	  target->first);
      }).unsafe_get0().refcount;
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

  std::vector<laddr_t> get_mapped_addresses(test_transaction_t &t) {
    std::vector<laddr_t> addresses;
    addresses.reserve(t.mappings.size());
    for (auto &i: t.mappings) {
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
      auto laddr = i.first;
      auto len = i.second.len;

      auto ret_list = with_trans_intr(
	*t.t,
	[=](auto &t) {
	  return lba_manager->get_mappings(
	    t, laddr, len);
	}).unsafe_get0();
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.addr, ret->get_paddr());
      EXPECT_EQ(laddr, ret->get_key());
      EXPECT_EQ(len, ret->get_length());

      auto ret_pin = with_trans_intr(
	*t.t,
	[=](auto &t) {
	  return lba_manager->get_mapping(
	    t, laddr);
	}).unsafe_get0();
      EXPECT_EQ(i.second.addr, ret_pin->get_paddr());
      EXPECT_EQ(laddr, ret_pin->get_key());
      EXPECT_EQ(len, ret_pin->get_length());
    }
    with_trans_intr(
      *t.t,
      [=, &t](auto &) {
	return lba_manager->scan_mappings(
	  *t.t,
	  0,
	  L_ADDR_MAX,
	  [iter=t.mappings.begin(), &t](auto l, auto p, auto len) mutable {
	    EXPECT_NE(iter, t.mappings.end());
	    EXPECT_EQ(l, iter->first);
	    EXPECT_EQ(p, iter->second.addr);
	    EXPECT_EQ(len, iter->second.len);
	    ++iter;
	  });
      }).unsafe_get();
  }
};

TEST_F(btree_lba_manager_test, basic)
{
  run_async([this] {
    laddr_t laddr = 0x12345678 * block_size;
    {
      // write initial mapping
      auto t = create_transaction();
      check_mappings(t);  // check in progress transaction sees mapping
      check_mappings();   // check concurrent does not
      auto ret = alloc_mapping(t, laddr, block_size, get_paddr());
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
	auto ret = alloc_mapping(t, 0, block_size, get_paddr());
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
	auto ret = alloc_mapping(t, 0, block_size, get_paddr());
	// just to speed things up a bit
	if ((i % 100 == 0) && (j == 3)) {
	  check_mappings(t);
	  check_mappings();
	}
	incref_mapping(t, ret->get_key());
	decref_mapping(t, ret->get_key());
      }
      logger().debug("submitting transaction");
      submit_test_transaction(std::move(t));
      if (i % 50 == 0) {
	check_mappings();
      }
    }
    {
      auto addresses = get_mapped_addresses();
      auto t = create_transaction();
      for (unsigned i = 0; i != addresses.size(); ++i) {
	if (i % 2 == 0) {
	  incref_mapping(t, addresses[i]);
	  decref_mapping(t, addresses[i]);
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
    }
    {
      auto addresses = get_mapped_addresses();
      auto t = create_transaction();
      for (unsigned i = 0; i != addresses.size(); ++i) {
	incref_mapping(t, addresses[i]);
	decref_mapping(t, addresses[i]);
	decref_mapping(t, addresses[i]);
      }
      check_mappings(t);
      submit_test_transaction(std::move(t));
      check_mappings();
    }
  });
}

TEST_F(btree_lba_manager_test, single_transaction_split_merge)
{
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 400; ++i) {
	alloc_mapping(t, 0, block_size, get_paddr());
      }
      check_mappings(t);
      submit_test_transaction(std::move(t));
    }
    check_mappings();

    {
      auto addresses = get_mapped_addresses();
      auto t = create_transaction();
      for (unsigned i = 0; i != addresses.size(); ++i) {
	if (i % 4 != 0) {
	  decref_mapping(t, addresses[i]);
	}
      }
      check_mappings(t);
      submit_test_transaction(std::move(t));
    }
    check_mappings();

    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 600; ++i) {
	alloc_mapping(t, 0, block_size, get_paddr());
      }
      auto addresses = get_mapped_addresses(t);
      for (unsigned i = 0; i != addresses.size(); ++i) {
	decref_mapping(t, addresses[i]);
      }
      check_mappings(t);
      submit_test_transaction(std::move(t));
    }
    check_mappings();
  });
}

TEST_F(btree_lba_manager_test, split_merge_multi)
{
  run_async([this] {
    auto iterate = [&](auto f) {
      for (uint64_t i = 0; i < (1<<10); ++i) {
	auto t = create_transaction(false);
	logger().debug("opened transaction");
	for (unsigned j = 0; j < 5; ++j) {
	  f(t, (i * 5) + j);
	}
	logger().debug("submitting transaction");
	submit_test_transaction(std::move(t));
      }
    };
    iterate([&](auto &t, auto idx) {
      alloc_mapping(t, idx * block_size, block_size, get_paddr());
    });
    check_mappings();
    iterate([&](auto &t, auto idx) {
      if ((idx % 32) > 0) {
	decref_mapping(t, idx * block_size);
      }
    });
    check_mappings();
    iterate([&](auto &t, auto idx) {
      if ((idx % 32) > 0) {
	alloc_mapping(t, idx * block_size, block_size, get_paddr());
      }
    });
    check_mappings();
    iterate([&](auto &t, auto idx) {
      decref_mapping(t, idx * block_size);
    });
    check_mappings();
  });
}

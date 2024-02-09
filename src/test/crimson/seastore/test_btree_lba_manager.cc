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
  public seastar_test_suite_t, SegmentProvider, JournalTrimmer {

  segment_manager::EphemeralSegmentManagerRef segment_manager;
  SegmentManagerGroupRef sms;
  JournalRef journal;
  ExtentPlacementManagerRef epm;
  CacheRef cache;

  size_t block_size;

  WritePipeline pipeline;

  segment_id_t next;

  std::map<segment_id_t, segment_seq_t> segment_seqs;
  std::map<segment_id_t, segment_type_t> segment_types;

  journal_seq_t dummy_tail;

  mutable segment_info_t tmp_info;

  btree_test_base() = default;

  /*
   * JournalTrimmer interfaces
   */
  journal_seq_t get_journal_head() const final { return dummy_tail; }

  void set_journal_head(journal_seq_t) final {}

  journal_seq_t get_dirty_tail() const final { return dummy_tail; }

  journal_seq_t get_alloc_tail() const final { return dummy_tail; }

  void update_journal_tails(journal_seq_t, journal_seq_t) final {}

  bool try_reserve_inline_usage(std::size_t) final { return true; }

  void release_inline_usage(std::size_t) final {}

  std::size_t get_trim_size_per_cycle() const final {
    return 0;
  }

  /*
   * SegmentProvider interfaces
   */
  const segment_info_t& get_seg_info(segment_id_t id) const final {
    tmp_info = {};
    tmp_info.seq = segment_seqs.at(id);
    tmp_info.type = segment_types.at(id);
    return tmp_info;
  }

  segment_id_t allocate_segment(
    segment_seq_t seq,
    segment_type_t type,
    data_category_t,
    rewrite_gen_t
  ) final {
    auto ret = next;
    next = segment_id_t{
      segment_manager->get_device_id(),
      next.device_segment_id() + 1};
    segment_seqs[ret] = seq;
    segment_types[ret] = type;
    return ret;
  }

  void close_segment(segment_id_t) final {}

  void update_segment_avail_bytes(segment_type_t, paddr_t) final {}

  void update_modify_time(segment_id_t, sea_time_point, std::size_t) final {}

  SegmentManagerGroup* get_segment_manager_group() final { return sms.get(); }

  virtual void complete_commit(Transaction &t) {}
  seastar::future<> submit_transaction(TransactionRef t)
  {
    auto record = cache->prepare_record(*t, JOURNAL_SEQ_NULL, JOURNAL_SEQ_NULL);
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
    return segment_manager->init(
    ).safe_then([this] {
      return segment_manager->mkfs(
        segment_manager::get_ephemeral_device_config(0, 1, 0));
    }).safe_then([this] {
      sms.reset(new SegmentManagerGroup());
      journal = journal::make_segmented(*this, *this);
      epm.reset(new ExtentPlacementManager());
      cache.reset(new Cache(*epm));

      block_size = segment_manager->get_block_size();
      next = segment_id_t{segment_manager->get_device_id(), 0};
      sms->add_segment_manager(segment_manager.get());
      epm->test_init_no_background(segment_manager.get());
      journal->set_write_pipeline(&pipeline);

      return journal->open_for_mkfs().discard_result();
    }).safe_then([this] {
      dummy_tail = journal_seq_t{0,
        paddr_t::make_seg_paddr(segment_id_t(segment_manager->get_device_id(), 0), 0)};
      return epm->open_for_write();
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
	    }).si_then([&t] {
	      t.for_each_fresh_block_pre_submit([](auto &extent) {
		if (!extent->is_logical() ||
		    !extent->get_last_committed_crc()) {
		  extent->update_checksum();
		}
		assert(extent->calc_crc32c() == extent->get_last_committed_crc());
	      });
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
      sms.reset();
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
      mut_croot->root.lba_root =
	LBABtree::mkfs(mut_croot, get_op_context(t));
    });
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
	).si_then([f=std::move(f), &t](RootBlockRef croot) {
	  return seastar::do_with(
	    LBABtree(croot),
	    [f=std::move(f), &t](auto &btree) mutable {
	      return std::invoke(
		std::move(f), btree, t
	      );
	    });
	}).si_then([t=tref.get()]() mutable {
	  t->for_each_fresh_block_pre_submit([](auto &extent) {
	    if (!extent->is_logical() ||
		!extent->get_last_committed_crc()) {
	      extent->update_checksum();
	    }
	    assert(extent->calc_crc32c() == extent->get_last_committed_crc());
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
	    LBABtree(croot),
	    [f=std::move(f), &t](auto &btree) mutable {
	      return std::invoke(
		std::move(f), btree, t
	      );
	    });
	});
      }).unsafe_get0();
  }

  static auto get_map_val(extent_len_t len) {
    return lba_map_val_t{0, (pladdr_t)P_ADDR_NULL, len, 0};
  }

  device_off_t next_off = 0;
  paddr_t get_paddr() {
    next_off += block_size;
    return make_fake_paddr(next_off);
  }

  void insert(laddr_t addr, extent_len_t len) {
    ceph_assert(check.count(addr) == 0);
    check.emplace(addr, get_map_val(len));
    lba_btree_update([=, this](auto &btree, auto &t) {
      auto extents = cache->alloc_new_data_extents<TestBlock>(
	  t,
	  TestBlock::SIZE,
	  placement_hint_t::HOT,
	  0,
	  get_paddr());
      assert(extents.size() == 1);
      auto extent = extents.front();
      return btree.insert(
	get_op_context(t), addr, get_map_val(len), extent.get()
      ).si_then([addr, extent](auto p){
	auto& [iter, inserted] = p;
	assert(inserted);
	extent->set_laddr(addr);
      });
    });
  }

  void remove(laddr_t addr) {
    auto iter = check.find(addr);
    ceph_assert(iter != check.end());
    auto len = iter->second.len;
    check.erase(iter++);
    lba_btree_update([=, this](auto &btree, auto &t) {
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
    auto result = lba_btree_read([=, this](auto &btree, auto &t) {
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

  void complete_commit(Transaction &t) final {}

  LBAManager::mkfs_ret test_structure_setup(Transaction &t) final {
    lba_manager.reset(new BtreeLBAManager(*cache));
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
      cache->alloc_new_non_data_extent<TestBlockPhysical>(
          *t.t,
          TestBlockPhysical::SIZE,
          placement_hint_t::HOT,
          0);
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
    with_trans_intr(
      *t.t,
      [this](auto &t) {
	return seastar::do_with(
	  std::list<LogicalCachedExtentRef>(),
	  std::list<CachedExtentRef>(),
	  [this, &t](auto &lextents, auto &pextents) {
	  t.for_each_fresh_block_pre_submit(
	    [&lextents, &pextents](auto &extent) {
	    if (extent->is_logical()) {
	      if (!extent->get_last_committed_crc()) {
		extent->update_checksum();
	      }
	      assert(extent->calc_crc32c() == extent->get_last_committed_crc());
	      lextents.emplace_back(extent->template cast<LogicalCachedExtent>());
	    } else {
	      pextents.push_back(extent);
	    }
	  });
	  return lba_manager->update_mappings(
	    t, lextents
	  ).si_then([&pextents] {
	    for (auto &extent : pextents) {
	      assert(!extent->is_logical() && extent->is_valid());
	      extent->update_checksum();
	    }
	  });
	});
      }).unsafe_get0();
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

  device_off_t next_off = 0;
  paddr_t get_paddr() {
    next_off += block_size;
    return make_fake_paddr(next_off);
  }

  auto alloc_mapping(
    test_transaction_t &t,
    laddr_t hint,
    size_t len) {
    auto ret = with_trans_intr(
      *t.t,
      [=, this](auto &t) {
	auto extents = cache->alloc_new_data_extents<TestBlock>(
	    t,
	    TestBlock::SIZE,
	    placement_hint_t::HOT,
	    0,
	    get_paddr());
	assert(extents.size() == 1);
	auto extent = extents.front();
	return lba_manager->alloc_extent(
	  t, hint, *extent);
      }).unsafe_get0();
    logger().debug("alloc'd: {}", *ret);
    EXPECT_EQ(len, ret->get_length());
    auto [b, e] = get_overlap(t, ret->get_key(), len);
    EXPECT_EQ(b, e);
    t.mappings.emplace(
      std::make_pair(
	ret->get_key(),
	test_extent_t{
	  ret->get_val(),
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

    (void) with_trans_intr(
      *t.t,
      [=, this](auto &t) {
	return lba_manager->decref_extent(
	  t,
	  target->first,
	  true
	).si_then([this, &t, target](auto result) {
	  EXPECT_EQ(result.refcount, target->second.refcount);
	  if (result.refcount == 0) {
	    return cache->retire_extent_addr(
	      t, result.addr.get_paddr(), result.length);
	  }
	  return Cache::retire_extent_iertr::now();
	});
      }).unsafe_get0();
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
      [=, this](auto &t) {
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
    (void)with_trans_intr(
      *t.t,
      [=, this](auto &t) {
	return lba_manager->check_child_trackers(t);
      }).unsafe_get0();
    for (auto &&i: t.mappings) {
      auto laddr = i.first;
      auto len = i.second.len;

      auto ret_list = with_trans_intr(
	*t.t,
	[=, this](auto &t) {
	  return lba_manager->get_mappings(
	    t, laddr, len);
	}).unsafe_get0();
      EXPECT_EQ(ret_list.size(), 1);
      auto &ret = *ret_list.begin();
      EXPECT_EQ(i.second.addr, ret->get_val());
      EXPECT_EQ(laddr, ret->get_key());
      EXPECT_EQ(len, ret->get_length());

      auto ret_pin = with_trans_intr(
	*t.t,
	[=, this](auto &t) {
	  return lba_manager->get_mapping(
	    t, laddr);
	}).unsafe_get0();
      EXPECT_EQ(i.second.addr, ret_pin->get_val());
      EXPECT_EQ(laddr, ret_pin->get_key());
      EXPECT_EQ(len, ret_pin->get_length());
    }
    with_trans_intr(
      *t.t,
      [=, &t, this](auto &) {
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
      auto ret = alloc_mapping(t, laddr, block_size);
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
	auto ret = alloc_mapping(t, 0, block_size);
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
	auto ret = alloc_mapping(t, 0, block_size);
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
	alloc_mapping(t, 0, block_size);
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
	alloc_mapping(t, 0, block_size);
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
      alloc_mapping(t, idx * block_size, block_size);
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
	alloc_mapping(t, idx * block_size, block_size);
      }
    });
    check_mappings();
    iterate([&](auto &t, auto idx) {
      decref_mapping(t, idx * block_size);
    });
    check_mappings();
  });
}

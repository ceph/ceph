// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <random>

#include <boost/iterator/counting_iterator.hpp>

#include "test/crimson/gtest_seastar.h"
#include "test/crimson/seastore/transaction_manager_test_state.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/cache.h"
#include "crimson/os/seastore/transaction_manager.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"
#include "crimson/os/seastore/segment_manager.h"

#include "test/crimson/seastore/test_block.h"

using namespace crimson;
using namespace crimson::os;
using namespace crimson::os::seastore;

namespace {
  [[maybe_unused]] seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_test);
  }
}

struct test_extent_record_t {
  test_extent_desc_t desc;
  unsigned refcount = 0;
  test_extent_record_t() = default;
  test_extent_record_t(
    const test_extent_desc_t &desc,
    unsigned refcount) : desc(desc), refcount(refcount) {}

  void update(const test_extent_desc_t &to) {
    desc = to;
  }

  bool operator==(const test_extent_desc_t &rhs) const {
    return desc == rhs;
  }
  bool operator!=(const test_extent_desc_t &rhs) const {
    return desc != rhs;
  }
};

std::ostream &operator<<(std::ostream &lhs, const test_extent_record_t &rhs) {
  return lhs << "test_extent_record_t(" << rhs.desc
	     << ", refcount=" << rhs.refcount << ")";
}

struct transaction_manager_test_t :
  public seastar_test_suite_t,
  TMTestState {

  std::random_device rd;
  std::mt19937 gen;

  transaction_manager_test_t()
    : gen(rd()) {
  }

  laddr_t get_random_laddr(size_t block_size, laddr_t limit) {
    return block_size *
      std::uniform_int_distribution<>(0, (limit / block_size) - 1)(gen);
  }

  char get_random_contents() {
    return static_cast<char>(std::uniform_int_distribution<>(0, 255)(gen));
  }

  seastar::future<> set_up_fut() final {
    return tm_setup();
  }

  seastar::future<> tear_down_fut() final {
    return tm_teardown();
  }

  struct test_extents_t : std::map<laddr_t, test_extent_record_t> {
    using delta_t = std::map<laddr_t, std::optional<test_extent_record_t>>;

    struct delta_overlay_t {
      const test_extents_t &extents;
      const delta_t &delta;

      delta_overlay_t(
	const test_extents_t &extents,
	const delta_t &delta)
	: extents(extents), delta(delta) {}


      class iterator {
	friend class test_extents_t;

	const delta_overlay_t &parent;
	test_extents_t::const_iterator biter;
	delta_t::const_iterator oiter;
	std::optional<std::pair<laddr_t, test_extent_record_t>> cur;

	iterator(
	  const delta_overlay_t &parent,
	  test_extents_t::const_iterator biter,
	  delta_t::const_iterator oiter)
	  : parent(parent), biter(biter), oiter(oiter) {}

	laddr_t get_bkey() {
	  return biter == parent.extents.end() ? L_ADDR_MAX : biter->first;
	}

	laddr_t get_okey() {
	  return oiter == parent.delta.end() ? L_ADDR_MAX : oiter->first;
	}

	bool is_end() {
	  return oiter == parent.delta.end() && biter == parent.extents.end();
	}

	bool is_valid() {
	  return is_end() ||
	    ((get_okey() < get_bkey()) && (oiter->second)) ||
	    (get_okey() > get_bkey());
	}

	auto get_pair() {
	  assert(is_valid());
	  assert(!is_end());
	  auto okey = get_okey();
	  auto bkey = get_bkey();
	  return (
	    bkey < okey ?
	    std::pair<laddr_t, test_extent_record_t>(*biter) :
	    std::make_pair(okey, *(oiter->second)));
	}

	void adjust() {
	  while (!is_valid()) {
	    if (get_okey() < get_bkey()) {
	      assert(!oiter->second);
	      ++oiter;
	    } else {
	      assert(get_okey() == get_bkey());
	      ++biter;
	    }
	  }
	  assert(is_valid());
	  if (!is_end()) {
	    cur = get_pair();
	  } else {
	    cur = std::nullopt;
	  }
	}

      public:
	iterator(const iterator &) = default;
	iterator(iterator &&) = default;
	iterator &operator=(const iterator &) = default;
	iterator &operator=(iterator &&) = default;

	iterator &operator++() {
	  assert(is_valid());
	  assert(!is_end());
	  if (get_bkey() < get_okey()) {
	    ++biter;
	  } else {
	    ++oiter;
	  }
	  adjust();
	  return *this;
	}

	bool operator==(const iterator &o) const {
	  return o.biter == biter && o.oiter == oiter;
	}
	bool operator!=(const iterator &o) const {
	  return !(*this == o);
	}

	auto operator*() {
	  assert(!is_end());
	  return *cur;
	}
	auto operator->() {
	  assert(!is_end());
	  return &*cur;
	}
      };

      iterator begin() {
	auto ret = iterator{*this, extents.begin(), delta.begin()};
	ret.adjust();
	return ret;
      }

      iterator end() {
	auto ret = iterator{*this, extents.end(), delta.end()};
	// adjust unnecessary
	return ret;
      }

      iterator lower_bound(laddr_t l) {
	auto ret = iterator{*this, extents.lower_bound(l), delta.lower_bound(l)};
	ret.adjust();
	return ret;
      }

      iterator upper_bound(laddr_t l) {
	auto ret = iterator{*this, extents.upper_bound(l), delta.upper_bound(l)};
	ret.adjust();
	return ret;
      }

      iterator find(laddr_t l) {
	auto ret = lower_bound(l);
	if (ret == end() || ret->first != l) {
	  return end();
	} else {
	  return ret;
	}
      }
    };
  private:
    void check_available(
      laddr_t addr, extent_len_t len, const delta_t &delta
    ) const {
      delta_overlay_t overlay(*this, delta);
      for (const auto &i: overlay) {
	if (i.first < addr) {
	  EXPECT_FALSE(i.first + i.second.desc.len > addr);
	} else {
	  EXPECT_FALSE(addr + len > i.first);
	}
      }
    }

    void check_hint(
      laddr_t hint,
      laddr_t addr,
      extent_len_t len,
      delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      auto iter = overlay.lower_bound(hint);
      laddr_t last = hint;
      while (true) {
	if (iter == overlay.end() || iter->first > addr) {
	  EXPECT_EQ(addr, last);
	  break;
	}
	EXPECT_FALSE(iter->first - last > len);
	last = iter->first + iter->second.desc.len;
	++iter;
      }
    }

    std::optional<test_extent_record_t> &populate_delta(
      laddr_t addr, delta_t &delta, const test_extent_desc_t *desc) const {
      auto diter = delta.find(addr);
      if (diter != delta.end())
	return diter->second;

      auto iter = find(addr);
      if (iter == end()) {
	assert(desc);
	auto ret = delta.emplace(
	  std::make_pair(addr, test_extent_record_t{*desc, 0}));
	assert(ret.second);
	return ret.first->second;
      } else {
	auto ret = delta.emplace(*iter);
	assert(ret.second);
	return ret.first->second;
      }
    }
  public:
    delta_overlay_t get_overlay(const delta_t &delta) const {
      return delta_overlay_t{*this, delta};
    }

    void insert(TestBlock &extent, delta_t &delta) const {
      check_available(extent.get_laddr(), extent.get_length(), delta);
      delta[extent.get_laddr()] =
	test_extent_record_t{extent.get_desc(), 1};
    }

    void alloced(laddr_t hint, TestBlock &extent, delta_t &delta) const {
      check_hint(hint, extent.get_laddr(), extent.get_length(), delta);
      insert(extent, delta);
    }

    bool contains(laddr_t addr, const delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      return overlay.find(addr) != overlay.end();
    }

    test_extent_record_t get(laddr_t addr, const delta_t &delta) const {
      delta_overlay_t overlay(*this, delta);
      auto iter = overlay.find(addr);
      assert(iter != overlay.end());
      return iter->second;
    }

    void update(
      laddr_t addr,
      const test_extent_desc_t &desc,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, &desc);
      assert(rec);
      rec->desc = desc;
    }

    int inc_ref(
      laddr_t addr,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, nullptr);
      assert(rec);
      return ++rec->refcount;
    }

    int dec_ref(
      laddr_t addr,
      delta_t &delta) const {
      auto &rec = populate_delta(addr, delta, nullptr);
      assert(rec);
      assert(rec->refcount > 0);
      rec->refcount--;
      if (rec->refcount == 0) {
	delta[addr] = std::nullopt;
	return 0;
      } else {
	return rec->refcount;
      }
    }

    void consume(const delta_t &delta) {
      for (const auto &i : delta) {
	if (i.second) {
	  (*this)[i.first] = *i.second;
	} else {
	  erase(i.first);
	}
      }
    }

  } test_mappings;

  struct test_transaction_t {
    TransactionRef t;
    test_extents_t::delta_t mapping_delta;
  };

  test_transaction_t create_transaction() {
    return { itm.create_transaction(), {} };
  }

  test_transaction_t create_weak_transaction() {
    return { itm.create_weak_transaction(), {} };
  }

  TestBlockRef alloc_extent(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len,
    char contents) {
    auto extent = itm.alloc_extent<TestBlock>(
      *(t.t),
      hint,
      len).unsafe_get0();
    extent->set_contents(contents);
    EXPECT_FALSE(test_mappings.contains(extent->get_laddr(), t.mapping_delta));
    EXPECT_EQ(len, extent->get_length());
    test_mappings.alloced(hint, *extent, t.mapping_delta);
    return extent;
  }

  TestBlockRef alloc_extent(
    test_transaction_t &t,
    laddr_t hint,
    extent_len_t len) {
    return alloc_extent(
      t,
      hint,
      len,
      get_random_contents());
  }

  bool check_usage() {
    auto t = create_weak_transaction();
    SpaceTrackerIRef tracker(segment_cleaner->get_empty_space_tracker());
    with_trans_intr(
      *t.t,
      [this, &tracker](auto &t) {
	return lba_manager->scan_mapped_space(
	  t,
	  [&tracker](auto offset, auto len) {
	    tracker->allocate(
	      offset.segment,
	      offset.offset,
	      len);
	  });
      }).unsafe_get0();
    return segment_cleaner->debug_check_space(*tracker);
  }

  void replay() {
    logger().debug("{}: begin", __func__);
    EXPECT_TRUE(check_usage());
    restart();
    logger().debug("{}: end", __func__);
  }

  void check() {
    check_mappings();
    check_usage();
  }

  void check_mappings() {
    auto t = create_weak_transaction();
    check_mappings(t);
  }

  TestBlockRef get_extent(
    test_transaction_t &t,
    laddr_t addr,
    extent_len_t len) {
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));
    ceph_assert(test_mappings.get(addr, t.mapping_delta).desc.len == len);

    auto ext = itm.read_extent<TestBlock>(
      *t.t, addr, len
    ).unsafe_get0();
    EXPECT_EQ(addr, ext->get_laddr());
    return ext;
  }

  TestBlockRef try_get_extent(
    test_transaction_t &t,
    laddr_t addr,
    extent_len_t len) {
    ceph_assert(test_mappings.contains(addr, t.mapping_delta));
    ceph_assert(test_mappings.get(addr, t.mapping_delta).desc.len == len);

    using ertr = with_trans_ertr<TransactionManager::read_extent_iertr>;
    using ret = ertr::future<TestBlockRef>;
    auto ext = itm.read_extent<TestBlock>(
      *t.t, addr, len
    ).safe_then([](auto ext) -> ret {
      return ertr::make_ready_future<TestBlockRef>(ext);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<TestBlockRef>();
      },
      crimson::ct_error::assert_all{
	"get_extent got invalid error"
      }
    ).get0();
    if (ext) {
      EXPECT_EQ(addr, ext->get_laddr());
    }
    return ext;
  }

  test_block_mutator_t mutator;
  TestBlockRef mutate_extent(
    test_transaction_t &t,
    TestBlockRef ref) {
    ceph_assert(test_mappings.contains(ref->get_laddr(), t.mapping_delta));
    ceph_assert(
      test_mappings.get(ref->get_laddr(), t.mapping_delta).desc.len ==
      ref->get_length());

    auto ext = itm.get_mutable_extent(*t.t, ref)->cast<TestBlock>();
    EXPECT_EQ(ext->get_laddr(), ref->get_laddr());
    EXPECT_EQ(ext->get_desc(), ref->get_desc());
    mutator.mutate(*ext, gen);

    test_mappings.update(ext->get_laddr(), ext->get_desc(), t.mapping_delta);
    return ext;
  }

  TestBlockRef mutate_addr(
    test_transaction_t &t,
    laddr_t offset,
    size_t length) {
    auto ext = get_extent(t, offset, length);
    mutate_extent(t, ext);
    return ext;
  }

  void inc_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    ceph_assert(test_mappings.get(offset, t.mapping_delta).refcount > 0);

    auto refcnt = itm.inc_ref(*t.t, offset).unsafe_get0();
    auto check_refcnt = test_mappings.inc_ref(offset, t.mapping_delta);
    EXPECT_EQ(refcnt, check_refcnt);
  }

  void dec_ref(test_transaction_t &t, laddr_t offset) {
    ceph_assert(test_mappings.contains(offset, t.mapping_delta));
    ceph_assert(test_mappings.get(offset, t.mapping_delta).refcount > 0);

    auto refcnt = itm.dec_ref(*t.t, offset).unsafe_get0();
    auto check_refcnt = test_mappings.dec_ref(offset, t.mapping_delta);
    EXPECT_EQ(refcnt, check_refcnt);
    if (refcnt == 0)
      logger().debug("dec_ref: {} at refcount 0", offset);
  }

  void check_mappings(test_transaction_t &t) {
    auto overlay = test_mappings.get_overlay(t.mapping_delta);
    for (const auto &i: overlay) {
      logger().debug("check_mappings: {}->{}", i.first, i.second);
      auto ext = get_extent(t, i.first, i.second.desc.len);
      EXPECT_EQ(i.second, ext->get_desc());
    }
    with_trans_intr(
      *t.t,
      [this, &overlay](auto &t) {
	return lba_manager->scan_mappings(
	  t,
	  0,
	  L_ADDR_MAX,
	  [iter=overlay.begin(), &overlay](auto l, auto p, auto len) mutable {
	    EXPECT_NE(iter, overlay.end());
	    logger().debug(
	      "check_mappings: scan {}",
	      l);
	    EXPECT_EQ(l, iter->first);
	    ++iter;
	  });
      }).unsafe_get0();
  }

  bool try_submit_transaction(test_transaction_t t) {
    using ertr = with_trans_ertr<TransactionManager::submit_transaction_iertr>;
    using ret = ertr::future<bool>;
    bool success = itm.submit_transaction(*t.t
    ).safe_then([]() -> ret {
      return ertr::make_ready_future<bool>(true);
    }).handle_error(
      [](const crimson::ct_error::eagain &e) {
	return seastar::make_ready_future<bool>(false);
      },
      crimson::ct_error::assert_all{
	"try_submit_transaction hit invalid error"
      }
    ).then([this](auto ret) {
      return segment_cleaner->run_until_halt().then([ret] { return ret; });
    }).get0();

    if (success) {
      test_mappings.consume(t.mapping_delta);
    }

    return success;
  }

  void submit_transaction(test_transaction_t &&t) {
    bool success = try_submit_transaction(std::move(t));
    EXPECT_TRUE(success);
  }

  void submit_transaction_expect_conflict(test_transaction_t &&t) {
    bool success = try_submit_transaction(std::move(t));
    EXPECT_FALSE(success);
  }
};

TEST_F(transaction_manager_test_t, basic)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_F(transaction_manager_test_t, mutate)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    ASSERT_TRUE(check_usage());
    replay();
    {
      auto t = create_transaction();
      auto ext = get_extent(
	t,
	ADDR,
	SIZE);
      auto mut = mutate_extent(t, ext);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    ASSERT_TRUE(check_usage());
    replay();
    check();
  });
}

TEST_F(transaction_manager_test_t, allocate_lba_conflict)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    constexpr laddr_t ADDR2 = 0xFE * SIZE;
    auto t = create_transaction();
    auto t2 = create_transaction();

    // These should conflict as they should both modify the lba root
    auto extent = alloc_extent(
      t,
      ADDR,
      SIZE,
      'a');
    ASSERT_EQ(ADDR, extent->get_laddr());
    check_mappings(t);
    check();

    auto extent2 = alloc_extent(
      t2,
      ADDR2,
      SIZE,
      'a');
    ASSERT_EQ(ADDR2, extent2->get_laddr());
    check_mappings(t2);
    extent2.reset();

    submit_transaction(std::move(t2));
    submit_transaction_expect_conflict(std::move(t));
  });
}

TEST_F(transaction_manager_test_t, mutate_lba_conflict)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 300; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }

    constexpr laddr_t ADDR = 150 * SIZE;
    {
      auto t = create_transaction();
      auto t2 = create_transaction();

      mutate_addr(t, ADDR, SIZE);
      mutate_addr(t2, ADDR, SIZE);

      submit_transaction(std::move(t));
      submit_transaction_expect_conflict(std::move(t2));
    }
    check();

    {
      auto t = create_transaction();
      mutate_addr(t, ADDR, SIZE);
      submit_transaction(std::move(t));
    }
    check();
  });
}

TEST_F(transaction_manager_test_t, concurrent_mutate_lba_no_conflict)
{
  constexpr laddr_t SIZE = 4096;
  constexpr size_t NUM = 500;
  constexpr laddr_t addr = 0;
  constexpr laddr_t addr2 = SIZE * (NUM - 1);
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < NUM; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      submit_transaction(std::move(t));
    }

    {
      auto t = create_transaction();
      auto t2 = create_transaction();

      mutate_addr(t, addr, SIZE);
      mutate_addr(t2, addr2, SIZE);

      submit_transaction(std::move(t));
      submit_transaction(std::move(t2));
    }
    check();
  });
}

TEST_F(transaction_manager_test_t, create_remove_same_transaction)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      dec_ref(t, ADDR);
      check_mappings(t);

      extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');

      submit_transaction(std::move(t));
      check();
    }
    replay();
    check();
  });
}

TEST_F(transaction_manager_test_t, split_merge_read_same_transaction)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 300; ++i) {
	auto extent = alloc_extent(
	  t,
	  laddr_t(i * SIZE),
	  SIZE);
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
    {
      auto t = create_transaction();
      for (unsigned i = 0; i < 240; ++i) {
	dec_ref(
	  t,
	  laddr_t(i * SIZE));
      }
      check_mappings(t);
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_F(transaction_manager_test_t, inc_dec_ref)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    constexpr laddr_t ADDR = 0xFF * SIZE;
    {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	ADDR,
	SIZE,
	'a');
      ASSERT_EQ(ADDR, extent->get_laddr());
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      inc_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
    replay();
    {
      auto t = create_transaction();
      dec_ref(t, ADDR);
      check_mappings(t);
      check();
      submit_transaction(std::move(t));
      check();
    }
  });
}

TEST_F(transaction_manager_test_t, cause_lba_split)
{
  constexpr laddr_t SIZE = 4096;
  run_async([this] {
    for (unsigned i = 0; i < 200; ++i) {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	i * SIZE,
	SIZE,
	(char)(i & 0xFF));
      ASSERT_EQ(i * SIZE, extent->get_laddr());
      submit_transaction(std::move(t));
    }
    check();
  });
}

TEST_F(transaction_manager_test_t, random_writes)
{
  constexpr size_t TOTAL = 4<<20;
  constexpr size_t BSIZE = 4<<10;
  constexpr size_t PADDING_SIZE = 256<<10;
  constexpr size_t BLOCKS = TOTAL / BSIZE;
  run_async([this] {
    for (unsigned i = 0; i < BLOCKS; ++i) {
      auto t = create_transaction();
      auto extent = alloc_extent(
	t,
	i * BSIZE,
	BSIZE);
      ASSERT_EQ(i * BSIZE, extent->get_laddr());
      submit_transaction(std::move(t));
    }

    for (unsigned i = 0; i < 4; ++i) {
      for (unsigned j = 0; j < 65; ++j) {
	auto t = create_transaction();
	for (unsigned k = 0; k < 2; ++k) {
	  auto ext = get_extent(
	    t,
	    get_random_laddr(BSIZE, TOTAL),
	    BSIZE);
	  auto mut = mutate_extent(t, ext);
	  // pad out transaction
	  auto padding = alloc_extent(
	    t,
	    TOTAL + (k * PADDING_SIZE),
	    PADDING_SIZE);
	  dec_ref(t, padding->get_laddr());
	}
	submit_transaction(std::move(t));
      }
      replay();
      logger().debug("random_writes: checking");
      check();
      logger().debug("random_writes: done replaying/checking");
    }
  });
}

TEST_F(transaction_manager_test_t, random_writes_concurrent)
{
  constexpr unsigned WRITE_STREAMS = 256;

  constexpr size_t TOTAL = 4<<20;
  constexpr size_t BSIZE = 4<<10;
  constexpr size_t BLOCKS = TOTAL / BSIZE;
  run_async([this] {
    seastar::parallel_for_each(
      boost::make_counting_iterator(0u),
      boost::make_counting_iterator(WRITE_STREAMS),
      [&](auto idx) {
	for (unsigned i = idx; i < BLOCKS; i += WRITE_STREAMS) {
	  while (true) {
	    auto t = create_transaction();
	    auto extent = alloc_extent(
	      t,
	      i * BSIZE,
	      BSIZE);
	    ASSERT_EQ(i * BSIZE, extent->get_laddr());
	    if (try_submit_transaction(std::move(t)))
	      break;
	  }
	}
      }).get0();

    int writes = 0;
    unsigned failures = 0;
    seastar::parallel_for_each(
      boost::make_counting_iterator(0u),
      boost::make_counting_iterator(WRITE_STREAMS),
      [&](auto) {
        return seastar::async([&] {
          while (writes < 300) {
            auto t = create_transaction();
            auto ext = try_get_extent(
              t,
              get_random_laddr(BSIZE, TOTAL),
              BSIZE);
            if (!ext){
              failures++;
              continue;
            }
	    auto mut = mutate_extent(t, ext);
	    auto success = try_submit_transaction(std::move(t));
	    writes += success;
	    failures += !success;
	  }
	});
      }).get0();
    replay();
    logger().debug("random_writes: checking");
    check();
    logger().debug(
      "random_writes: {} suceeded, {} failed",
      writes,
      failures
    );
  });
}
